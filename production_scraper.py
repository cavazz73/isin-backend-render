import asyncio
import json
import re
import os
import argparse
import random
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURAZIONE ---
TARGET_COUNT = 80
OUTPUT_FILE = "data/certificates-data.json"

# --- FILTRO ASSET CLASS ---
# Keyword per accettare solo Indici, Materie Prime, Valute e Tassi.
# Tutto il resto (Azioni singole) viene scartato.
VALID_KEYWORDS = [
    # Indici
    "EURO STOXX", "EUROSTOXX", "S&P", "SP500", "NASDAQ", "DOW JONES", "FTSE", "DAX", 
    "CAC", "IBEX", "NIKKEI", "HANG SENG", "MSCI", "STOXX", "BANKS", "AUTOMOBILES", 
    "INSURANCE", "UTILITIES", "ENERGY", "TECH", "MIB", "LEONARDO", "ENI", "ENEL", "INTESA",
    # Commodities
    "GOLD", "ORO", "SILVER", "ARGENTO", "OIL", "PETROLIO", "BRENT", "WTI", "GAS", 
    "COPPER", "RAME", "PALLADIUM", "PLATINUM", "WHEAT", "CORN", "NATURAL GAS",
    # Valute / Tassi
    "EUR/", "USD/", "/EUR", "/USD", "EURIBOR", "CMS", "IRS", "SOFR", "ESTR", 
    "LIBOR", "T-NOTE", "BUND", "BTP", "FOREX"
]

# --- HELPER ---
def clean_text(text):
    if not text: return "N/D"
    return re.sub(r'\s+', ' ', text).strip()

def parse_float(text):
    if not text: return 0.0
    text = text.upper().replace('EUR', '').replace('€', '').replace('%', '').strip()
    # Gestione formati: 1.234,56 vs 1234.56
    if ',' in text and '.' in text: text = text.replace('.', '').replace(',', '.')
    elif ',' in text: text = text.replace(',', '.')
    
    clean = re.sub(r'[^\d.]', '', text)
    try: return float(clean)
    except: return 0.0

def is_valid_asset(underlying_name):
    u = underlying_name.upper()
    for kw in VALID_KEYWORDS:
        if kw in u: return True
    return False

def calculate_scenarios(price, strike, barrier):
    """Genera scenari matematici per i grafici se mancano i dati"""
    if not price or price <= 0: return []
    if not strike or strike <= 0: strike = price
    if not barrier or barrier <= 0: barrier = strike * 0.60 
    
    scenarios = []
    variations = [-50, -40, -30, -20, -10, 0, 10, 20]
    
    for var in variations:
        underlying_sim = strike * (1 + (var / 100))
        if underlying_sim >= barrier: redemption = 100.0
        else: redemption = (underlying_sim / strike) * 100
        
        pl_pct = ((redemption - price) / price) * 100
        
        scenarios.append({
            "variation_pct": var,
            "underlying_price": round(underlying_sim, 2),
            "redemption": round(redemption, 2),
            "pl_pct": round(pl_pct, 2)
        })
    return scenarios

# ==========================================
# SCRAPER CORE (Logic)
# ==========================================
async def scrape_certificate(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    try:
        await page.goto(url, timeout=20000, wait_until="domcontentloaded")
        if "home.asp" in page.url: return None

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        # Estrazione Testo Grezzo ("Raggi X")
        full_text = clean_text(soup.get_text(separator=' | '))

        if "EMITTENTE" not in full_text.upper(): return None

        # 1. NOME
        nome_tag = soup.find('td', class_='titolo_scheda')
        nome = clean_text(nome_tag.get_text()) if nome_tag else f"Cert {isin}"
        
        # 2. SOTTOSTANTE (Per filtro)
        sottostante = "N/D"
        # Regex per trovare il sottostante
        m_und = re.search(r'Sottostante[^a-zA-Z0-9]{0,15}([a-zA-Z0-9 &/\.\-\+]{3,50})', full_text, re.IGNORECASE)
        if m_und: 
            sottostante = clean_text(m_und.group(1))
        else: 
            sottostante = nome

        # FILTRO ASSET CLASS
        if not is_valid_asset(sottostante) and not is_valid_asset(nome):
            return "SKIPPED_TYPE"

        # 3. PREZZO
        prezzo = 0.0
        # Cerca Prezzo seguito da un numero (ignorando testo intermedio)
        regex_prezzo = r'(?:Prezzo|Ultimo|Valore|Quotazione|Ask|Lettera)[^0-9]{0,30}(\
