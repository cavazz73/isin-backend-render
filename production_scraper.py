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
TARGET_COUNT = 80  # Si ferma appena trova 80 certificati VALIDI (Indici/Commodity/ecc)
OUTPUT_FILE = "data/certificates-data.json"

# --- FILTRO ASSET CLASS ---
# Parole chiave per accettare il sottostante. Se non ne contiene nessuna, si assume sia un'Azione e si scarta.
VALID_KEYWORDS = [
    # Indici
    "EURO STOXX", "EUROSTOXX", "S&P", "SP500", "NASDAQ", "DOW JONES", "FTSE", "DAX", "CAC", "IBEX", "NIKKEI", "HANG SENG", "MSCI", "STOXX", "BANKS", "AUTOMOBILES", "INSURANCE", "UTILITIES", "ENERGY", "TECH",
    # Commodities
    "GOLD", "ORO", "SILVER", "ARGENTO", "OIL", "PETROLIO", "BRENT", "WTI", "GAS", "COPPER", "RAME", "PALLADIUM", "PLATINUM", "WHEAT", "CORN",
    # Valute / Tassi
    "EUR/", "USD/", "/EUR", "/USD", "EURIBOR", "CMS", "IRS", "SOFR", "ESTR", "LIBOR", "T-NOTE", "BUND", "BTP"
]

# --- HELPER ---
def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip() if text else "N/D"

def parse_float(text):
    if not text: return 0.0
    text = text.upper().replace('EUR', '').replace('€', '').replace('%', '').strip()
    if ',' in text and '.' in text: text = text.replace('.', '').replace(',', '.')
    elif ',' in text: text = text.replace(',', '.')
    clean = re.sub(r'[^\d.]', '', text)
    try: return float(clean)
    except: return 0.0

def is_valid_asset(underlying_name):
    """Ritorna True solo se il sottostante è Indice, Commodity o Tasso/Valuta"""
    u = underlying_name.upper()
    for kw in VALID_KEYWORDS:
        if kw in u:
            return True
    return False

def calculate_scenarios(price, strike, barrier):
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
# MOTORE DI SCRAPING (REGEX ROBUSTO)
# ==========================================
async def scrape_certificate(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    try:
        await page.goto(url, timeout=20000, wait_until="domcontentloaded")
        if "home.asp" in page.url: return None

        # Estrazione Testo Grezzo (la tecnica che funziona!)
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        full_text = clean_text(soup.get_text(separator=' | '))

        if "EMITTENTE" not in full_text.upper(): return None

        # 1. NOME & SOTTOSTANTE
        nome_tag = soup.find('td', class_='titolo_scheda')
        nome = clean_text(nome_tag.get_text()) if nome_tag else f"Cert {isin}"
        
        # Cerchiamo il sottostante per filtrarlo
        sottostante = "N/D"
        m_und = re.search(r'Sottostante[^a-zA-Z0-9]{0,10}([a-zA-Z0-9 &/\.\-\+]{3,50})', full_text, re.IGNORECASE)
        if m_und: 
            sottostante = clean_text(m_und.group(1))
        else:
            # Fallback: usa il nome se contiene indizi
            sottostante = nome

        # 2. FILTRO (Il cuore della tua richiesta)
        if not is_valid_asset(sottostante) and not is_valid_asset(nome):
            # print(f"      🚫 SCARTATO (Azione/Altro): {sottostante}")
            return "SKIPPED_TYPE"

        # 3. PREZZO (Regex "Raggi X")
        prezzo = 0.0
        # Cerca "Prezzo/Ultimo/Valore" seguito da un numero
        regex_prezzo = r'(?:Prezzo|Ultimo|Valore|Quotazione|Ask|Lettera)[^0-9]{0,20}(\d+[.,]\d+)'
        match = re.search(regex_prezzo, full_text, re.IGNORECASE)
        if match:
            prezzo = parse_float(match.group(1))
        
        if prezzo <= 0: return None

        # 4. DATI TECNICI
        def get_regex_val(keywords):
            for kw in keywords:
                pat = kw + r'[^0-9]{0,20}(\d+[.,]\d+)'
                m = re.search(pat, full_text, re.IGNORECASE)
                if m: return parse_float(m.group(1))
            return 0.0

        strike = get_regex_val(["Strike", "Iniziale", "Strike Level"])
        barriera = get_regex_val(["Barriera", "Barrier", "Livello Barriera"])
        cedola = get_regex_val(["Cedola", "Premio", "Bonus"])
        
        # Emittente
        emittente = "N/D"
        m_em = re.search(r'Emittente[^a-zA-Z0-9]{0,10}([a-zA-Z ]{3,30})', full_text, re.IGNORECASE)
        if m_em: emittente = clean_text(m_em.group(1))

        # Defaults
        if strike == 0: strike = prezzo
        if barriera == 0: barriera = prezzo * 0.60

        return {
            "isin": isin,
            "name": nome,
            "type": "Certificate",
            "asset_class": "Indices/Commodities", # Etichetta per frontend
            "issuer": emittente,
            "market": "SeDeX",
            "currency": "EUR",
            "price": prezzo,
            "bid_price": prezzo,
            "ask_price": prezzo,
            "annual_coupon_yield": cedola,
            "barrier_down": barriera,
            "strike": strike,
            "maturity_date": (datetime.now() + timedelta(days=730)).isoformat(),
            "underlyings": [{"name": sottostante, "strike": strike, "barrier": barriera}],
            "scenario_analysis": calculate_scenarios(prezzo, strike, barriera),
            "source": "Certificati&Derivati"
        }

    except Exception:
        return None

# ==========================================
# MAIN RUNNER
# ==========================================
async def run_batch():
    print("--- 🚀 AVVIO SCRAPER (Filtro: Indici/Commodity/Tassi) ---")
    results = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36']
        )
        page = await browser.new_page()

        # 1. Recupero Nuove Emissioni (Limitato ai primi 150)
        isins = []
        try:
            # print("   📥 Scarico lista nuove emissioni...")
            await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", timeout=25000)
            
            # Prende SOLO i link nella tabella principale, non tutto il sito
            links = await page.locator("table a[href*='isin=']").all()
            
            for l in links:
                href = await l.get_attribute("href")
                m = re.search(r'isin=([A-Z0-9]{12})', href or "", re.IGNORECASE)
                if m: isins.append(m.group(1))
            
            # Rimuove duplicati e LIMITA a 200 per non intasare
            isins = list(set(isins))[:200] 
        except:
            # Fallback se la lista fallisce
            isins = ['IT0006771510', 'DE000HD8SXZ1', 'XS2470031936', 'CH1390857220', 'IT0006755018', 'XS2544207512']

        print(f"📋 Analizzerò i {len(isins)} certificati più recenti.")
        print(f"🎯 Obiettivo: Salvare {TARGET_COUNT} certificati (Indici/Commodities).")

        for
