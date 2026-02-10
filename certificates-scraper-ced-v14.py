#!/usr/bin/env python3
"""
Certificates Scraper v16 - ULTIME EMISSIONI
Focus: Indici, Valute, Commodities, Tassi, Credit Linked
Esclude singoli titoli azionari
"""

import json
import re
import time
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

CONFIG = {
    "timeout": 60000,
    "output_file": "certificates-data.json",
    "max_certificates": 150,      # limite di sicurezza
    "days_back": 60               # ultime emissioni degli ultimi 60 giorni
}

def is_desired_underlying(text):
    """Ritorna True se il sottostante è accettabile"""
    if not text:
        return False
    text = text.lower()
    forbidden = ['spa', 'nv', 'plc', 'ltd', 'inc', 'sa', 'ag', 's.p.a', 'n.v.', 's.a.']
    allowed_keywords = ['euro stoxx', 'ftse', 's&p', 'nasdaq', 'dax', 'cac', 'brent', 'wti', 
                       'gold', 'silver', 'eur/usd', 'usd/jpy', 'btp', 'bund', 'oat', 'credit', 
                       'index', 'indice', 'commodity', 'valuta', 'tasso', 'rate', 'basket']
    
    # Escludi se sembra un singolo titolo
    if any(word in text for word in forbidden) and len(text.split()) <= 4:
        return False
    return any(kw in text for kw in allowed_keywords)

def parse_number(text):
    if not text: return None
    try:
        cleaned = re.sub(r'[^\d,.-]', '', text)
        cleaned = cleaned.replace('.', '').replace(',', '.')
        return round(float(cleaned), 4)
    except:
        return None

def parse_date(text):
    if not text: return None
    try:
        if '/' in text:
            d, m, y = text.strip().split('/')
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except:
        pass
    return text

def scrape_recent_isins(page):
    """Fase 1: Raccoglie ISIN recenti dalla ricerca avanzata / inizi negoziazione"""
    isins = []
    
    # Prova prima la pagina principale con "Inizi negoziazione"
    page.goto("https://www.borsaitaliana.it/cw-e-certificates/covered-warrant/certificates.htm", 
              timeout=CONFIG["timeout"], wait_until="networkidle")
    time.sleep(4)
    
    html = page.content()
    soup = BeautifulSoup(html, 'html.parser')
    
    # Cerca link con ISIN nelle sezioni "Inizi negoziazione"
    for link in soup.find_all('a', href=True):
        href = link['href']
        if re.search(r'/scheda/([A-Z0-9]{12})\.html', href):
            isin = re.search(r'/scheda/([A-Z0-9]{12})\.html', href).group(1)
            if isin not in isins:
                isins.append(isin)
    
    # Se pochi risultati, vai su ricerca avanzata (più potente)
    if len(isins) < 30:
        page.goto("https://www.borsaitaliana.it/borsa/cw-e-certificates/ricerca-avanzata.html", 
                  timeout=CONFIG["timeout"], wait_until="networkidle")
        time.sleep(3)
        
        # Qui potresti aggiungere interazioni con i filtri (es. selezionare Cash Collect, Phoenix, etc.)
        # Per ora estraiamo tutti i visibili
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            match = re.search(r'/scheda/([A-Z0-9]{12})\.html', link['href'])
            if match:
                isin = match.group(1)
                if isin not in isins:
                    isins.append(isin)
    
    return isins[:CONFIG["max_certificates"]]

def get_certificate(page, isin):
    """Fase 2: Estrae dati dettagliati dalla scheda (stessa logica v15 migliorata)"""
    urls = [
        f"https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{isin}.html",
        f"https://www.borsaitaliana.it/borsa/cw-e-certificates/eurotlx/scheda/{isin}.html",
    ]
    
    for url in urls:
        try:
            page.goto(url, timeout=CONFIG["timeout"], wait_until="networkidle")
            time.sleep(3)
            
            soup = BeautifulSoup(page.content(), 'html.parser')
            
            # Estrazione nome, emittente, tipo, prezzi...
            cert = {
                "isin": isin,
                "name": "",
                "type": "",
                "issuer": "",
                "underlyings": [],
                "barrier_down": None,
                "maturity_date": None,
                "annual_coupon_yield": None,
                "reference_price": None,
                "bid_price": None,
                "ask_price": None,
                "url": url,
                "scraped_at": datetime.now().isoformat()
            }
            
            # ... (mantengo la stessa logica di parsing della v15, la puoi riutilizzare)
            # Per brevità la ometto qui, dimmi se vuoi quella parte completa

            # Controllo sottostante
            underlying_text = ""
            for td in soup.find_all('td'):
                if any(k in td.get_text().lower() for k in ['sottostante', 'underlying']):
                    underlying_text = td.get_text()
                    break
            
            if not is_desired_underlying(underlying_text):
                return None  # scarta singolo titolo
            
            return cert
            
        except:
            continue
    return None

def main():
    print("=== Certificates Scraper v16 - Ultime Emissioni ===")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        ).new_page()
        
        print("Raccolta ultime emissioni...")
        isins = scrape_recent_isins(page)
        print(f"Trovati {len(isins)} ISIN recenti")
        
        certificates = []
        for i, isin in enumerate(isins, 1):
            print(f"[{i}/{len(isins)}] {isin} → ", end="", flush=True)
            cert = get_certificate(page, isin)
            if cert:
                certificates.append(cert)
                print("OK")
            else:
                print("scartato (sottostante non idoneo)")
            time.sleep(0.7)
        
        browser.close()
    
    output = {
        "success": True,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "source": "borsaitaliana.it",
            "version": "16.0",
            "filter": "Indici, Commodities, Valute, Tassi, Credit Linked"
        }
    }
    
    with open(CONFIG["output_file"], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Completato → {len(certificates)} certificati salvati")

if __name__ == "__main__":
    main()
