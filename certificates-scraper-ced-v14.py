#!/usr/bin/env python3
"""
Certificates Scraper v14 - BORSA ITALIANA
ISIN verificati + filtro sottostanti

FILTRI: Solo Indici, Valute, Commodities, Tassi, Credit Linked
NO AZIONI SINGOLE
"""

import json
import re
import time
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# ===================================
# ISIN VERIFICATI - Certificati su Indici/Commodities
# ===================================

VERIFIED_ISINS = [
    # Certificati su Euro Stoxx 50 / FTSE MIB
    "XS2209061329",  # IS EP CP EURO STOXX 50
    "IT0005525610",  # Uc Cc Ftse Mib/Eurostoxx50
    "IT0005498297",  # Uc Cc Ftse Mib/Eurostoxx50
    "CH1283542608",  # Lq Exp Euro Stoxx 50/Ftse Mib
    "XS1385761249",  # Bpa Exp Euro Stoxx 50/Ftse Mib
    
    # Altri certificati su indici (da aggiungere)
    "DE000HV4K3K8",  # UniCredit su indici
    "DE000HV4K3L6",
    "DE000HV4K3M4",
    "IT0005521411",  # Su indici
    "XS2314073839",
    "XS2314074050",
    "XS2394956712",
    "XS2314660502",
    
    # Tracker/Benchmark su indici
    "NL0015436031",
    "NL0015436072",
    "DE000SF5ACX5",
    
    # Commodities
    "GB00B15KXV33",  # Gold ETC
    "JE00B1VS3770",  # WTI Oil
    "GB00B15KXQ89",  # Silver
    "XS1073722347",  # Commodity
]

CONFIG = {
    "detail_url": "https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/",
    "eurotlx_url": "https://www.borsaitaliana.it/borsa/cw-e-certificates/eurotlx/scheda/",
    "timeout": 45000,
    "output_file": "certificates-data.json"
}

# ESCLUDI solo se contiene ESCLUSIVAMENTE azioni singole
SINGLE_STOCKS = [
    "unicredit", "intesa", "enel", "eni", "generali", "ferrari",
    "stellantis", "stm", "telecom", "leonardo", "pirelli", "moncler",
    "tesla", "apple", "amazon", "nvidia", "microsoft", "alphabet",
    "google", "meta", "netflix", "amd", "intel", "adobe"
]


def is_single_stock_only(text):
    """Ritorna True SOLO se è un certificato su singola azione"""
    if not text:
        return False  # Se vuoto, NON escludere
    
    t = text.lower()
    
    # Se contiene "indice", "index", "stoxx", "mib", "dax", etc -> NON è single stock
    index_keywords = ["indic", "index", "stoxx", "mib", "dax", "s&p", "nasdaq", 
                      "nikkei", "commodity", "oro", "gold", "oil", "petrolio",
                      "basket", "paniere", "worst of"]
    
    for kw in index_keywords:
        if kw in t:
            return False  # Ha un indice/commodity -> INCLUDI
    
    # Verifica se ha SOLO azioni singole
    for stock in SINGLE_STOCKS:
        if stock in t:
            # Controlla se c'è anche qualcosa di non-stock
            other_found = False
            for kw in index_keywords:
                if kw in t:
                    other_found = True
                    break
            if not other_found:
                return True  # Solo azione singola -> ESCLUDI
    
    return False  # Default: INCLUDI


def parse_number(text):
    if not text or text.strip().upper() in ['N.A.', 'N.D.', '-', '', 'N/A', '--']:
        return None
    try:
        cleaned = re.sub(r'[EUR€%\s\xa0]', '', text.strip())
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        return round(float(cleaned), 4)
    except:
        return None


def parse_date(text):
    if not text or 'N.A' in text or 'Open' in text or '--' in text:
        return None
    try:
        if '/' in text:
            p = text.strip().split('/')
            if len(p) == 3:
                return f"{p[2]}-{p[1].zfill(2)}-{p[0].zfill(2)}"
    except:
        pass
    return text


def get_certificate(page, isin):
    """Estrae dati certificato da Borsa Italiana"""
    
    # Prova URL standard
    urls = [
        f"{CONFIG['detail_url']}{isin}.html",
        f"{CONFIG['eurotlx_url']}{isin}.html",
        f"{CONFIG['eurotlx_url']}{isin}-ETLX.html"
    ]
    
    for url in urls:
        try:
            page.goto(url, timeout=CONFIG["timeout"], wait_until="domcontentloaded")
            time.sleep(0.5)
            
            html = page.content()
            
            # Verifica che la pagina esista
            if "Page Not Found" in html or "404" in html or "non trovato" in html.lower():
                continue
            
            soup = BeautifulSoup(html, 'html.parser')
            
            cert = {
                "isin": isin,
                "name": "",
                "type": "",
                "issuer": "",
                "market": "SeDeX",
                "currency": "EUR",
                "underlying": "",
                "strike": None,
                "barrier_pct": None,
                "expiry_date": None,
                "reference_price": None,
                "bid_price": None,
                "ask_price": None,
                "source": "borsaitaliana.it",
                "url": url,
                "scraped_at": datetime.now().isoformat()
            }
            
            # Nome
            h1 = soup.find('h1')
            if h1:
                cert["name"] = h1.get_text(strip=True)
            if not cert["name"]:
                title = soup.find('title')
                if title:
                    cert["name"] = title.get_text(strip=True).split(' - ')[0]
            
            # Cerca dati nelle tabelle
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all(['th', 'td'])
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        
                        if any(x in label for x in ['riferimento', 'reference', 'ultimo', 'close']):
                            p = parse_number(value)
                            if p and p > 0:
                                cert["reference_price"] = p
                        elif 'bid' in label or 'denaro' in label:
                            cert["bid_price"] = parse_number(value)
                        elif 'ask' in label or 'lettera' in label:
                            cert["ask_price"] = parse_number(value)
                        elif 'emittente' in label or 'issuer' in label:
                            cert["issuer"] = value
                        elif 'sottostante' in label or 'underlying' in label:
                            if 'valore' not in label:
                                cert["underlying"] = value
                        elif 'scadenza' in label or 'expiry' in label:
                            cert["expiry_date"] = parse_date(value)
                        elif 'barriera' in label or 'barrier' in label:
                            cert["barrier_pct"] = parse_number(value)
                        elif 'strike' in label:
                            cert["strike"] = parse_number(value)
                        elif 'tipologia' in label or 'marketing' in label:
                            cert["type"] = value
            
            # Se ha un nome, ritorna
            if cert["name"]:
                return cert
                
        except Exception as e:
            continue
    
    return None


def main():
    print("=" * 60)
    print("Certificates Scraper v14 - BORSA ITALIANA")
    print("ISIN verificati su Indici/Commodities/Valute/Tassi")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    certificates = []
    skipped = 0
    errors = 0
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            locale="it-IT"
        )
        page = context.new_page()
        
        try:
            print(f"\nProcessing {len(VERIFIED_ISINS)} verified ISINs...\n")
            
            for i, isin in enumerate(VERIFIED_ISINS, 1):
                print(f"[{i}/{len(VERIFIED_ISINS)}] {isin}...", end=" ", flush=True)
                
                cert = get_certificate(page, isin)
                
                if cert and cert.get("name"):
                    # Verifica se è solo azione singola
                    check_text = f"{cert.get('name', '')} {cert.get('underlying', '')}"
                    
                    if is_single_stock_only(check_text):
                        skipped += 1
                        print(f"SKIP (single stock: {check_text[:30]})")
                    else:
                        certificates.append(cert)
                        price = cert.get("reference_price")
                        price_str = f"{price} EUR" if price else "N/A"
                        name = cert.get("name", "")[:35]
                        print(f"OK - {price_str} - {name}")
                else:
                    errors += 1
                    print("NOT FOUND")
                
                time.sleep(0.3)
            
        finally:
            browser.close()
    
    # Output
    output = {
        "metadata": {
            "version": "14.0",
            "timestamp": datetime.now().isoformat(),
            "source": "borsaitaliana.it",
            "total": len(certificates),
            "skipped_stocks": skipped,
            "not_found": errors,
            "filter": "Indices, Commodities, Currencies, Rates (NO single stocks)"
        },
        "certificates": certificates
    }
    
    with open(CONFIG["output_file"], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 60)
    print("COMPLETED")
    print(f"  Saved: {len(certificates)} certificates")
    print(f"  Skipped (single stocks): {skipped}")
    print(f"  Not found: {errors}")
    print(f"  File: {CONFIG['output_file']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
