#!/usr/bin/env python3
"""
Certificates Scraper v14 - BORSA ITALIANA
Fonte unica, affidabile

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
# CONFIGURAZIONE
# ===================================

CONFIG = {
    "base_url": "https://www.borsaitaliana.it",
    "list_url": "https://www.borsaitaliana.it/borsa/cw-e-certificates/elenco.html",
    "detail_url": "https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/",
    "max_pages": 10,
    "max_certificates": 200,
    "timeout": 60000,
    "output_file": "certificates-data.json"
}

# ===================================
# FILTRI SOTTOSTANTI
# ===================================

# INCLUDI: Indici, Valute, Commodities, Tassi, Credit Linked
TARGET_KEYWORDS = [
    # Indici
    "index", "indice", "indici", "ftse", "mib", "stoxx", "eurostoxx",
    "dax", "cac", "ibex", "s&p", "s&p500", "nasdaq", "dow jones", 
    "nikkei", "hang seng", "russell", "msci", "topix", "smi", "aex",
    # Commodities
    "gold", "oro", "silver", "argento", "oil", "petrolio", "wti", "brent",
    "natural gas", "gas", "copper", "rame", "platinum", "platino",
    "palladium", "palladio", "commodity", "commodities",
    # Valute
    "eur/usd", "usd/jpy", "eur/gbp", "forex", "currency", "valuta",
    # Tassi
    "euribor", "libor", "bund", "btp", "treasury", "tasso", "rate",
    # Credit Linked
    "credit", "cds",
    # Basket/Paniere
    "basket", "paniere", "worst of", "best of"
]

# ESCLUDI: Azioni singole
EXCLUDE_KEYWORDS = [
    # Italiane
    "unicredit", "intesa", "sanpaolo", "enel", "eni", "generali", 
    "ferrari", "stellantis", "stm", "telecom", "tim", "leonardo",
    "pirelli", "moncler", "campari", "mediobanca", "fineco",
    "poste", "snam", "terna", "recordati", "amplifon", "nexi",
    # USA
    "tesla", "apple", "amazon", "nvidia", "microsoft", "alphabet",
    "google", "meta", "netflix", "amd", "intel", "adobe", "oracle",
    "paypal", "visa", "mastercard", "boeing", "pfizer",
    # EU
    "lvmh", "asml", "sap", "siemens", "allianz", "basf", "bayer",
    "nestle", "novartis", "roche", "shell", "airbus"
]


def is_target_underlying(text):
    """Verifica se sottostante è target"""
    if not text:
        return True
    
    t = text.lower()
    
    # Escludi azioni singole
    for exc in EXCLUDE_KEYWORDS:
        if exc in t and "basket" not in t and "worst" not in t and "indic" not in t:
            return False
    
    # Includi se ha keyword target
    for kw in TARGET_KEYWORDS:
        if kw in t:
            return True
    
    return False


def parse_number(text):
    """Converte stringa in numero italiano"""
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
    """Converte data italiana in ISO"""
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


def get_certificate_detail(page, isin):
    """Estrae dettagli certificato da Borsa Italiana"""
    url = f"{CONFIG['detail_url']}{isin}.html"
    
    try:
        page.goto(url, timeout=CONFIG["timeout"], wait_until="domcontentloaded")
        time.sleep(1)
        
        html = page.content()
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
            "day_change_pct": None,
            "source": "borsaitaliana.it",
            "scraped_at": datetime.now().isoformat()
        }
        
        # Nome da h1 o title
        h1 = soup.find('h1')
        if h1:
            cert["name"] = h1.get_text(strip=True)
        else:
            title = soup.find('title')
            if title:
                cert["name"] = title.get_text(strip=True).split(' - ')[0]
        
        # Cerca in tutte le tabelle
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['th', 'td'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    
                    # Prezzi
                    if any(x in label for x in ['riferimento', 'reference', 'ultimo', 'last']):
                        if 'prezzo' in label or 'price' in label or 'close' in label:
                            cert["reference_price"] = parse_number(value)
                    elif 'bid' in label or 'denaro' in label:
                        cert["bid_price"] = parse_number(value)
                    elif 'ask' in label or 'lettera' in label:
                        cert["ask_price"] = parse_number(value)
                    elif 'var' in label and '%' in label:
                        cert["day_change_pct"] = parse_number(value)
                    
                    # Info strumento
                    elif 'emittente' in label or 'issuer' in label:
                        cert["issuer"] = value
                    elif 'sottostante' in label or 'underlying' in label:
                        if 'valore' not in label and 'value' not in label:
                            cert["underlying"] = value
                    elif 'scadenza' in label or 'expiry' in label:
                        cert["expiry_date"] = parse_date(value)
                    elif 'barriera' in label or 'barrier' in label:
                        cert["barrier_pct"] = parse_number(value)
                    elif 'strike' in label:
                        cert["strike"] = parse_number(value)
                    elif 'tipologia' in label or 'marketing' in label or 'type' in label:
                        cert["type"] = value
        
        return cert
        
    except Exception as e:
        print(f"Error: {e}")
        return None


def get_certificates_list(page):
    """Ottiene lista ISIN da Borsa Italiana"""
    print("Fetching certificates list from Borsa Italiana...")
    
    all_isins = set()
    
    # Lista URL da provare
    urls_to_try = [
        "https://www.borsaitaliana.it/borsa/cw-e-certificates/elenco.html",
        "https://www.borsaitaliana.it/borsa/cw-e-certificates/tutti.html",
        "https://www.borsaitaliana.it/borsa/cw-e-certificates/lista.html"
    ]
    
    for url in urls_to_try:
        try:
            print(f"  Trying: {url.split('/')[-1]}")
            page.goto(url, timeout=CONFIG["timeout"], wait_until="domcontentloaded")
            time.sleep(2)
            
            html = page.content()
            
            # Cerca tutti gli ISIN (pattern: 2 lettere + 10 alfanumerici)
            isins = re.findall(r'\b([A-Z]{2}[A-Z0-9]{10})\b', html)
            
            for isin in isins:
                all_isins.add(isin)
            
            print(f"    Found: {len(isins)} ISINs")
            
            if len(all_isins) >= 50:
                break
                
        except Exception as e:
            print(f"    Error: {e}")
            continue
    
    # Prova paginazione
    for pg in range(1, CONFIG["max_pages"] + 1):
        if len(all_isins) >= CONFIG["max_certificates"]:
            break
            
        try:
            url = f"https://www.borsaitaliana.it/borsa/cw-e-certificates/lista.html?&page={pg}"
            page.goto(url, timeout=CONFIG["timeout"], wait_until="domcontentloaded")
            time.sleep(1)
            
            html = page.content()
            isins = re.findall(r'\b([A-Z]{2}[A-Z0-9]{10})\b', html)
            
            before = len(all_isins)
            for isin in isins:
                all_isins.add(isin)
            
            added = len(all_isins) - before
            if added == 0:
                break
                
            print(f"  Page {pg}: +{added} ISINs (total: {len(all_isins)})")
            
        except:
            break
    
    print(f"  Total unique ISINs: {len(all_isins)}")
    return list(all_isins)


def main():
    print("=" * 60)
    print("Certificates Scraper v14 - BORSA ITALIANA")
    print("Filtri: Indici, Commodities, Valute, Tassi, Credit Linked")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    certificates = []
    skipped = 0
    errors = 0
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="it-IT"
        )
        page = context.new_page()
        
        try:
            # 1. Ottieni lista ISIN
            isin_list = get_certificates_list(page)
            
            if not isin_list:
                print("WARNING: No ISINs from list, using fallback...")
                # Fallback: ISIN noti di certificati su indici
                isin_list = [
                    "DE000VV1GPU6", "DE000VV5ZU05", "DE000VV6B9X4",
                    "NLBNPIT1XAF4", "NLBNPIT1XAG2", "DE000HV8F1D1",
                    "DE000HV8F1E9", "XS2314660502", "XS2394956712"
                ]
            
            isin_list = isin_list[:CONFIG["max_certificates"]]
            print(f"\nProcessing {len(isin_list)} certificates...\n")
            
            # 2. Estrai dettagli per ogni ISIN
            for i, isin in enumerate(isin_list, 1):
                print(f"[{i}/{len(isin_list)}] {isin}...", end=" ", flush=True)
                
                cert = get_certificate_detail(page, isin)
                
                if cert:
                    # Filtra per sottostante
                    check_text = f"{cert.get('name', '')} {cert.get('underlying', '')}"
                    
                    if is_target_underlying(check_text):
                        certificates.append(cert)
                        price = cert.get("reference_price")
                        price_str = f"{price} EUR" if price else "N/A"
                        underlying = cert.get("underlying", "")[:25] or cert.get("name", "")[:25]
                        print(f"OK - {price_str} - {underlying}")
                    else:
                        skipped += 1
                        print(f"SKIP (single stock)")
                else:
                    errors += 1
                    print("FAIL")
                
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
            "errors": errors,
            "filter": "Indices, Commodities, Currencies, Rates, Credit Linked (NO single stocks)"
        },
        "certificates": certificates
    }
    
    with open(CONFIG["output_file"], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 60)
    print("COMPLETED")
    print(f"  Saved: {len(certificates)} certificates")
    print(f"  Skipped (stocks): {skipped}")
    print(f"  Errors: {errors}")
    print(f"  File: {CONFIG['output_file']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
