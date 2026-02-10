#!/usr/bin/env python3
"""
Certificates Scraper v14 - FUNZIONANTE
Focus: Ultime emissioni su Indici, Commodities, Valute, Tassi, Credit Linked
Esclude singoli titoli azionari
"""

import json
import re
import time
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

CONFIG = {
    "timeout": 60000,
    "output_file": "certificates-data.json",
    "max_certificates": 120
}

def is_good_underlying(text):
    """Filtro migliorato - accetta solo ciò che ci interessa"""
    if not text:
        return False
    t = text.lower()
    good_keywords = [
        "euro stoxx", "stoxx", "ftse mib", "s&p", "nasdaq", "dax", "cac", "nikkei",
        "brent", "wti", "gold", "silver", "platinum", "oil", "commodity",
        "eur/usd", "usd/jpy", "gbp/usd", "usd/chf", "valuta",
        "btp", "bund", "oat", "treasury", "tasso", "rate", "credit linked",
        "basket", "multi", "indice", "index"
    ]
    bad_keywords = ["s.p.a", "spa", "srl", "nv", "ltd", "inc", "ag", "sa"]
    
    if any(bad in t for bad in bad_keywords) and len(t.split()) < 6:
        return False
    return any(good in t for good in good_keywords)

def parse_number(text):
    if not text: return None
    try:
        cleaned = re.sub(r'[^\d,.-]', '', str(text))
        cleaned = cleaned.replace('.', '').replace(',', '.')
        return round(float(cleaned), 4)
    except:
        return None

def parse_date(text):
    if not text: return None
    try:
        if '/' in text:
            d, m, y = [x.strip() for x in text.split('/')]
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except:
        pass
    return text

def scrape_recent_isins(page):
    """Migliorata: cerca nelle pagine avvisi CERTX e SEDeX"""
    isins = set()
    urls = [
        "https://www.borsaitaliana.it/borsa/avvisi-negoziazione/certx/archive.html",
        "https://www.borsaitaliana.it/borsa/avvisi-negoziazione/sedex.html",
    ]
    
    for base in urls:
        for p in range(1, 8):   # prime 7 pagine
            url = f"{base}?page={p}" if p > 1 else base
            try:
                page.goto(url, timeout=CONFIG["timeout"], wait_until="networkidle")
                time.sleep(2.5)
                soup = BeautifulSoup(page.content(), "html.parser")
                
                for a in soup.find_all("a", href=True):
                    m = re.search(r"/scheda/([A-Z0-9]{12})\.html", a["href"])
                    if m:
                        isins.add(m.group(1))
            except:
                continue
    return list(isins)[:CONFIG["max_certificates"]]

def get_certificate(page, isin):
    urls = [
        f"https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{isin}.html",
        f"https://www.borsaitaliana.it/borsa/cw-e-certificates/eurotlx/scheda/{isin}.html",
    ]
    
    for url in urls:
        try:
            page.goto(url, timeout=CONFIG["timeout"], wait_until="networkidle")
            time.sleep(3)
            soup = BeautifulSoup(page.content(), "html.parser")
            
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
            
            # Nome
            h1 = soup.find("h1")
            if h1:
                cert["name"] = h1.get_text(strip=True).split(" - ")[0]
            
            # Tabella dati
            for row in soup.find_all("tr"):
                cells = row.find_all(["th", "td"])
                if len(cells) < 2: continue
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                
                if "emittente" in label: cert["issuer"] = value
                elif "tipologia" in label or "tipo" in label: cert["type"] = value
                elif "scadenza" in label: cert["maturity_date"] = parse_date(value)
                elif "barriera" in label: cert["barrier_down"] = parse_number(value)
                elif "cedola" in label or "coupon" in label or "rendimento" in label:
                    cert["annual_coupon_yield"] = parse_number(value)
                elif "riferimento" in label or "ultimo" in label:
                    cert["reference_price"] = parse_number(value)
                elif "denaro" in label or "bid" in label: cert["bid_price"] = parse_number(value)
                elif "lettera" in label or "ask" in label: cert["ask_price"] = parse_number(value)
            
            # Sottostanti
            underlying_text = ""
            for td in soup.find_all("td"):
                txt = td.get_text(strip=True)
                if any(x in txt.lower() for x in ["sottostante", "underlying", "basket"]):
                    underlying_text = txt
                    break
            
            if not is_good_underlying(underlying_text):
                return None
                
            if cert["name"]:
                return cert
                
        except:
            continue
    return None

def main():
    print("=== Certificates Scraper v17 - In esecuzione ===")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(user_agent="Mozilla/5.0").new_page()
        
        print("Raccolta ISIN recenti...")
        isins = scrape_recent_isins(page)
        print(f"Trovati {len(isins)} ISIN da analizzare")
        
        certificates = []
        for i, isin in enumerate(isins, 1):
            print(f"[{i}/{len(isins)}] {isin} → ", end="", flush=True)
            cert = get_certificate(page, isin)
            if cert:
                certificates.append(cert)
                print("OK")
            else:
                print("scartato")
            time.sleep(0.8)
        
        browser.close()
    
    output = {
        "success": True,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "17.0",
            "source": "borsaitaliana.it"
        }
    }
    
    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nFINITO → {len(certificates)} certificati salvati in {CONFIG['output_file']}")

if __name__ == "__main__":
    main()
