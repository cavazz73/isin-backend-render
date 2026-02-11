#!/usr/bin/env python3
"""
Certificates Scraper v21 - COMPLETO
Fonti: certificatiederivati.it (Nuove Emissioni)
Emittenti: Tutti (Mediobanca, UniCredit, Marex, Leonteq, BNP, Vontobel, SG, UBS, Goldman, etc.)
Filtro: Indici, Commodities, Valute, Tassi, Credit Linked, Basket/Multi
Esclude: Singoli titoli azionari
"""

import json
import re
import time
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

CONFIG = {
    "timeout": 90000,
    "output_file": "certificates-data.json",
    "max_certificates": 150,
    "headless": True
}

def is_desired_underlying(text):
    if not text:
        return False
    t = text.lower()
    good = [
        "euro stoxx", "stoxx", "ftse mib", "s&p", "nasdaq", "dax", "cac", "nikkei",
        "brent", "wti", "gold", "silver", "platinum", "oil", "commodity", "orange juice",
        "eur/usd", "usd/jpy", "gbp/usd", "usd/chf", "eur/chf", "valuta", "currency",
        "btp", "bund", "oat", "treasury", "euribor", "tasso", "rate", "credit linked",
        "credit event", "basket", "multi", "worst of", "best of", "indice", "index"
    ]
    bad_single = ["s.p.a", "spa", "srl", "ltd", "inc", "ag", "sa", "nv", "stellantis", "ferrari", "eni", "intesa", "fineco", "unicredit", "mediobanca"]
    
    if any(b in t for b in bad_single) and not any(g in t for g in ["basket", "worst of", "multi"]):
        return False
    return any(g in t for g in good)

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

def scrape_nuove_emissioni():
    certificates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        page = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)").new_page()
        
        print("Apertura pagina Nuove Emissioni...")
        page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", 
                  timeout=CONFIG["timeout"], wait_until="networkidle")
        time.sleep(5)
        
        soup = BeautifulSoup(page.content(), "html.parser")
        rows = soup.find_all("tr")
        
        print(f"Trovate {len(rows)} righe nella tabella...")
        
        for row in rows[1:]:  # salta header
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
                
            isin = cells[0].get_text(strip=True)
            name = cells[1].get_text(strip=True)
            issuer = cells[2].get_text(strip=True)
            underlying = cells[3].get_text(strip=True)
            market = cells[4].get_text(strip=True)
            
            if not isin or len(isin) != 12:
                continue
                
            if not is_desired_underlying(underlying):
                continue  # filtra subito i singoli titoli
                
            cert = {
                "isin": isin,
                "name": name,
                "issuer": issuer,
                "type": "Cash Collect / Phoenix / Credit Linked",  # da raffinare in dettaglio
                "underlyings": [{"name": underlying}],
                "underlying_text": underlying,
                "barrier_down": None,
                "maturity_date": None,
                "annual_coupon_yield": None,
                "reference_price": None,
                "market": market,
                "url": f"https://www.certificatiederivati.it/bs_promo_ugc.asp?t=ccollect&isin={isin}",
                "scraped_at": datetime.now().isoformat()
            }
            
            certificates.append(cert)
            print(f"✓ {isin} | {issuer} | {underlying[:60]}...")
            
            if len(certificates) >= CONFIG["max_certificates"]:
                break
                
        browser.close()
    
    return certificates

def main():
    print("=== Certificates Scraper v21 - Tutti gli emittenti ===\n")
    
    certs = scrape_nuove_emissioni()
    
    output = {
        "success": True,
        "count": len(certs),
        "certificates": certs,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "21.0",
            "source": "certificatiederivati.it - Nuove Emissioni",
            "filter": "Indici, Commodities, Valute, Tassi, Credit Linked, Basket"
        }
    }
    
    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nFINITO → {len(certs)} certificati salvati in {CONFIG['output_file']}")
    print("Ora puoi caricare questo JSON nel tuo progetto isin-research.com")

if __name__ == "__main__":
    main()
