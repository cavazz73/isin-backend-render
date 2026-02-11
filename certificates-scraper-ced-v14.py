#!/usr/bin/env python3
"""
Certificates Scraper v20 - DEBUG MODE + più tentativi URL
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
    "max_certificates": 100
}

def parse_number(text):
    if not text: return None
    try:
        cleaned = re.sub(r'[^\d\.,-]', '', str(text))
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

def is_good_underlying(text):
    # TEMPORANEO: permissivo per debug - accetta quasi tutto se c'è testo
    if not text or len(text.strip()) < 5:
        return False
    return True  # ← Cambia a filtro reale dopo aver visto i log

def collect_isins(page):
    isins = set()
    urls = [
        "https://www.borsaitaliana.it/cw-e-certificates/covered-warrant/certificates.htm",
        "https://www.borsaitaliana.it/borsa/cw-e-certificates/ricerca-avanzata.html"
    ]
    
    for url in urls:
        try:
            page.goto(url, timeout=CONFIG["timeout"], wait_until="domcontentloaded")
            time.sleep(6)  # più tempo per JS
            soup = BeautifulSoup(page.content(), "html.parser")
            
            for a in soup.find_all("a", href=True):
                href = a["href"]
                m = re.search(r'/scheda/([A-Z0-9]{12})(?:-SEDX|-ETLX)?\.html', href)
                if m:
                    isins.add(m.group(1))
        except Exception as e:
            print(f"Errore su {url}: {str(e)}")
    
    return list(isins)[:CONFIG["max_certificates"]]

def get_certificate(page, isin):
    prefixes = ["", "-SEDX", "-ETLX", "-SEDEX", "-EUROTLX"]
    for prefix in prefixes:
        url = f"https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{isin}{prefix}.html"
        try:
            print(f"   Tentativo URL: {url}")
            response = page.goto(url, timeout=CONFIG["timeout"], wait_until="domcontentloaded")
            time.sleep(4)
            
            if response and response.status != 200:
                print(f"      Status: {response.status} → skip")
                continue
            
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            
            title_text = soup.title.string if soup.title else ""
            print(f"      Titolo pagina: {title_text[:80]}...")
            
            if "non trovato" in title_text.lower() or "404" in title_text:
                continue
            
            cert = {
                "isin": isin,
                "name": "",
                "issuer": "",
                "type": "",
                "underlying_text": "",
                "barrier_down": None,
                "maturity_date": None,
                "annual_coupon_yield": None,
                "reference_price": None,
                "url": url,
                "scraped_at": datetime.now().isoformat()
            }
            
            h1 = soup.find("h1")
            if h1:
                cert["name"] = h1.get_text(strip=True).split(" - ")[0].strip()
                print(f"      Nome estratto: {cert['name']}")
            
            underlying_found = ""
            for td in soup.find_all("td"):
                txt = td.get_text(strip=True)
                if len(txt) > 5 and any(k in txt.lower() for k in ["sottostante", "underlying", "basket", "indice", "commodity", "valuta", "tasso"]):
                    underlying_found = txt
                    break
            
            cert["underlying_text"] = underlying_found
            print(f"      Sottostante estratto: '{underlying_found[:100]}...'")
            
            # Parsing altri campi (breve)
            for tr in soup.find_all("tr"):
                cells = [c.get_text(strip=True) for c in tr.find_all(["th","td"])]
                if len(cells) < 2: continue
                lbl, val = cells[0].lower(), cells[1]
                if "emittente" in lbl: cert["issuer"] = val
                if "tipologia" in lbl or "tipo" in lbl: cert["type"] = val
                if "scadenza" in lbl: cert["maturity_date"] = parse_date(val)
                if "barriera" in lbl: cert["barrier_down"] = parse_number(val)
                if any(w in lbl for w in ["cedola", "coupon", "rendimento", "yield"]):
                    cert["annual_coupon_yield"] = parse_number(val)
                if any(w in lbl for w in ["riferimento", "ultimo", "close"]):
                    cert["reference_price"] = parse_number(val)
            
            if cert["name"] and is_good_underlying(cert["underlying_text"]):
                print("      → ACCETTO")
                return cert
            else:
                print("      → scartato (no name o underlying non buono)")
        except Exception as e:
            print(f"      Errore su {url}: {str(e)}")
            continue
    
    print("   Tutti i tentativi falliti")
    return None

def main():
    print("=== Certificates Scraper v20 - DEBUG + più URL ===\n")
    
    certificates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = context.new_page()
        
        print("Raccolta ISIN...")
        isins = collect_isins(page)
        print(f"Trovati {len(isins)} ISIN candidati")
        
        for i, isin in enumerate(isins, 1):
            print(f"[{i}/{len(isins)}] {isin}")
            cert = get_certificate(page, isin)
            if cert:
                certificates.append(cert)
            time.sleep(1.5)
        
        browser.close()
    
    output = {
        "success": len(certificates) > 0,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "20.0"
        }
    }
    
    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nFINITO → {len(certificates)} certificati salvati")
    if len(certificates) == 0:
        print("Guarda i log sopra per capire dove blocca (titolo pagina, sottostante estratto, errori).")
        print("Se continua a fallire → prossima opzione: scraping da vontobel.com o certificatiederivati.it")

if __name__ == "__main__":
    main()
