#!/usr/bin/env python3
"""
Certificates Scraper v19 – tenta ricerca avanzata + fallback emittenti
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

def is_good_underlying(text):
    if not text:
        return False
    t = text.lower()
    good = ["euro stoxx", "stoxx", "ftse mib", "s&p", "nasdaq", "dax", "cac", "brent", "gold", "silver", "eur/usd", "btp", "bund", "credit linked", "basket", "indice", "commodity", "valuta", "tasso"]
    bad = ["s.p.a", "spa", "srl", "nv", "ltd", "inc", "ag", "sa"]
    if any(b in t for b in bad) and "basket" not in t and len(t.split()) <= 5:
        return False
    return any(g in t for g in good)

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
            parts = [x.strip() for x in text.split('/')]
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
    except:
        pass
    return text

def collect_isins_from_search(page):
    isins = set()
    url = "https://www.borsaitaliana.it/borsa/cw-e-certificates/ricerca-avanzata.html"
    
    try:
        page.goto(url, timeout=CONFIG["timeout"], wait_until="networkidle")
        time.sleep(5)  # tempo per caricamento form JS
        
        # Tentativo manuale di interazione minima (se il form è visibile)
        # Se non funziona, fallback su pagina principale certificati
        page.evaluate("""
            () => {
                const select = document.querySelector('select[name="mercato"]');
                if (select) select.value = 'SEDX';
                const dateFrom = document.querySelector('input[name="dataInizio"]');
                if (dateFrom) dateFrom.value = '01/01/2026';
                // Clic su cerca se esiste
                const btn = document.querySelector('input[type="submit"], button[type="submit"]');
                if (btn) btn.click();
            }
        """)
        time.sleep(8)  # attesa risultati
        
        soup = BeautifulSoup(page.content(), "html.parser")
        
        for a in soup.find_all("a", href=True):
            m = re.search(r'/scheda/([A-Z0-9]{12})\.html', a['href'])
            if m:
                isins.add(m.group(1))
        
        # Fallback: pagina principale certificati
        if len(isins) < 10:
            page.goto("https://www.borsaitaliana.it/cw-e-certificates/covered-warrant/certificates.htm", timeout=CONFIG["timeout"], wait_until="networkidle")
            time.sleep(4)
            soup = BeautifulSoup(page.content(), "html.parser")
            for a in soup.find_all("a", href=True):
                m = re.search(r'/scheda/([A-Z0-9]{12})\.html', a['href'])
                if m:
                    isins.add(m.group(1))
    except Exception as e:
        print(f"Errore raccolta ISIN: {e}")
    
    return list(isins)[:CONFIG["max_certificates"]]

def get_certificate(page, isin):
    urls = [
        f"https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{isin}.html",
        f"https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{isin}-SEDX.html",
    ]
    
    for url in urls:
        try:
            page.goto(url, timeout=CONFIG["timeout"], wait_until="networkidle")
            time.sleep(3)
            soup = BeautifulSoup(page.content(), "html.parser")
            
            if "non trovato" in soup.get_text().lower() or "404" in str(soup.title):
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
            
            for tr in soup.find_all("tr"):
                tds = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
                if len(tds) < 2: continue
                lbl, val = tds[0].lower(), tds[1]
                if "emittente" in lbl: cert["issuer"] = val
                if "tipologia" in lbl or "tipo" in lbl: cert["type"] = val
                if "scadenza" in lbl: cert["maturity_date"] = parse_date(val)
                if "barriera" in lbl: cert["barrier_down"] = parse_number(val)
                if any(w in lbl for w in ["cedola", "coupon", "rendimento", "yield"]):
                    cert["annual_coupon_yield"] = parse_number(val)
                if any(w in lbl for w in ["riferimento", "ultimo", "close", "reference"]):
                    cert["reference_price"] = parse_number(val)
            
            # Underlying
            for td in soup.find_all("td"):
                txt = td.get_text(strip=True)
                if any(k in txt.lower() for k in ["sottostante", "underlying", "basket", "indici", "commodit", "valut", "tasso"]):
                    cert["underlying_text"] = txt
                    break
            
            if cert["name"] and is_good_underlying(cert["underlying_text"]):
                return cert
        except:
            continue
    return None

def main():
    print("=== Certificates Scraper v19 – Prova ricerca avanzata ===\n")
    
    certificates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)").new_page()
        
        print("Raccolta ISIN dalla ricerca avanzata / pagina principale...")
        isins = collect_isins_from_search(page)
        print(f"Trovati {len(isins)} ISIN candidati")
        
        for i, isin in enumerate(isins, 1):
            print(f"[{i}/{len(isins)}] {isin} → ", end="", flush=True)
            cert = get_certificate(page, isin)
            if cert:
                certificates.append(cert)
                print("OK")
            else:
                print("scartato / non trovato")
            time.sleep(1.2)
        
        browser.close()
    
    output = {
        "success": len(certificates) > 0,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "19.0",
            "note": "Filtro su indici/commodity/valute/tassi/credit linked"
        }
    }
    
    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nFINITO → {len(certificates)} certificati salvati")
    if len(certificates) == 0:
        print("Se ancora 0: il sito richiede interazione JS complessa o CAPTCHA → considera alternative come vontobel.com, leonteq.com o certificatiederivati.it")

if __name__ == "__main__":
    main()
