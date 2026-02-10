#!/usr/bin/env python3
"""
Certificates Scraper v18 – cerca ISIN nelle notizie di inizio negoziazione CERTX 2026
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
    "max_certificates": 80,
    "max_pages": 5   # quante pagine di avvisi visitare
}

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

def is_good_underlying(text):
    if not text: return False
    t = text.lower()
    good = ["stoxx", "ftse", "s&p", "nasdaq", "dax", "cac", "brent", "gold", "silver", "eur/usd", "btp", "bund", "credit linked", "basket", "indice", "commodity", "valuta"]
    bad_single = ["s.p.a", "spa", "s.r.l", "ltd", "inc", "ag"] 
    if any(b in t for b in bad_single) and "basket" not in t and len(t.split()) < 5:
        return False
    return any(g in t for g in good)

def collect_news_pages(page):
    base = "https://www.borsaitaliana.it/borsa/avvisi-negoziazione/certx/archive.html"
    news_urls = []
    
    try:
        page.goto(base, timeout=CONFIG["timeout"], wait_until="networkidle")
        time.sleep(3)
        soup = BeautifulSoup(page.content(), "html.parser")
        
        # Cerca link alle notizie di inizio negoziazione
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/borsa/avvisi-negoziazione/certx/" in href and re.search(r'/\d{4}/\d+\.html', href):
                full = "https://www.borsaitaliana.it" + href if href.startswith("/") else href
                news_urls.append(full)
    except Exception as e:
        print(f"Errore su pagina avvisi: {e}")
    
    return news_urls[:CONFIG["max_pages"] * 20]  # limite approssimativo

def extract_isin_from_news(page, news_url):
    try:
        page.goto(news_url, timeout=CONFIG["timeout"], wait_until="networkidle")
        time.sleep(2)
        soup = BeautifulSoup(page.content(), "html.parser")
        
        text = soup.get_text(" ", strip=True)
        # Cerca ISIN 12 caratteri alfanumerici
        isins = re.findall(r'\b([A-Z]{2}\d{10}|[A-Z]{1,2}\d{9}[A-Z]?)\b', text)
        return list(set(isins))  # unici
    except:
        return []

def get_certificate_data(page, isin):
    for prefix in ["", "-SEDX", "-ETLX"]:
        url = f"https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{isin}{prefix}.html"
        try:
            page.goto(url, timeout=CONFIG["timeout"], wait_until="networkidle")
            time.sleep(2.5)
            soup = BeautifulSoup(page.content(), "html.parser")
            
            if "strumento non trovato" in soup.get_text().lower() or "404" in soup.title.string.lower():
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
            
            for row in soup.find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["th","td"])]
                if len(cells) < 2: continue
                lbl, val = cells[0].lower(), cells[1]
                if "emittente" in lbl or "issuer" in lbl: cert["issuer"] = val
                if "tipologia" in lbl or "tipo" in lbl: cert["type"] = val
                if "scadenza" in lbl: cert["maturity_date"] = parse_date(val)
                if "barriera" in lbl: cert["barrier_down"] = parse_number(val)
                if "cedola" in lbl or "coupon" in lbl or "yield" in lbl or "rendimento" in lbl:
                    cert["annual_coupon_yield"] = parse_number(val)
                if "riferimento" in lbl or "ultimo" in lbl: cert["reference_price"] = parse_number(val)
            
            # Cerca testo sottostante
            for td in soup.find_all("td"):
                txt = td.get_text(strip=True)
                if any(k in txt.lower() for k in ["sottostante","underlying","basket","indici","commodit","valut","tasso"]):
                    cert["underlying_text"] = txt
                    break
            
            if cert["name"] and is_good_underlying(cert["underlying_text"]):
                return cert
        except:
            continue
    return None

def main():
    print("=== Certificates Scraper v18 – In esecuzione ===\n")
    
    certificates = []
    all_isins = set()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
        page = context.new_page()
        
        print("1. Raccolta pagine notizie inizio negoziazione...")
        news_urls = collect_news_pages(page)
        print(f"   Trovate {len(news_urls)} pagine notizia")
        
        print("2. Estrazione ISIN dalle notizie...")
        for i, url in enumerate(news_urls, 1):
            isins = extract_isin_from_news(page, url)
            for isin in isins:
                if len(isin) == 12 and isin not in all_isins:
                    all_isins.add(isin)
            print(f"   {i}/{len(news_urls)} → trovati {len(isins)} ISIN (totale unici: {len(all_isins)})")
        
        isin_list = list(all_isins)[:CONFIG["max_certificates"]]
        print(f"\n3. Analisi di {len(isin_list)} ISIN candidati...")
        
        for i, isin in enumerate(isin_list, 1):
            print(f"   [{i}/{len(isin_list)}] {isin} → ", end="", flush=True)
            cert = get_certificate_data(page, isin)
            if cert:
                certificates.append(cert)
                print("OK")
            else:
                print("scartato / non trovato")
            time.sleep(1.0)  # gentilezza verso il server
        
        browser.close()
    
    output = {
        "success": len(certificates) > 0,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "18.0",
            "source": "borsaitaliana.it - avvisi CERTX"
        }
    }
    
    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nFINITO → {len(certificates)} certificati salvati in {CONFIG['output_file']}")
    if len(certificates) == 0:
        print("Suggerimento: se ancora 0, controlla se Playwright carica JS correttamente o se il sito ha cambiato struttura ulteriore.")

if __name__ == "__main__":
    main()
