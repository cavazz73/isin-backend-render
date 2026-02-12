#!/usr/bin/env python3
# certificates-scraper-v23-reale.py
# Estrae dati reali dalle schede di dettaglio su certificatiederivati.it

import json
import time
import re
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup, Tag

CONFIG = {
    "timeout": 90000,
    "output_file": "certificates-data.json",
    "max_certificates": 40,           # limite per non essere bloccati
    "headless": True,
    "base_url": "https://www.certificatiederivati.it"
}

def clean_number(text):
    if not text:
        return None
    text = re.sub(r'[^\d,\.-]', '', text.strip())
    text = text.replace('.', '').replace(',', '.')
    try:
        return round(float(text), 4)
    except:
        return None

def parse_date(text):
    if not text:
        return None
    try:
        if '/' in text:
            d, m, y = [x.strip() for x in text.split('/')]
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except:
        pass
    return text

def extract_detail(page, isin):
    url = f"{CONFIG['base_url']}/bs_promo_ugc.asp?t=ccollect&isin={isin}"
    try:
        page.goto(url, timeout=CONFIG["timeout"], wait_until="networkidle")
        time.sleep(4)  # tempo per caricamento JS + tabelle

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        cert = {
            "isin": isin,
            "name": "",
            "issuer": "",
            "type": "",
            "currency": "EUR",
            "market": "",
            "annual_coupon_yield": None,
            "barrier_down": None,
            "maturity_date": None,
            "reference_price": None,
            "bid_price": None,
            "ask_price": None,
            "underlyings": [],
            "scenario_analysis": None,
            "url": url,
            "scraped_at": datetime.now().isoformat()
        }

        # Nome e tipo
        title = soup.find("h1") or soup.find("title")
        if title:
            cert["name"] = title.get_text(strip=True).split("ISIN")[0].strip()

        # Emittente, scadenza, barriera, rendimento (cerca label + valore)
        for row in soup.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
            if len(cells) >= 2:
                label, value = cells[0].lower(), cells[1]
                if any(k in label for k in ["emittente", "issuer"]):
                    cert["issuer"] = value
                elif any(k in label for k in ["scadenza", "maturity"]):
                    cert["maturity_date"] = parse_date(value)
                elif any(k in label for k in ["barriera", "barrier"]):
                    cert["barrier_down"] = clean_number(value)
                elif any(k in label for k in ["cedola", "coupon", "rendimento", "yield"]):
                    cert["annual_coupon_yield"] = clean_number(value)
                elif any(k in label for k in ["riferimento", "ultimo", "reference"]):
                    cert["reference_price"] = clean_number(value)
                elif "denaro" in label or "bid" in label:
                    cert["bid_price"] = clean_number(value)
                elif "lettera" in label or "ask" in label:
                    cert["ask_price"] = clean_number(value)

        # Tabella Sottostanti (cerca tabella con classi o id contenenti "sottostante")
        table = soup.find("table", string=re.compile(r"(Sottostante|Underlying|Basket)", re.I))
        if table:
            for tr in table.find_all("tr")[1:]:  # salta header
                tds = [td.get_text(strip=True) for td in tr.find_all("td")]
                if len(tds) >= 3:
                    name = tds[0]
                    strike = clean_number(tds[1]) if len(tds) > 1 else None
                    spot = clean_number(tds[2]) if len(tds) > 2 else None
                    barrier = clean_number(tds[3]) if len(tds) > 3 else cert["barrier_down"]
                    cert["underlyings"].append({
                        "name": name,
                        "strike": strike,
                        "spot": spot,
                        "barrier": barrier,
                        "worst_of": "W" if "worst" in name.lower() else ""
                    })

        if not cert["name"] or not cert["underlyings"]:
            return None

        return cert

    except Exception as e:
        print(f"Errore su {isin}: {str(e)}")
        return None

def main():
    print("=== Certificates Scraper v23 - DATI REALI ===\n")
    
    certificates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        page = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ).new_page()

        # 1. Vai su nuove emissioni e raccogli ISIN
        page.goto(f"{CONFIG['base_url']}/db_bs_nuove_emissioni.asp", timeout=CONFIG["timeout"])
        time.sleep(4)
        soup = BeautifulSoup(page.content(), "html.parser")

        isins = []
        for tr in soup.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) >= 1:
                isin = tds[0].get_text(strip=True)
                if re.match(r'^[A-Z0-9]{12}$', isin):
                    isins.append(isin)

        print(f"Trovati {len(isins)} ISIN candidati")

        # 2. Visita dettaglio per i primi N
        for i, isin in enumerate(isins[:CONFIG["max_certificates"]], 1):
            print(f"[{i}/{len(isins)}] {isin} ... ", end="", flush=True)
            cert = extract_detail(page, isin)
            if cert:
                certificates.append(cert)
                print("OK")
            else:
                print("fallito")
            time.sleep(2.5)  # anti-ban

        browser.close()

    # Salva
    output = {
        "success": len(certificates) > 0,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "23.0-reale",
            "source": "certificatiederivati.it - schede dettaglio"
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(certificates)} certificati reali salvati")

if __name__ == "__main__":
    main()
