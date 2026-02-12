#!/usr/bin/env python3
# certificates-scraper-v23.1-reale-debug.py

import json
import time
import re
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup

CONFIG = {
    "timeout": 120000,
    "output_file": "certificates-data.json",
    "max_certificates": 10,  # per test veloce – aumenta dopo
    "headless": True,        # metti False per debug locale
    "base_url": "https://www.certificatiederivati.it"
}

def clean_number(text):
    if not text: return None
    text = re.sub(r'[^\d,\.-]', '', text.strip())
    text = text.replace('.', '').replace(',', '.')
    try: return round(float(text), 4)
    except: return None

def parse_date(text):
    if not text: return None
    try:
        if '/' in text:
            d, m, y = [x.strip() for x in text.split('/')]
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except: pass
    return text

def extract_detail(page, isin):
    url = f"{CONFIG['base_url']}/bs_promo_ugc.asp?t=ccollect&isin={isin}"
    print(f"Visito: {url}")
    try:
        page.goto(url, timeout=CONFIG["timeout"], wait_until="domcontentloaded")
        time.sleep(6)  # più tempo per JS

        # Aspetta che appaia almeno un elemento chiave (es. titolo o tabella)
        try:
            page.wait_for_selector("h1, .title, table", timeout=15000)
        except PlaywrightTimeoutError:
            print("Timeout attesa caricamento pagina")
            return None

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
            "url": url,
            "scraped_at": datetime.now().isoformat()
        }

        # Nome
        h1 = soup.find("h1") or soup.find("title")
        if h1:
            cert["name"] = h1.get_text(strip=True).split("ISIN")[0].strip()
            print(f"  Nome: {cert['name']}")

        # Cerca sezioni con label + valore
        for div in soup.find_all(["div", "p", "span", "td"]):
            text = div.get_text(strip=True).lower()
            if "emittente" in text or "issuer" in text:
                cert["issuer"] = div.find_next(["span", "td", "p"]).get_text(strip=True) if div.find_next() else ""
            elif "scadenza" in text or "maturity" in text:
                cert["maturity_date"] = parse_date(div.find_next(["span", "td"]).get_text(strip=True))
            elif "barriera" in text or "barrier" in text:
                cert["barrier_down"] = clean_number(div.find_next(["span", "td"]).get_text(strip=True))
            elif "cedola" in text or "coupon" in text or "rendimento" in text:
                cert["annual_coupon_yield"] = clean_number(div.find_next(["span", "td"]).get_text(strip=True))

        # Tabella sottostanti (cerca classi comuni)
        table = soup.find("table", class_=re.compile(r"(sottostante|underlying|basket|table-striped|table-responsive)", re.I))
        if not table:
            table = soup.find("table")  # fallback

        if table:
            rows = table.find_all("tr")
            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    name = cells[0].get_text(strip=True)
                    strike = clean_number(cells[1].get_text(strip=True))
                    spot = clean_number(cells[2].get_text(strip=True))
                    barrier = clean_number(cells[3].get_text(strip=True)) if len(cells) > 3 else cert["barrier_down"]
                    cert["underlyings"].append({
                        "name": name,
                        "strike": strike,
                        "spot": spot,
                        "barrier": barrier,
                        "worst_of": "W" if "worst" in name.lower() else ""
                    })

        # Log debug
        print(f"  Estratti: {len(cert['underlyings'])} sottostanti | Barriera: {cert['barrier_down']} | Cedola: {cert['annual_coupon_yield']}")

        if not cert["name"] or not cert["underlyings"]:
            return None

        return cert

    except Exception as e:
        print(f"  ERRORE: {str(e)}")
        return None

def main():
    print("=== Certificates Scraper v23.1 - DATI REALI DEBUG ===\n")

    certificates = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = context.new_page()

        # Raccolta ISIN
        page.goto(f"{CONFIG['base_url']}/db_bs_nuove_emissioni.asp")
        time.sleep(5)
        soup = BeautifulSoup(page.content(), "html.parser")

        isins = []
        for tr in soup.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) >= 1:
                isin = tds[0].get_text(strip=True)
                if re.match(r'^[A-Z0-9]{12}$', isin):
                    isins.append(isin)

        print(f"Trovati {len(isins)} ISIN candidati")

        # Dettaglio
        for i, isin in enumerate(isins[:CONFIG["max_certificates"]], 1):
            print(f"[{i}] {isin} → ", end="")
            cert = extract_detail(page, isin)
            if cert:
                certificates.append(cert)
                print("OK")
            else:
                print("fallito")
            time.sleep(3.5)  # anti-ban + caricamento

        browser.close()

    output = {
        "success": len(certificates) > 0,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "23.1-debug",
            "source": "certificatiederivati.it - schede dettaglio"
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(certificates)} certificati salvati")

if __name__ == "__main__":
    main()
