#!/usr/bin/env python3
# certificates-scraper-v24-vontobel-reale.py

import json
import time
import re
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

CONFIG = {
    "timeout": 90000,
    "output_file": "certificates-data.json",
    "max_certificates": 40,
    "headless": True,
    "base_url": "https://certificati.vontobel.com"
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
        if '.' in text:
            d, m, y = [x.strip() for x in text.split('.')]
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except: pass
    return text

def extract_vontobel_cert(page, url):
    try:
        page.goto(url, timeout=CONFIG["timeout"], wait_until="networkidle")
        time.sleep(4)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        cert = {
            "isin": "",
            "name": "",
            "issuer": "Vontobel",
            "type": "",
            "currency": "EUR",
            "annual_coupon_yield": None,
            "barrier_down": None,
            "maturity_date": None,
            "reference_price": None,
            "underlyings": [],
            "url": url,
            "scraped_at": datetime.now().isoformat()
        }

        # Nome
        h1 = soup.find("h1", class_=re.compile("product-title|headline"))
        if h1:
            cert["name"] = h1.get_text(strip=True)

        # ISIN (cerca testo con ISIN)
        isin_tag = soup.find(string=re.compile(r"ISIN|WKN", re.I))
        if isin_tag:
            parent = isin_tag.find_parent(["div", "span", "p"])
            if parent:
                cert["isin"] = parent.get_text(strip=True).split("ISIN")[-1].strip()[:12]

        # Scadenza, barriera, cedola
        for row in soup.find_all("div", class_=re.compile("key-figure|detail-row|product-detail")):
            text = row.get_text(strip=True).lower()
            if "scadenza" in text or "maturity" in text:
                cert["maturity_date"] = parse_date(row.find_next(string=True))
            elif "barriera" in text or "barrier" in text:
                cert["barrier_down"] = clean_number(row.find_next(string=True))
            elif "cedola" in text or "coupon" in text or "rendimento" in text:
                cert["annual_coupon_yield"] = clean_number(row.find_next(string=True))

        # Sottostanti
        underlying_section = soup.find(string=re.compile(r"Sottostante|Underlying|Basket", re.I))
        if underlying_section:
            parent = underlying_section.find_parent(["div", "section"])
            if parent:
                for li in parent.find_all("li"):
                    name = li.get_text(strip=True)
                    cert["underlyings"].append({"name": name, "worst_of": "W" if "worst" in name.lower() else ""})

        if not cert["isin"] or not cert["name"]:
            return None

        print(f"  OK → {cert['name']} | Cedola: {cert['annual_coupon_yield']} | Barriera: {cert['barrier_down']}")
        return cert

    except Exception as e:
        print(f"  Errore: {str(e)}")
        return None

def main():
    print("=== Vontobel Real Scraper v24 ===\n")

    certificates = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        page = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)").new_page()

        # Vai su emissioni recenti
        url = f"{CONFIG['base_url']}/it/it/emissioni-recenti"
        print(f"Carico {url}")
        page.goto(url, timeout=CONFIG["timeout"], wait_until="networkidle")
        time.sleep(5)

        soup = BeautifulSoup(page.content(), "html.parser")

        # Cerca link ai certificati
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/it/it/prodotto/" in href or "isin" in href.lower():
                full_url = CONFIG["base_url"] + href if href.startswith("/") else href
                links.append(full_url)

        print(f"Trovati {len(links)} link candidati")

        for i, url in enumerate(links[:CONFIG["max_certificates"]], 1):
            print(f"[{i}] {url} → ", end="")
            cert = extract_vontobel_cert(page, url)
            if cert:
                certificates.append(cert)
                print("OK")
            else:
                print("fallito")
            time.sleep(4)

        browser.close()

    output = {
        "success": len(certificates) > 0,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "24.0-vontobel",
            "source": "vontobel.com/it/emissioni-recenti"
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(certificates)} certificati reali salvati")

if __name__ == "__main__":
    main()
