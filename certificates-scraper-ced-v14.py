#!/usr/bin/env python3
# Scraper per Borsa Italiana - Ricerca Avanzata (tutti emittenti, ultimi 90 giorni)
# Filtra per indici, valute, commodities, tassi, credit linked, basket

import json
import time
import re
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

CONFIG = {
    "output_file": "certificates-data.json",
    "max_certificates": 50,
    "headless": True,
    "filter_keywords": ["indice", "index", "basket", "worst of", "commodity", "valuta", "currency", "tasso", "rate", "credit linked"]
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

def is_desired_underlying(text):
    t = text.lower()
    for kw in CONFIG["filter_keywords"]:
        if kw in t:
            return True
    return False

def main():
    print("=== Scraper Borsa Italiana - Ricerca Avanzata ===\n")

    certificates = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        page = browser.new_context(user_agent="Mozilla/5.0").new_page()

        url = "https://www.borsaitaliana.it/borsa/cw-e-certificates/ricerca-avanzata.html"
        print(f"Apertura {url}")
        page.goto(url, wait_until="networkidle")
        time.sleep(5)

        # Imposta data inizio (ultimi 90 giorni)
        from_date = (datetime.now() - timedelta(days=90)).strftime("%d/%m/%Y")
        page.fill("input[name='dataInizio']", from_date)

        # Seleziona mercato SeDeX / CERT-X
        page.select_option("select[name='mercato']", "SEDX")

        # Submit form
        page.click("button[type='submit']", timeout=15000)
        page.wait_for_load_state("networkidle", timeout=60000)
        time.sleep(8)

        # Estrazione ISIN, nome, emittente, sottostante
        soup = BeautifulSoup(page.content(), "html.parser")
        table = soup.find("table", class_="t-table")
        if table:
            rows = table.find_all("tr")[1:]  # salta header
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 5:
                    isin = cells[0]
                    name = cells[1]
                    issuer = cells[2]
                    underlying = cells[3]
                    type = cells[4]

                    if is_desired_underlying(underlying):
                        cert = {
                            "isin": isin,
                            "name": name,
                            "issuer": issuer,
                            "underlying_text": underlying,
                            "type": type,
                            "annual_coupon_yield": None,
                            "barrier_down": None,
                            "maturity_date": None,
                            "reference_price": None,
                            "url": f"https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{isin}.html",
                            "scraped_at": datetime.now().isoformat()
                        }
                        certificates.append(cert)
                        print(f"✓ {isin} - {issuer} - {underlying[:60]}...")

        browser.close()

    output = {
        "success": True,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "28.0",
            "source": "borsaitaliana.it - ricerca avanzata"
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(certificates)} certificati salvati")

if __name__ == "__main__":
    main()
