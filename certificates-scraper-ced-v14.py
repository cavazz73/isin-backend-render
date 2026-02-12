#!/usr/bin/env python3
# certificates-scraper-v24-reale-ricerca-avanzata.py

import json
import time
import re
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

CONFIG = {
    "output_file": "certificates-data.json",
    "max_certificates": 40,
    "headless": True
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

def main():
    print("=== Scraper v24 - Ricerca Avanzata Reale ===\n")

    certificates = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        page = browser.new_context(user_agent="Mozilla/5.0").new_page()

        # 1. Apri pagina ricerca avanzata
        print("Apertura ricerca avanzata...")
        page.goto("https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp", wait_until="networkidle")
        time.sleep(4)

        # 2. Seleziona "investment" (db=2)
        page.select_option("#tipodb", "2")
        time.sleep(1)

        # 3. Imposta data scadenza ultimi 90 giorni
        from_date = (datetime.now() - timedelta(days=90)).strftime("%d/%m/%Y")
        page.fill("#FiltroDal", from_date)
        time.sleep(1)

        # 4. Submit form
        print("Invio ricerca...")
        page.click("input[value='Avvia Ricerca']")
        page.wait_for_load_state("networkidle")
        time.sleep(6)

        # 5. Parse risultati
        soup = BeautifulSoup(page.content(), "html.parser")
        rows = soup.find_all("tr")

        isins = []
        for row in rows:
            tds = row.find_all("td")
            if len(tds) >= 1:
                isin = tds[0].get_text(strip=True)
                if re.match(r'^[A-Z0-9]{12}$', isin):
                    isins.append(isin)

        print(f"Trovati {len(isins)} ISIN recenti")

        # 6. Dettaglio per ogni ISIN (limitato)
        for i, isin in enumerate(isins[:CONFIG["max_certificates"]], 1):
            print(f"[{i}/{len(isins)}] {isin} → ", end="", flush=True)
            
            detail_url = f"https://www.certificatiederivati.it/bs_promo_ugc.asp?t=ccollect&isin={isin}"
            page.goto(detail_url, wait_until="networkidle")
            time.sleep(5)

            detail_soup = BeautifulSoup(page.content(), "html.parser")

            cert = {
                "isin": isin,
                "name": "",
                "issuer": "",
                "type": "Cash Collect / Phoenix / Credit Linked",
                "underlyings": [],
                "barrier_down": None,
                "maturity_date": None,
                "annual_coupon_yield": None,
                "reference_price": None,
                "url": detail_url,
                "scraped_at": datetime.now().isoformat()
            }

            # Nome
            h1 = detail_soup.find("h1")
            if h1:
                cert["name"] = h1.get_text(strip=True).split("ISIN")[0].strip()

            # Emittente, scadenza, barriera, rendimento
            for row in detail_soup.find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
                if len(cells) >= 2:
                    label, value = cells[0].lower(), cells[1]
                    if "emittente" in label:
                        cert["issuer"] = value
                    elif "scadenza" in label:
                        cert["maturity_date"] = parse_date(value)
                    elif "barriera" in label:
                        cert["barrier_down"] = clean_number(value)
                    elif any(k in label for k in ["cedola", "coupon", "rendimento", "yield"]):
                        cert["annual_coupon_yield"] = clean_number(value)

            # Sottostanti (tabella)
            table = detail_soup.find("table")
            if table:
                for tr in table.find_all("tr")[1:]:
                    tds = [td.get_text(strip=True) for td in tr.find_all("td")]
                    if len(tds) >= 1:
                        name = tds[0]
                        cert["underlyings"].append({"name": name})

            if cert["name"]:
                certificates.append(cert)
                print("OK")
            else:
                print("fallito")

            time.sleep(3)

        browser.close()

    # Salva
    output = {
        "success": True,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "24.0",
            "source": "certificatiederivati.it - ricerca avanzata"
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(certificates)} certificati reali salvati")

if __name__ == "__main__":
    main()
