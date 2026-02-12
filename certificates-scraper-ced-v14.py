#!/usr/bin/env python3
# certificates-scraper-v24.1 - Ricerca Avanzata ROBUSTA

import json
import time
import re
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

CONFIG = {
    "output_file": "certificates-data.json",
    "max_certificates": 30,
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
    print("=== Scraper v24.1 - Ricerca Avanzata ULTRA ROBUSTA ===\n")

    certificates = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = context.new_page()

        url = "https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp"
        print(f"Apertura {url}")

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_load_state("networkidle", timeout=40000)
            time.sleep(6)
        except Exception as e:
            print(f"Errore caricamento pagina ricerca: {e}")
            browser.close()
            return

        # Debug: screenshot iniziale
        page.screenshot(path="ricerca-avanzata.png")

        # Aspetta e seleziona tipodb
        try:
            print("Aspetto select #tipodb...")
            page.wait_for_selector("#tipodb", timeout=30000)
            page.select_option("#tipodb", "2")
            print("Select #tipodb impostato su 2 (investment)")
        except PlaywrightTimeoutError:
            print("Timeout su #tipodb → fallback JS")
            try:
                page.evaluate('document.querySelector("#tipodb").value = "2";')
                print("Fallback JS: valore impostato")
            except:
                print("Fallito anche JS → salto selezione")
        except Exception as e:
            print(f"Errore select: {e}")
            page.screenshot(path="error-select.png")

        time.sleep(2)

        # Imposta data recente
        from_date = (datetime.now() - timedelta(days=90)).strftime("%d/%m/%Y")
        page.fill("#FiltroDal", from_date)
        time.sleep(1)

        # Submit
        try:
            print("Invio ricerca...")
            page.click("input[value='Avvia Ricerca']", timeout=15000)
            page.wait_for_load_state("networkidle", timeout=60000)
            time.sleep(8)
        except Exception as e:
            print(f"Errore submit: {e}")
            page.screenshot(path="error-submit.png")
            browser.close()
            return

        # Estrazione ISIN dai risultati
        soup = BeautifulSoup(page.content(), "html.parser")
        rows = soup.find_all("tr")

        isins = []
        for row in rows:
            tds = row.find_all("td")
            if len(tds) >= 1:
                isin = tds[0].get_text(strip=True)
                if re.match(r'^[A-Z0-9]{12}$', isin):
                    isins.append(isin)

        print(f"Trovati {len(isins)} ISIN")

        # Dettaglio (limitato)
        for i, isin in enumerate(isins[:CONFIG["max_certificates"]], 1):
            print(f"[{i}] {isin} → ", end="")
            detail_url = f"https://www.certificatiederivati.it/bs_promo_ugc.asp?t=ccollect&isin={isin}"
            page.goto(detail_url, wait_until="networkidle")
            time.sleep(6)

            detail_soup = BeautifulSoup(page.content(), "html.parser")

            cert = {
                "isin": isin,
                "name": "",
                "issuer": "",
                "type": "",
                "annual_coupon_yield": None,
                "barrier_down": None,
                "maturity_date": None,
                "url": detail_url,
                "scraped_at": datetime.now().isoformat()
            }

            h1 = detail_soup.find("h1")
            if h1:
                cert["name"] = h1.get_text(strip=True).split("ISIN")[0].strip()

            for row in detail_soup.find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
                if len(cells) >= 2:
                    label, value = cells[0].lower(), cells[1]
                    if "emittente" in label: cert["issuer"] = value
                    if "scadenza" in label: cert["maturity_date"] = parse_date(value)
                    if "barriera" in label: cert["barrier_down"] = clean_number(value)
                    if any(k in label for k in ["cedola", "coupon", "rendimento"]): cert["annual_coupon_yield"] = clean_number(value)

            if cert["name"]:
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
            "version": "24.1",
            "source": "certificatiederivati.it ricerca avanzata"
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(certificates)} certificati")

if __name__ == "__main__":
    main()
