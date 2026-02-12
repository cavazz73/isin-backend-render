#!/usr/bin/env python3
# certificates-scraper-v23.2 - DATI REALI con anti-timeout

import json
import time
import re
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup

CONFIG = {
    "timeout": 120000,
    "output_file": "certificates-data.json",
    "max_certificates": 10,          # aumenta dopo i test
    "headless": True,
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

def goto_with_retry(page, url, retries=3):
    for attempt in range(1, retries + 1):
        try:
            print(f"  Tentativo {attempt}/{retries} → goto {url}")
            page.goto(url, timeout=CONFIG["timeout"], wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle", timeout=30000)
            time.sleep(5)  # tempo extra per JS
            return True
        except PlaywrightTimeoutError as e:
            print(f"  Timeout al tentativo {attempt}: {str(e)}")
            time.sleep(5)
    print("  Tutti i tentativi falliti")
    return False

def extract_detail(page, isin):
    url = f"{CONFIG['base_url']}/bs_promo_ugc.asp?t=ccollect&isin={isin}"
    print(f"Visito scheda: {url}")

    if not goto_with_retry(page, url):
        return None

    try:
        # Aspetta elemento visibile (es. titolo o contenitore dati)
        page.wait_for_selector("h1, .title, .card, table", timeout=20000)
    except PlaywrightTimeoutError:
        print("  Timeout attesa caricamento contenuti")
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

    # Nome dal titolo
    title_tag = soup.find("h1") or soup.find("title")
    if title_tag:
        cert["name"] = title_tag.get_text(strip=True).split("ISIN")[0].strip()
        print(f"  Nome estratto: {cert['name'][:60]}...")

    # Cerca label + valore in tabelle o div
    for elem in soup.find_all(["td", "div", "span", "p", "th"]):
        text = elem.get_text(strip=True).lower()
        next_text = elem.find_next(["td", "span", "div"]).get_text(strip=True) if elem.find_next() else ""

        if "emittente" in text or "issuer" in text:
            cert["issuer"] = next_text
        elif "scadenza" in text or "maturity" in text:
            cert["maturity_date"] = parse_date(next_text)
        elif "barriera" in text or "barrier" in text:
            cert["barrier_down"] = clean_number(next_text)
        elif any(k in text for k in ["cedola", "coupon", "rendimento", "yield"]):
            cert["annual_coupon_yield"] = clean_number(next_text)
        elif "riferimento" in text or "ultimo" in text:
            cert["reference_price"] = clean_number(next_text)

    # Tabella sottostanti - cerca classi comuni o testo
    table = soup.find("table", class_=re.compile(r"(sottostante|underlying|basket|table|responsive)", re.I))
    if not table:
        table = soup.find("table")

    if table:
        rows = table.find_all("tr")
        print(f"  Trovata tabella con {len(rows)} righe")
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

    # Debug finale
    print(f"  Risultato: {len(cert['underlyings'])} sottostanti | Cedola: {cert['annual_coupon_yield']} | Barriera: {cert['barrier_down']}")

    if not cert["name"] or len(cert["underlyings"]) == 0:
        return None

    return cert

def main():
    print("=== Certificates Scraper v23.2 - DATI REALI con anti-timeout ===\n")

    certificates = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        # Raccolta ISIN
        url_list = f"{CONFIG['base_url']}/db_bs_nuove_emissioni.asp"
        print(f"Raccolta ISIN da {url_list}")
        if not goto_with_retry(page, url_list):
            print("Impossibile caricare pagina elenco")
            browser.close()
            return

        soup = BeautifulSoup(page.content(), "html.parser")
        isins = []
        for tr in soup.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) >= 1:
                isin = tds[0].get_text(strip=True)
                if re.match(r'^[A-Z0-9]{12}$', isin):
                    isins.append(isin)

        print(f"Trovati {len(isins)} ISIN candidati")

        # Processa dettaglio
        for i, isin in enumerate(isins[:CONFIG["max_certificates"]], 1):
            print(f"[{i}/{len(isins)}] {isin} → ", end="")
            cert = extract_detail(page, isin)
            if cert:
                certificates.append(cert)
                print("OK")
            else:
                print("fallito")
            time.sleep(4)  # pausa più lunga anti-ban

        browser.close()

    output = {
        "success": len(certificates) > 0,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "23.2",
            "source": "certificatiederivati.it - schede dettaglio"
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(certificates)} certificati salvati in {CONFIG['output_file']}")

if __name__ == "__main__":
    main()
