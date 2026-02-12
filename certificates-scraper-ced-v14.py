#!/usr/bin/env python3
# certificates-scraper-v23.3 - Blindata con debug HTML

import json
import time
import re
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup

CONFIG = {
    "timeout": 120000,
    "output_file": "certificates-data.json",
    "max_certificates": 15,
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

def safe_next_text(elem):
    next_el = elem.find_next(["td", "span", "div", "p"])
    return next_el.get_text(strip=True) if next_el else ""

def extract_detail(page, isin):
    url = f"{CONFIG['base_url']}/bs_promo_ugc.asp?t=ccollect&isin={isin}"
    print(f"Visito: {url}")

    try:
        page.goto(url, timeout=CONFIG["timeout"], wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=40000)
        time.sleep(6)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        # Debug: titolo e presenza sponsor
        title = soup.title.string if soup.title else ""
        print(f"  Titolo pagina: {title[:80]}...")

        if "sponsorizzata" in title.lower() or "unicredit" in title.lower():
            print("  Pagina sponsorizzata → salto")
            return None

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
            "underlyings": [],
            "url": url,
            "scraped_at": datetime.now().isoformat()
        }

        # Nome
        h1 = soup.find("h1")
        if h1:
            cert["name"] = h1.get_text(strip=True).split("ISIN")[0].strip()
            print(f"  Nome: {cert['name']}")

        # Ricerca label + valore (più sicura)
        for elem in soup.find_all(["td", "div", "span", "th", "p"]):
            text = elem.get_text(strip=True).lower()
            if not text: continue

            next_text = safe_next_text(elem)

            if any(k in text for k in ["emittente", "issuer"]):
                cert["issuer"] = next_text
            elif any(k in text for k in ["scadenza", "maturity"]):
                cert["maturity_date"] = parse_date(next_text)
            elif any(k in text for k in ["barriera", "barrier"]):
                cert["barrier_down"] = clean_number(next_text)
            elif any(k in text for k in ["cedola", "coupon", "rendimento", "yield"]):
                cert["annual_coupon_yield"] = clean_number(next_text)
            elif any(k in text for k in ["riferimento", "ultimo", "reference"]):
                cert["reference_price"] = clean_number(next_text)

        # Tabella sottostanti
        table = soup.find("table", string=re.compile(r"(Sottostante|Underlying|Basket|Strike|Spot)", re.I))
        if not table:
            table = soup.find("table")

        if table:
            print(f"  Trovata tabella sottostanti con {len(table.find_all('tr'))} righe")
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if len(tds) >= 3:
                    name = tds[0].get_text(strip=True)
                    strike = clean_number(tds[1].get_text(strip=True))
                    spot = clean_number(tds[2].get_text(strip=True))
                    barrier = clean_number(tds[3].get_text(strip=True)) if len(tds) > 3 else cert["barrier_down"]
                    cert["underlyings"].append({
                        "name": name,
                        "strike": strike,
                        "spot": spot,
                        "barrier": barrier,
                        "worst_of": "W" if "worst" in name.lower() else ""
                    })

        if cert["name"] and cert["underlyings"]:
            print(f"  SUCCESSO: {len(cert['underlyings'])} sottostanti, cedola {cert['annual_coupon_yield']}, barriera {cert['barrier_down']}")
            return cert
        else:
            print("  FALLITO: nome o sottostanti mancanti")
            # Debug HTML parziale se fallisce
            print("  HTML parziale (prime 1000 char):")
            print(html[:1000])
            return None

    except Exception as e:
        print(f"  ERRORE GENERICO: {str(e)}")
        return None

def main():
    print("=== Certificates Scraper v23.2 - DATI REALI DEBUG ===\n")

    certificates = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = context.new_page()

        # Raccolta ISIN
        list_url = f"{CONFIG['base_url']}/db_bs_nuove_emissioni.asp"
        print(f"Raccolta da {list_url}")
        page.goto(list_url, timeout=CONFIG["timeout"], wait_until="domcontentloaded")
        time.sleep(6)

        soup = BeautifulSoup(page.content(), "html.parser")
        isins = []
        for tr in soup.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) >= 1:
                isin = tds[0].get_text(strip=True)
                if re.match(r'^[A-Z0-9]{12}$', isin):
                    isins.append(isin)

        print(f"Trovati {len(isins)} ISIN")

        for i, isin in enumerate(isins[:CONFIG["max_certificates"]], 1):
            print(f"[{i}] {isin} → ", end="")
            cert = extract_detail(page, isin)
            if cert:
                certificates.append(cert)
                print("OK")
            else:
                print("fallito")
            time.sleep(5)

        browser.close()

    output = {
        "success": len(certificates) > 0,
        "count": len(certificates),
        "certificates": certificates,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "23.2",
            "source": "certificatiederivati.it"
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(certificates)} certificati")

if __name__ == "__main__":
    main()
