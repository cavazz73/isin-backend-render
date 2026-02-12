#!/usr/bin/env python3
# certificates-scraper-v25-multi-fonte-reale.py

import json
import time
import re
from datetime import datetime
from playwright.sync_api import sync_playwright

CONFIG = {
    "output_file": "certificates-data.json",
    "max_certificates": 50,
    "headless": True
}

def clean_number(text):
    if not text: return None
    text = re.sub(r'[^\d,\.-]', '', str(text).strip())
    text = text.replace('.', '').replace(',', '.')
    try: return round(float(text), 4)
    except: return None

def main():
    print("=== Scraper v25 - MULTI FONTE REALE (Vontobel + Leonteq + BNP) ===\n")

    all_certs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        page = browser.new_context(user_agent="Mozilla/5.0").new_page()

        sources = [
            ("Vontobel", "https://certificati.vontobel.com/it/it/emissioni-recenti"),
            ("Leonteq", "https://www.leonteq.com/it/prodotti/nuove-emissioni"),
            ("BNP Paribas", "https://certificates.bnpparibas.it/it/emissioni-recenti")
        ]

        for name, url in sources:
            print(f"\n→ Caricamento {name}...")
            page.goto(url, wait_until="networkidle")
            time.sleep(6)

            # Estrazione link (pattern comune)
            links = page.eval_on_selector_all("a[href*='prodotto'], a[href*='detail'], a[href*='isin']", 
                                             "els => els.map(el => el.href)")

            print(f"   Trovati {len(links)} link candidati su {name}")

            for link in links[:15]:  # limite per fonte
                try:
                    page.goto(link, wait_until="networkidle")
                    time.sleep(4)

                    title = page.eval_on_selector("h1, .title, .product-name", "el => el ? el.innerText.trim() : ''")
                    isin = page.eval_on_selector("text=/[A-Z0-9]{12}/", "el => el ? el.innerText.match(/[A-Z0-9]{12}/)?.[0] : ''")

                    if isin and title:
                        cert = {
                            "isin": isin,
                            "name": title,
                            "issuer": name,
                            "type": "Phoenix / Cash Collect / Credit Linked",
                            "annual_coupon_yield": None,
                            "barrier_down": None,
                            "maturity_date": None,
                            "underlyings": [{"name": "Basket / Indice"}],
                            "url": link,
                            "scraped_at": datetime.now().isoformat()
                        }
                        all_certs.append(cert)
                        print(f"   ✓ {isin} → {name}")
                except:
                    continue

                time.sleep(3)

        browser.close()

    # Salva
    output = {
        "success": True,
        "count": len(all_certs),
        "certificates": all_certs,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "25.0-multi",
            "source": "Vontobel + Leonteq + BNP"
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(all_certs)} certificati multi-emittente salvati")

if __name__ == "__main__":
    main()
