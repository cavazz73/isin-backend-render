#!/usr/bin/env python3
# v26 - Multi-emittente reale: Vontobel + Leonteq + BNP

import json
import time
from datetime import datetime
from playwright.sync_api import sync_playwright

CONFIG = {
    "output_file": "certificates-data.json",
    "max_per_source": 20,
    "headless": True
}

def extract_cert(page, issuer):
    certs = []
    try:
        # Link prodotti (più generico possibile)
        links = page.eval_on_selector_all(
            "a[href*='prodotto'], a[href*='detail'], a[href*='isin'], a:contains('Dettagli'), a:contains('Scheda'), a:contains('ISIN')",
            "els => els.map(el => el.href)"
        )

        for link in links[:CONFIG["max_per_source"]]:
            page.goto(link, wait_until="networkidle", timeout=60000)
            time.sleep(4)

            title = page.eval_on_selector("h1, .title, .product-name", "el => el ? el.innerText.trim() : ''")
            isin = page.eval_on_selector("text, span, div", "el => el.innerText.match(/[A-Z0-9]{12}/)?.[0] || ''")

            if isin and title:
                cert = {
                    "isin": isin,
                    "name": title,
                    "issuer": issuer,
                    "type": "Phoenix / Cash Collect / Credit Linked",
                    "url": link,
                    "scraped_at": datetime.now().isoformat()
                }
                certs.append(cert)
                print(f"   ✓ {isin} - {title[:50]}...")
    except Exception as e:
        print(f"Errore su {issuer}: {e}")

    return certs

def main():
    print("=== v26 - Multi-emittente reale ===\n")

    all_certs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        page = browser.new_context(user_agent="Mozilla/5.0").new_page()

        sources = [
            ("Vontobel", "https://certificati.vontobel.com/it/it/emissioni-recenti"),
            ("Leonteq", "https://www.leonteq.com/it/prodotti/nuove-emissioni"),
            ("BNP Paribas", "https://certificates.bnpparibas.com/it/it/emissioni-recenti")
        ]

        for name, url in sources:
            print(f"→ {name}")
            page.goto(url, wait_until="networkidle")
            time.sleep(8)
            certs = extract_cert(page, name)
            all_certs.extend(certs)
            time.sleep(5)

        browser.close()

    output = {
        "success": True,
        "count": len(all_certs),
        "certificates": all_certs,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "26.0",
            "sources": [s[0] for s in sources]
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(all_certs)} certificati salvati")

if __name__ == "__main__":
    main()
