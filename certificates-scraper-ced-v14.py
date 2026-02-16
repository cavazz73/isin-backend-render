#!/usr/bin/env python3
# v27 - Vontobel + Leonteq - Stabile e reale (2026)

import json
import time
from datetime import datetime
from playwright.sync_api import sync_playwright

CONFIG = {
    "output_file": "certificates-data.json",
    "max_per_source": 25,
    "headless": True
}

def extract_certs(page, issuer, base_url):
    certs = []
    try:
        page.goto(base_url, wait_until="networkidle", timeout=60000)
        time.sleep(8)

        # Cerca link ai prodotti (selettore molto generico)
        links = page.eval_on_selector_all(
            'a[href*="detail"], a[href*="prodotto"], a[href*="isin"], a[href*="scheda"], a',
            "els => els.map(el => el.href).filter(href => href && (href.includes('detail') || href.includes('prodotto') || href.includes('isin') || href.includes('scheda')))"
        )

        print(f"   Trovati {len(links)} link candidati su {issuer}")

        for link in links[:CONFIG["max_per_source"]]:
            if not link.startswith('http'):
                link = base_url.rsplit('/', 1)[0] + '/' + link.lstrip('/')
            page.goto(link, wait_until="networkidle", timeout=60000)
            time.sleep(4)

            title = page.eval_on_selector("h1, .title, .product-name, [class*='title']", "el => el ? el.innerText.trim() : ''")
            isin = page.eval_on_selector("*:contains('ISIN')", "el => el.innerText.match(/[A-Z0-9]{12}/)?.[0] || ''")

            if isin and title:
                cert = {
                    "isin": isin,
                    "name": title,
                    "issuer": issuer,
                    "type": "Phoenix / Cash Collect / Credit Linked",
                    "annual_coupon_yield": None,
                    "barrier_down": None,
                    "maturity_date": None,
                    "underlyings": [{"name": "Basket / Indice"}],
                    "url": link,
                    "scraped_at": datetime.now().isoformat()
                }
                certs.append(cert)
                print(f"   ✓ {isin} - {title[:60]}...")

    except Exception as e:
        print(f"   Errore su {issuer}: {e}")

    return certs

def main():
    print("=== v27 - Vontobel + Leonteq (migliore soluzione unica) ===\n")

    all_certs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        page = browser.new_context(user_agent="Mozilla/5.0").new_page()

        sources = [
            ("Vontobel", "https://certificati.vontobel.com/it/it/emissioni-recenti"),
            ("Leonteq", "https://www.leonteq.com/it/prodotti/nuove-emissioni")
        ]

        for issuer, url in sources:
            print(f"→ {issuer}")
            certs = extract_certs(page, issuer, url)
            all_certs.extend(certs)
            time.sleep(5)

        browser.close()

    output = {
        "success": len(all_certs) > 0,
        "count": len(all_certs),
        "certificates": all_certs,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "27.0",
            "sources": ["Vontobel", "Leonteq"]
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(all_certs)} certificati salvati")

if __name__ == "__main__":
    main()
