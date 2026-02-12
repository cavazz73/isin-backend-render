#!/usr/bin/env python3
# v26 - Multi-emittente ufficiale reale (2026)

import json
import time
from datetime import datetime
from playwright.sync_api import sync_playwright

CONFIG = {
    "output_file": "certificates-data.json",
    "max_per_source": 15,
    "headless": True
}

def extract_from_page(page, issuer):
    certs = []
    try:
        # Link generico: cerca a con href contenente 'prodotto', 'detail', 'isin', o testo 'Dettagli'
        links = page.eval_on_selector_all(
            'a[href*="prodotto"], a[href*="detail"], a[href*="isin"], a:contains("Dettagli"), a:contains("Scheda"), a:contains("ISIN")',
            "els => els.map(el => ({href: el.href, text: el.innerText.trim()}))"
        )

        for link in links:
            if not link['href']: continue
            if not any(x in link['href'].lower() for x in ['prodotto', 'detail', 'isin', 'scheda']): continue

            full_link = link['href'] if link['href'].startswith('http') else CONFIG["sources"][issuer] + link['href']
            page.goto(full_link, wait_until="networkidle", timeout=60000)
            time.sleep(4)

            title = page.eval_on_selector("h1, .title, .product-name, [class*='title']", "el => el ? el.innerText.trim() : ''")
            isin = page.eval_on_selector("*:contains('ISIN')", "el => el.innerText.match(/[A-Z0-9]{12}/)?.[0] || ''")

            if isin and title:
                cert = {
                    "isin": isin,
                    "name": title,
                    "issuer": issuer,
                    "type": "Phoenix / Cash Collect / Credit Linked",
                    "url": full_link,
                    "scraped_at": datetime.now().isoformat()
                }
                certs.append(cert)
                print(f"   ✓ {isin} - {title[:50]}... ({issuer})")
    except Exception as e:
        print(f"   Errore su {issuer}: {e}")

    return certs

def main():
    print("=== v26 - Multi-emittente ufficiale reale (2026) ===\n")

    all_certs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=CONFIG["headless"])
        page = browser.new_context(user_agent="Mozilla/5.0").new_page()

        sources = {
            "Vontobel": "https://certificati.vontobel.com/it/it/emissioni-recenti",
            "Leonteq": "https://www.leonteq.com/it/prodotti/nuove-emissioni",
            "BNP Paribas": "https://certificates.bnpparibas.com/it/it/emissioni-recenti",
            "UniCredit": "https://www.unicreditcertificates.it/it/it/emissioni-recenti",
            "Mediobanca": "https://www.mediobancacertificati.it/it/prodotti/nuove-emissioni",
            "Marex": "https://www.marexcertificates.com/it/prodotti/nuove-emissioni"
        }

        for issuer, url in sources.items():
            print(f"→ {issuer}")
            try:
                page.goto(url, wait_until="networkidle", timeout=60000)
                time.sleep(8)
                certs = extract_from_page(page, issuer)
                all_certs.extend(certs)
            except Exception as e:
                print(f"   Errore caricamento {issuer}: {e}")

            time.sleep(5)

        browser.close()

    output = {
        "success": len(all_certs) > 0,
        "count": len(all_certs),
        "certificates": all_certs,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "version": "26.0",
            "sources": list(sources.keys())
        }
    }

    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nFINITO → {len(all_certs)} certificati da emittenti ufficiali")

if __name__ == "__main__":
    main()
