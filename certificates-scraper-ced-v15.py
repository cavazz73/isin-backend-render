#!/usr/bin/env python3
"""
Scraper CED v17 - dati mappati per API certificates-8.js
- Campi generati compatibili con:
  - c.isin, c.name, c.issuer, c.type
  - c.underlying, c.underlying_name, c.underlying_category
  - c.annual_coupon_yield, c.barrier, c.barrier_down
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
import re

RECENT_DAYS = int(os.getenv("RECENT_DAYS", "30"))
cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)
DATE_FORMAT = "%d/%m/%Y"


def classify_underlying_category(sott_str: str) -> str:
    if not sott_str:
        return "other"
    s = sott_str.lower()
    if "indice" in s or any(k in s for k in ["ftse", "dax", "sp", "euro stoxx", "nasdaq"]):
        return "index"
    if any(k in s for k in ["eur/", "usd/", "gbp/", "chf/", "jpy/", "fx", "valuta"]):
        return "fx"
    if any(k in s for k in ["euribor", "tasso", "rate", "eonia", "sonia", "libor"]):
        return "rate"
    if any(k in s for k in ["credit", "cln", "linked"]):
        return "credit"
    if any(k in s for k in ["basket", "worst of", "best of"]):
        return "basket"
    return "single"


async def scrape_ced_completo(page):
    print("🔍 SCAN COMPLETO: https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp")
    await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", wait_until="networkidle")
    await page.wait_for_timeout(5000)

    html = await page.content()
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tr")
    print(f"📊 Totale righe: {len(rows)}")

    certificati = []

    for i, row in enumerate(rows):
        cols = [col.get_text(strip=True) for col in row.find_all(["td", "th"])]

        # Skip header e righe troppo corte
        if len(cols) < 7 or cols[0] in ["ISIN", "NOME", "EMITTENTE", "SOTTOSTANTE"]:
            continue

        isin = cols[0].strip()
        if not re.match(r"^[A-Z0-9]{12}$", isin):
            continue

        try:
            # [0=ISIN,1=NOME,2=EMITTENTE,3=SOTT.,4=...,5=DATA,...]
            data_str = cols[5].strip()
            data_em = datetime.strptime(data_str, DATE_FORMAT)

            if data_em < cutoff_date:
                continue

            nome = cols[1].strip()
            emittente = cols[2].strip()
            sottostante = cols[3].strip()

            cert = {
                # 🔹 campi base attesi dal backend/frontend
                "isin": isin,
                "name": nome,
                "issuer": emittente,
                "type": "Certificato",  # CED non dà sempre il tipo, placeholder generico

                # 🔹 sottostanti
                "underlying": sottostante,
                "underlying_name": sottostante,
                "underlying_category": classify_underlying_category(sottostante),

                # 🔹 date & mercato
                "issue_date": data_str,
                "maturity_date": None,  # non disponibile in questa pagina
                "market": "SeDeX",

                # 🔹 campi numerici richiesti ma non presenti qui
                "price": None,
                "strike": None,
                "barrier": None,
                "barrier_down": None,
                "annual_coupon_yield": None,

                # 🔹 per compatibilità con API
                "scenario_analysis": None,
                "source": "CED_nuove_emissioni",
            }

            certificati.append(cert)

            if len(certificati) % 100 == 0:
                print(f"⏳ {len(certificati)} recenti | Riga {i}")

        except (ValueError, IndexError):
            continue

    print(f"🎯 Totale certificati recenti: {len(certificati)}")
    return certificati


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        certificati_recenti = await scrape_ced_completo(page)
        await browser.close()

        # DataFrame per debug/CSV
        df = pd.DataFrame(certificati_recenti)

        # JSON lista pura (come prima)
        df.to_json(
            "certificates-recenti.json",
            orient="records",
            indent=2,
            date_format="iso",
            force_ascii=False,
        )

        df.to_csv("certificates-recenti.csv", index=False)

        # JSON FORMATO ATTESO DA certificates-8.js
        backend_payload = {
            "success": True,
            "count": len(certificati_recenti),
            "certificates": certificati_recenti,
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "version": "ced-v17",
                "recent_days": RECENT_DAYS,
                "cutoff_date": cutoff_date.strftime(DATE_FORMAT),
                "sources": ["Certificati e Derivati - nuove emissioni"],
            },
        }

        with open("certificates-data.json", "w", encoding="utf-8") as f:
            json.dump(backend_payload, f, indent=2, ensure_ascii=False)

        print(f"\n🏆 SUCCESS: {backend_payload['count']} certificati recenti")
        print(f"📅 Da: {cutoff_date.strftime(DATE_FORMAT)}")

        if len(certificati_recenti) > 0:
            print("\n📊 Emittenti TOP 5:")
            print(df["issuer"].value_counts().head())
            print("\n📊 Categorie sottostante:")
            print(df["underlying_category"].value_counts())
            print(f"\n📋 Prime 3 ISIN: {list(df['isin'])[:3]}")
        else:
            print("❌ Nessun certificato recente trovato")


if __name__ == "__main__":
    asyncio.run(main())
