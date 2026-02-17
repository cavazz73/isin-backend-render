#!/usr/bin/env python3
"""
Scraper CED v17 - FIX DATE COLONNA 5 + Pandas Safe
- Estrae tutte le nuove emissioni da CED
- Filtra per ultimi RECENT_DAYS
- Salva:
  - certificates-recenti.json (lista pura)
  - certificates-recenti.csv
  - certificates-data.json (FORMATO BACKEND: success/count/certificates/metadata)
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
import re

RECENT_DAYS = int(os.getenv('RECENT_DAYS', '30'))
cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)
DATE_FORMAT = '%d/%m/%Y'


def classify_sottostante(sott_str):
    if not sott_str or len(sott_str) < 2:
        return 'Altro'
    sott_str = sott_str.lower()
    keywords = {
        'Indici': ['indice', 'ftse', 'dax', 'spx', 'euro stoxx', 's&p', 'nasdaq'],
        'Valute': ['eur', 'usd', 'gbp', 'chf', 'jpy', 'valuta', 'fx', 'cross'],
        'Tassi': ['euribor', 'tasso', 'eonia', 'sonia', 'libor'],
        'Credit Link': ['credit', 'cln', 'linked']
    }
    for cat, kws in keywords.items():
        if any(kw in sott_str for kw in kws):
            return cat
    return 'Singolo'


async def scrape_ced_completo(page):
    print("🔍 SCAN COMPLETO: https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp")
    await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", wait_until="networkidle")
    await page.wait_for_timeout(5000)

    html = await page.content()
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tr")
    print(f"📊 Totale righe: {len(rows)}")

    certificati = []
    emittenti_unici = set()

    for i, row in enumerate(rows):
        cols = [col.get_text(strip=True) for col in row.find_all(["td", "th"])]

        # Skip header e righe troppo corte
        if len(cols) < 7 or cols[0] in ["ISIN", "NOME", "EMITTENTE", "SOTTOSTANTE"]:
            continue

        # Validazione ISIN (12 char alfanumerici)
        isin = cols[0].strip()
        if not re.match(r"^[A-Z0-9]{12}$", isin):
            continue

        try:
            # STRUTTURA: [0=ISIN,1=NOME,2=EMITTENTE,3=SOTT.,4=...,5=DATA,...]
            data_str = cols[5].strip()
            data_em = datetime.strptime(data_str, DATE_FORMAT)

            if data_em >= cutoff_date:
                nome = cols[1]
                emittente = cols[2]
                sottostante = cols[3]

                emittenti_unici.add(emittente)

                cert = {
                    "ISIN": isin,
                    "Nome": nome,
                    "Emittente": emittente,
                    "Sottostante": sottostante,
                    "Categoria_Sottostante": classify_sottostante(sottostante),
                    "Data_Emissione": data_str,
                    "Mercato": "SeDeX",
                }
                certificati.append(cert)

                if len(certificati) % 50 == 0:
                    print(f"⏳ {len(certificati)} recenti | Riga {i}")

        except (ValueError, IndexError):
            continue

    print(f"🎯 EMITTENTI ({len(emittenti_unici)}): {sorted(emittenti_unici)}")
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

        # DataFrame per analisi/CSV
        df = pd.DataFrame(certificati_recenti)

        # 1) JSON lista pura (per debug / frontend)
        df.to_json(
            "certificates-recenti.json",
            orient="records",
            indent=2,
            date_format="iso",
            force_ascii=False,
        )

        # 2) CSV
        df.to_csv("certificates-recenti.csv", index=False)

        # 3) JSON FORMATO BACKEND (quello che il tuo server si aspetta)
        backend_payload = {
            "success": True,
            "count": len(certificati_recenti),
            "certificates": certificati_recenti,
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "version": "ced-v17",
                "sources": ["Certificati e Derivati"],
                "recent_days": RECENT_DAYS,
                "cutoff_date": cutoff_date.strftime(DATE_FORMAT),
            },
        }

        with open("certificates-data.json", "w", encoding="utf-8") as f:
            json.dump(backend_payload, f, indent=2, ensure_ascii=False)

        print(f"\n🏆 SUCCESS: {backend_payload['count']} certificati recenti")
        print(f"📅 Da: {cutoff_date.strftime(DATE_FORMAT)}")

        if len(certificati_recenti) > 0:
            print("\n📊 Emittenti TOP 5:")
            print(df["Emittente"].value_counts().head())
            print("\n📊 Categorie:")
            print(df["Categoria_Sottostante"].value_counts())
            print(f"\n📋 Prime 3: {list(df['ISIN'])[:3]}")
        else:
            print("❌ Nessun certificato recente trovato")


if __name__ == "__main__":
    asyncio.run(main())
