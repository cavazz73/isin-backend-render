#!/usr/bin/env python3
"""
Scraper CED v15 per isin-research.com - SOLO RECENTI (30gg) con categoria sottostanti
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os
import time

# Config
RECENT_DAYS = int(os.getenv('RECENT_DAYS', '30'))
URL_NUOVE = 'https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp'
cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)
DATE_FORMAT = '%d/%m/%Y'

async def classify_sottostante(sott_str):
    """Categorizza sottostante."""
    if not sott_str:
        return 'Altro'
    sott_str = sott_str.lower()
    if any(kw in sott_str for kw in ['indice', 'ftse', 'dax', 'spx', 'euro stoxx', 's&p']):
        return 'Indici'
    elif any(kw in sott_str for kw in ['eur', 'usd', 'gbp', 'valuta', 'fx', 'cross']):
        return 'Valute'
    elif any(kw in sott_str for kw in ['euribor', 'tasso', 'eonia', 'sonia', 'libor']):
        return 'Tassi'
    elif any(kw in sott_str for kw in ['credit', 'cln', 'linked']):
        return 'Credit Link'
    return 'Singolo'  # Azioni/commodity

async def scrape_ced(page):
    """Scraping nuove emissioni CED."""
    print(f"📡 Scraping {URL_NUOVE} (recenti da {cutoff_date.strftime(DATE_FORMAT)})")
    await page.goto(URL_NUOVE, wait_until='networkidle')
    await page.wait_for_selector('table', timeout=30000)
    
    content = await page.content()
    soup = BeautifulSoup(content, 'lxml')
    rows = soup.select('table tr')[1:]  # Skip header
    
    certificati = []
    for i, row in enumerate(rows):
        cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
        if len(cols) < 6:
            continue
        try:
            isin, emittente, tipo, sottostante, data_str, mercato = cols[:6]
            data_em = datetime.strptime(data_str, DATE_FORMAT)
            if data_em >= cutoff_date:
                cert = {
                    'ISIN': isin,
                    'Emittente': emittente,
                    'Tipo': tipo,
                    'Sottostante': sottostante,
                    'Categoria_Sottostante': await classify_sottostante(sottostante),
                    'Data_Emissione': data_str,
                    'Mercato': mercato,
                    'Recente': 'Sì'
                }
                certificati.append(cert)
                print(f"✅ {len(certificati)}: {isin} - {cert['Categoria_Sottostante']}")
        except ValueError as e:
            print(f"⚠️ Skip riga {i}: {e}")
            continue
    
    return certificati

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = await context.new_page()
        
        certificati_recenti = await scrape_ced(page)
        await browser.close()
        
        # Output per sito
        df = pd.DataFrame(certificati_recenti)
        df.to_json('certificates-recenti.json', orient='records', date_format='iso', indent=2)
        df.to_csv('certificates-recenti.csv', index=False)
        
        # Compatibilità vecchio formato
        with open('certificates-data.json', 'w') as f:
            json.dump(certificati_recenti, f, indent=2, ensure_ascii=False)
        
        print(f"🏆 COMPLETATO: {len(certificati_recenti)} certificati recenti salvati!")
        print(f"📁 File: certificates-recenti.json/csv + certificates-data.json")

if __name__ == '__main__':
    asyncio.run(main())
