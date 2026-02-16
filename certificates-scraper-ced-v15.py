#!/usr/bin/env python3
"""
Scraper CED v15 FIX - Skip header + validazione colonne
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os
import time
import re

# Config
RECENT_DAYS = int(os.getenv('RECENT_DAYS', '30'))
URL_NUOVE = 'https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp'
cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)
DATE_FORMAT = '%d/%m/%Y'

async def classify_sottostante(sott_str):
    """Categorizza sottostante."""
    if not sott_str or len(sott_str) < 3:
        return 'Altro'
    sott_str = sott_str.lower()
    keywords = {
        'Indici': ['indice', 'ftse', 'dax', 'spx', 'euro stoxx', 's&p', 'nasdaq'],
        'Valute': ['eur', 'usd', 'gbp', 'chf', 'jpy', 'valuta', 'fx', 'cross'],
        'Tassi': ['euribor', 'tasso', 'eonia', 'sonia', 'libor', 'sofr'],
        'Credit Link': ['credit', 'cln', 'linked']
    }
    for cat, kws in keywords.items():
        if any(kw in sott_str for kw in kws):
            return cat
    return 'Singolo'  # Azioni/commodity

async def is_valid_date(date_str):
    """Valida se stringa è data reale (non header)."""
    if not date_str or len(date_str) < 5:
        return False
    if not re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{4}$', date_str):
        return False
    try:
        datetime.strptime(date_str, DATE_FORMAT)
        return True
    except ValueError:
        return False

async def scrape_ced(page):
    """Scraping CED con validazione robusta."""
    print(f"📡 Scraping {URL_NUOVE} (recenti da {cutoff_date.strftime(DATE_FORMAT)})")
    await page.goto(URL_NUOVE, wait_until='networkidle')
    await page.wait_for_selector('table', timeout=30000)
    
    content = await page.content()
    soup = BeautifulSoup(content, 'lxml')
    tables = soup.find_all('table')
    print(f"🔍 Trovate {len(tables)} tabelle")
    
    certificati = []
    for table_idx, table in enumerate(tables):
        rows = table.find_all('tr')
        print(f"📋 Tabella {table_idx}: {len(rows)} righe")
        
        for row_idx, row in enumerate(rows):
            cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
            
            # Skip righe troppo corte o header
            if len(cols) < 6:
                continue
            
            # Salta header (controlla prima colonna data)
            data_str = cols[-1] if len(cols) > 5 else ''  # Data ultima colonna tipicamente
            if not await is_valid_date(data_str):
                print(f"⚠️ Skip riga {row_idx} (header): '{data_str}'")
                continue
            
            try:
                # Adatta colonne: ISIN tipicamente prima, data ultima
                isin = cols[0] if len(cols[0]) == 12 else ''
                if not isin or not isin.isalnum():
                    continue
                    
                emittente = cols[1] if cols[1] else 'N/D'
                tipo = cols[2] if len(cols) > 2 else 'Certificato'
                sottostante = cols[3] if len(cols) > 3 else 'N/D'
                mercato = cols[-2] if len(cols) > 4 else 'SeDeX'
                
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
                    print(f"✅ {len(certificati)}: {isin} - {cert['Categoria_Sottostante']} - {data_str}")
                
            except (ValueError, IndexError) as e:
                print(f"⚠️ Skip riga {row_idx}: {e}")
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
        
        # Output
        df = pd.DataFrame(certificati_recenti)
        df.to_json('certificates-recenti.json', orient='records', date_format='iso', indent=2)
        df.to_csv('certificates-recenti.csv', index=False)
        
        # Compatibilità sito
        with open('certificates-data.json', 'w') as f:
            json.dump(certificati_recenti, f, indent=2, ensure_ascii=False)
        
        print(f"\n🏆 COMPLETATO: {len(certificati_recenti)} certificati recenti salvati!")
        print(f"📁 certificates-recenti.json/csv + certificates-data.json")
        
        if certificati_recenti:
            print("📊 Breakdown per categoria:")
            cats = pd.DataFrame(certificati_recenti)['Categoria_Sottostante'].value_counts()
            print(cats)

if __name__ == '__main__':
    asyncio.run(main())
