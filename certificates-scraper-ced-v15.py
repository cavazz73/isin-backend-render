#!/usr/bin/env python3
"""
Scraper CED v17 - SCAN COMPLETO 12k+ righe + Tutti emittenti
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

async def classify_sottostante(sott_str):
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
    """Scansione COMPLETA 12k+ righe."""
    print("🔍 SCAN COMPLETO: https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp")
    await page.goto('https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp', wait_until='networkidle')
    await page.wait_for_timeout(5000)
    
    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.select('table tr')
    print(f"📊 Totale righe: {len(rows)}")
    
    certificati = []
    emittenti_unici = set()
    
    for i, row in enumerate(rows):
        cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
        
        # Skip header e righe vuote
        if len(cols) < 6 or cols[0] in ['ISIN', 'NOME', 'EMITTENTE', 'SOTTOSTANTE']:
            continue
        
        # Estrai ISIN (prima colonna)
        isin = cols[0].strip()
        if not re.match(r'^[A-Z0-9]{12}$', isin):
            continue
            
        try:
            # Colonne fisse dalla struttura vista: [ISIN, NOME, EMITTENTE, SOTT., ?, DATA]
            nome = cols[1]
            emittente = cols[2]
            sottostante = cols[3]
            data_str = cols[-1]  # Data sempre ultima colonna
            
            data_em = datetime.strptime(data_str, DATE_FORMAT)
            if data_em >= cutoff_date:
                emittenti_unici.add(emittente)
                
                cert = {
                    'ISIN': isin,
                    'Nome': nome,
                    'Emittente': emittente,
                    'Sottostante': sottostante,
                    'Categoria_Sottostante': classify_sottostante(sottostante),
                    'Data_Emissione': data_str,
                    'Mercato': 'SeDeX'
                }
                certificati.append(cert)
                
                if len(certificati) % 50 == 0:
                    print(f"⏳ Processate {i}/{len(rows)} righe | {len(certificati)} recenti")
                    
        except (ValueError, IndexError):
            continue
    
    print(f"\n🎯 EMITTENTI TROVATI ({len(emittenti_unici)}): {sorted(list(emittenti_unici))}")
    return certificati

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        page = await context.new_page()
        
        certificati_recenti = await scrape_ced_completo(page)
        await browser.close()
        
        # Output per isin-research.com
        df = pd.DataFrame(certificati_recenti)
        df.to_json('certificates-recenti.json', orient='records', indent=2, date_format='iso')
        df.to_csv('certificates-recenti.csv', index=False)
        
        # Compatibilità certificates-data.json
        with open('certificates-data.json', 'w', encoding='utf-8') as f:
            json.dump(certificati_recenti, f, indent=2, ensure_ascii=False)
        
        print(f"\n🏆 SUCCESS: {len(certificati_recenti)} certificati recenti (da {cutoff_date.strftime(DATE_FORMAT)})")
        print("📊 Breakdown per categoria:")
        print(df['Categoria_Sottostante'].value_counts())
        print("📊 Per emittente:")
        print(df['Emittente'].value_counts().head(10))
        
        print("\n📁 File pronti per isin-research.com:")
        print("- certificates-recenti.json")
        print("- certificates-recenti.csv") 
        print("- certificates-data.json")

if __name__ == '__main__':
    asyncio.run(main())
