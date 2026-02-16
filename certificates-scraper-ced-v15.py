#!/usr/bin/env python3
"""
Scraper CED v16 - DEBUG COMPLETO + Fallback multi-sorgente
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import os
import time
import re
from datetime import datetime, timedelta

RECENT_DAYS = int(os.getenv('RECENT_DAYS', '30'))
cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)
DATE_FORMAT = '%d/%m/%Y'

async def classify_sottostante(sott_str):
    if not sott_str or len(sott_str) < 2:
        return 'Altro'
    sott_str = sott_str.lower()
    keywords = {
        'Indici': ['indice', 'ftse', 'dax', 'spx', 'euro stoxx', 's&p', 'nasdaq'],
        'Valute': ['eur', 'usd', 'gbp', 'chf', 'jpy', 'valuta', 'fx'],
        'Tassi': ['euribor', 'tasso', 'eonia', 'sonia'],
        'Credit Link': ['credit', 'cln']
    }
    for cat, kws in keywords.items():
        if any(kw in sott_str for kw in kws):
            return cat
    return 'Singolo'

async def scrape_ced_nuove(page):
    """Metodo 1: Tabella nuove emissioni."""
    print("🔍 METODO 1: db_bs_nuove_emissioni.asp")
    await page.goto('https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp', wait_until='networkidle')
    await page.wait_for_timeout(3000)
    
    # DEBUG: Salva HTML per analisi
    html = await page.content()
    with open('debug_ced.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("💾 HTML salvato in debug_ced.html")
    
    soup = BeautifulSoup(html, 'lxml')
    
    # Cerca TUTTI i possibili selettori tabella
    selectors = [
        'table tr',
        '.table tr', 
        '#table tr',
        'table.table tr'
    ]
    
    for selector in selectors:
        rows = soup.select(selector)
        print(f"   Selettore '{selector}': {len(rows)} righe trovate")
        if len(rows) > 10:  # Probabile tabella dati
            print(f"   🎯 PROVA selettore: {selector}")
            certificati = await parse_rows_smart(rows)
            if certificati:
                print(f"   ✅ {len(certificati)} certificati trovati!")
                return certificati
    
    return []

async def parse_rows_smart(rows):
    """Parse intelligente righe - rileva struttura automaticamente."""
    certificati = []
    
    for i, row in enumerate(rows[:50]):  # Solo prime 50 per test
        cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
        
        # Skip righe vuote/corte
        if len(cols) < 4 or all(len(c) < 2 for c in cols):
            continue
        
        print(f"   Riga {i}: {cols[:4]}...")  # DEBUG
        
        # Cerca ISIN (12 caratteri alfanumerici)
        isin_candidate = next((c for c in cols if re.match(r'^[A-Z]{2}[0-9A-Z]{9}[0-9]$', c)), None)
        if not isin_candidate:
            continue
            
        # Cerca data valida
        date_candidate = next((c for c in cols if re.match(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', c)), None)
        if not date_candidate:
            continue
            
        try:
            data_em = datetime.strptime(date_candidate, DATE_FORMAT)
            if data_em >= cutoff_date:
                certificati.append({
                    'ISIN': isin_candidate,
                    'Emittente': cols[0] if cols[0] != isin_candidate else 'N/D',
                    'Tipo': cols[1] if len(cols) > 1 else 'Certificato',
                    'Sottostante': cols[2] if len(cols) > 2 else 'N/D',
                    'Categoria_Sottostante': await classify_sottostante(cols[2] if len(cols) > 2 else ''),
                    'Data_Emissione': date_candidate,
                    'Mercato': 'SeDeX'
                })
                print(f"   ✅ CERTIFICATO: {isin_candidate}")
        except:
            continue
    
    return certificati

async def scrape_ced_homepage(page):
    """Fallback: Homepage CED."""
    print("🔍 METODO 2: Homepage come fallback")
    await page.goto('https://www.certificatiederivati.it/', wait_until='networkidle')
    # TODO: cerca link "Nuove Emissioni" e clicca
    return []

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = await context.new_page()
        
        # Prova metodo principale
        certificati = await scrape_ced_nuove(page)
        
        # Fallback se zero risultati
        if not certificati:
            certificati = await scrape_ced_homepage(page)
        
        await browser.close()
        
        # Output SEMPRE (anche vuoto)
        df = pd.DataFrame(certificati)
        df.to_json('certificates-recenti.json', orient='records', indent=2)
        df.to_csv('certificates-recenti.csv', index=False)
        
        print(f"\n🎯 RISULTATO FINALE: {len(certificati)} certificati recenti")
        if certificati:
            print("📊 Prime 3:", [c['ISIN'] for c in certificati[:3]])
        
        # COMPATIBILITÀ SITO
        with open('certificates-data.json', 'w') as f:
            json.dump(certificati, f, indent=2)

if __name__ == '__main__':
    asyncio.run(main())
