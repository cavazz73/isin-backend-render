#!/usr/bin/env python3
"""
CED Scraper v20 - Fix Playwright + Filtro Indici/Commodities/Valute/Tassi/Credit
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
import re
from typing import List, Dict

RECENT_DAYS = int(os.getenv('RECENT_DAYS', '30'))
MAX_DETAIL_ISIN = int(os.getenv('MAX_DETAIL_ISIN', '50'))

cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)
DATE_FORMAT = '%d/%m/%Y'

def is_valid_underlying(name: str) -> bool:
    n = name.lower()
    valid = [
        # Indici
        'nikkei', 'sp500', 's&p', 'dow', 'dax', 'cac', 'eurostoxx', 'ftse', 
        # Commodities  
        'oro', 'gold', 'silver', 'petrolio', 'oil', 'brent', 'wti', 'gas',
        # Valute
        'eur', 'usd', 'gbp', 'chf', 'jpy', 'fx',
        # Tassi
        'euribor', 'eonia', 'libor', 'swap', 'yield',
        # Credit
        'credit', 'cln', 'cds'
    ]
    return any(kw in n for kw in valid)

def has_stocks(underlyings: List[str]) -> bool:
    stocks = ['enel', 'eni', 'intesa', 'intel', 'asml', 'tesla', 'apple']
    return any(any(s in u.lower() for s in stocks) for u in underlyings)

async def scrape_listing(page) -> List[Dict]:
    print("📋 Scraping listing...")
    await page.goto('https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp', wait_until='networkidle')
    await page.wait_for_timeout(5000)
    
    soup = BeautifulSoup(await page.content(), 'lxml')
    rows = soup.select('table tr')
    
    certs = []
    for row in rows:
        cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
        if len(cols) < 7: continue
        
        isin = cols[0]
        if not re.match(r'^[A-Z0-9]{12}$', isin): continue
        
        try:
            data_em = datetime.strptime(cols[5], DATE_FORMAT)
            if data_em < cutoff_date: continue
            
            certs.append({
                'isin': isin, 'name': cols[1], 'issuer': cols[2],
                'type': 'Certificato', 'underlying': cols[3],
                'underlying_name': None, 'underlying_category': None,
                'issue_date': cols[5], 'maturity_date': None,
                'market': 'SeDeX', 'price': None,
                'barrier': None, 'barrier_down': None,
                'annual_coupon_yield': None, 'coupon_frequency': 'annual',
                'underlyings': [], 'source': 'CED_v20'
            })
        except: continue
    
    print(f"✅ {len(certs)} recenti")
    return certs[:MAX_DETAIL_ISIN * 2]

async def scrape_detail(page, cert: Dict) -> bool:
    isin = cert['isin']
    print(f"🔍 {isin}", end=" ")
    
    try:
        await page.goto(f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}", 
                       wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(3000)
        
        soup = BeautifulSoup(await page.content(), 'lxml')
        
        # Sottostanti
        underlyings = []
        for panel in soup.find_all('div', class_='panel'):
            if 'sottostante' in panel.get_text().lower():
                table = panel.find('table')
                if table:
                    for row in table.find('tbody').find_all('tr'):
                        cols = [c.get_text(strip=True) for c in row.find_all('td')]
                        if cols: underlyings.append(cols[0])
                break
        
        if not underlyings:
            print("⚠️ No data")
            return False
        
        # Filtri
        if has_stocks(underlyings):
            print("❌ Stocks")
            return False
        if not any(is_valid_underlying(u) for u in underlyings):
            print("❌ Invalid")
            return False
        
        print(f"✅ {underlyings[0][:20]}")
        
        # Tipo
        tipo = 'Certificato'
        h3 = soup.find('div', class_='panel-heading')
        if h3: tipo = h3.find('h3').get_text(strip=True).upper()
        
        # Scadenza
        for row in soup.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2 and 'Valutazione finale' in cells[0].get_text():
                cert['maturity_date'] = cells[1].get_text(strip=True)
                break
        
        # Barriera
        barriera_div = soup.find('div', id='barriera')
        if barriera_div:
            for cell in barriera_div.find_all('td'):
                m = re.search(r'(\d+(?:[.,]\d+)?)\s*%', cell.get_text())
                if m:
                    cert['barrier'] = float(m.group(1).replace(',', '.'))
                    cert['barrier_down'] = True
                    break
        
        # Cedola
        rel_div = soup.find('div', id='rilevamento')
        if rel_div:
            for cell in rel_div.find_all('td'):
                m = re.search(r'^(\d+(?:[.,]\d+)?)\s*%', cell.get_text())
                if m:
                    coupon = float(m.group(1).replace(',', '.'))
                    if 'mensile' in cell.get_text().lower():
                        coupon *= 12
                        cert['coupon_frequency'] = 'monthly'
                    cert['annual_coupon_yield'] = coupon
                    break
        
        cert.update({
            'type': tipo,
            'underlying_name': underlyings[0],
            'underlying_category': 'index',
            'underlyings': underlyings[:5]
        })
        
        return True
        
    except Exception as e:
        print(f"💥 {e}")
        return False

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()  # ✅ FIX: await qui
        page = await context.new_page()
        
        all_certs = await scrape_listing(page)
        valid = []
        
        for cert in all_certs[:MAX_DETAIL_ISIN]:
            if await scrape_detail(page, cert):
                valid.append(cert)
            await asyncio.sleep(1.5)
        
        await browser.close()
        
        output = {
            'success': True, 'count': len(valid),
            'certificates': valid,
            'metadata': {'version': 'v20-fixed', 'checked': len(all_certs)}
        }
        
        with open('certificates-data.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"🎉 {len(valid)} certificati validi salvati!")

if __name__ == '__main__':
    asyncio.run(main())
