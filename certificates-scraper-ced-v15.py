#!/usr/bin/env python3
"""
Scraper v15-10 - FILTRA SOLO: Indici/Commodities/Valute/Tassi/Credit Link
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
    """✅ TRUE solo per: indici/commodities/valute/tassi/credit link"""
    if not name:
        return False
    
    n = name.lower()
    
    # ✅ INDICI
    indices = ['nikkei', 'sp500', 's&p', 'dow', 'dax', 'cac', 'eurostoxx', 
               'ftse', 'russell', 'nasdaq', 'mdax', 'sdax']
    
    # ✅ COMMODITIES
    commodities = ['oro', 'gold', 'silver', 'argento', 'petrolio', 'oil', 
                   'brent', 'wti', 'gas', 'grano', 'wheat', 'mais', 'corn', 
                   'soia', 'soy', 'rame', 'copper']
    
    # ✅ VALUTE
    currencies = ['eur', 'usd', 'gbp', 'chf', 'jpy', 'fx', 'eurusd']
    
    # ✅ TASSI
    rates = ['euribor', 'eonia', 'libor', 'swap', 'yield']
    
    # ✅ CREDIT LINK
    credit = ['credit', 'cln', 'cds']
    
    valid_keywords = indices + commodities + currencies + rates + credit
    
    return any(keyword in n for keyword in valid_keywords)

def has_single_stock(underlyings: List[str]) -> bool:
    """❌ TRUE se contiene azioni singole"""
    stock_keywords = ['enel', 'eni', 'intesa', 'unicredit', 'generali', 
                     'intel', 'asml', 'amd', 'tesla', 'nvidia', 'apple']
    return any(any(stock in u.lower() for stock in stock_keywords) for u in underlyings)

async def scrape_listing(page) -> List[Dict]:
    print("📋 Scraping TUTTI certificati recenti...")
    await page.goto('https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp', wait_until='networkidle')
    await page.wait_for_timeout(5000)
    
    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.select('table tr')
    
    certificati = []
    for row in rows:
        cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
        if len(cols) < 7: continue
        
        isin = cols[0].strip()
        if not re.match(r'^[A-Z0-9]{12}$', isin): continue
        
        try:
            data_str = cols[5].strip()
            data_em = datetime.strptime(data_str, DATE_FORMAT)
            if data_em < cutoff_date: continue
            
            certificati.append({
                'isin': isin, 'name': cols[1].strip(), 'issuer': cols[2].strip(),
                'type': 'Certificato', 'underlying': cols[3].strip(),
                'underlying_name': None, 'underlying_category': None,
                'issue_date': data_str, 'maturity_date': None,
                'market': 'SeDeX', 'price': None, 'strike': None,
                'barrier': None, 'barrier_down': None,
                'annual_coupon_yield': None, 'coupon_frequency': 'annual',
                'trigger_autocallable': None, 'underlyings': [],
                'scenario_analysis': None, 'source': 'CED_v20'
            })
        except (ValueError, IndexError):
            continue
    
    print(f"✅ {len(certificati)} totali recenti")
    return certificati[:MAX_DETAIL_ISIN * 2]

async def scrape_detail(page, cert: Dict) -> bool:
    isin = cert['isin']
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    print(f"🔍 {isin}", end=" ")
    
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(3000)
        
        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 1. SOTTOSTANTI COMPLETI
        underlyings = []
        sottostanti_panel = None
        for panel in soup.find_all('div', class_='panel'):
            title = panel.find('div', class_='panel-heading')
            if title and any(kw in title.get_text().lower() for kw in ['sottostante', 'sottostanti']):
                sottostanti_panel = panel
                break
        
        if sottostanti_panel:
            table = sottostanti_panel.find('table')
            if table:
                tbody = table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    for row in rows:
                        cols = [c.get_text(strip=True) for c in row.find_all('td')]
                        if cols:
                            underlyings.append(cols[0])
        
        # 2. FILTRI RIGIDI
        if not underlyings:
            print("⚠️ No sottostanti")
            return False
        
        # ❌ SCARTA se ha azioni
        if has_single_stock(underlyings):
            print(f"❌ AZIONI: {', '.join(underlyings[:2])}")
            return False
        
        # ✅ ACCETTA solo se ha sottostante valido
        if not any(is_valid_underlying(u) for u in underlyings):
            print(f"❌ Invalidi: {', '.join(underlyings[:2])}")
            return False
        
        print(f"✅ {underlyings[0][:25]}...")
        
        # 3. TIPO
        tipo_panel = soup.find('div', class_='panel-heading')
        tipo = 'Certificato'
        if tipo_panel:
            h3 = tipo_panel.find('h3')
            if h3: tipo = h3.get_text(strip=True).upper()
        
        # 4. SCADENZA
        maturity_date = None
        for row in soup.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2 and 'Data Valutazione finale' in cells[0].get_text():
                maturity_date = cells[1].get_text(strip=True)
                if maturity_date == '01/01/1900': maturity_date = None
                break
        
        # 5. BARRIERA
        barrier, barrier_down = None, None
        barriera_div = soup.find('div', id='barriera')
        if barriera_div:
            cells = barriera_div.find_all('td')
            for cell in cells:
                text = cell.get_text(strip=True)
                match = re.search(r'(\d+(?:[.,]\d+)?)\s*%', text)
                if match:
                    barrier = float(match.group(1).replace(',', '.'))
                    barrier_down = True
                    break
        
        # 6. CEDOLA (annualizza se mensile)
        coupon = None
        coupon_freq = 'annual'
        rilevamento_div = soup.find('div', id='rilevamento')
        if rilevamento_div:
            cells = rilevamento_div.find_all('td')
            for cell in cells:
                text = cell.get_text(strip=True)
                match = re.search(r'^(\d+(?:[.,]\d+)?)\s*%', text)
                if match:
                    coupon = float(match.group(1).replace(',', '.'))
                    # Se trova "mensile" nel testo → annualizza
                    if 'mensile' in text.lower():
                        coupon *= 12
                        coupon_freq = 'monthly'
                    break
        
        # SALVA
        cert.update({
            'type': tipo,
            'maturity_date': maturity_date,
            'barrier': barrier,
            'barrier_down': barrier_down,
            'annual_coupon_yield': coupon,
            'coupon_frequency': coupon_freq,
            'underlying_name': underlyings[0] if underlyings else None,
            'underlying_category': 'index' if any(is_valid_underlying(u) for u in underlyings) else 'other',
            'underlyings': underlyings[:5]  # Primo 5 per debug
        })
        
        return True
        
    except Exception as e:
        print(f"💥 {str(e)[:30]}")
        return False

async def main():
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = await browser.new_context().new_page()
        
        all_certs = await scrape_listing(page)
        valid_certs = []
        
        for cert in all_certs[:MAX_DETAIL_ISIN]:
            if await scrape_detail(page, cert):
                valid_certs.append(cert)
            await asyncio.sleep(1.5)
        
        await browser.close()
        
        # SALVA
        output = {
            'success': True,
            'count': len(valid_certs),
            'certificates': valid_certs,
            'metadata': {
                'version': 'v20-strict-filter',
                'criteria': 'INDICI/COMMODITIES/VALUTE/TASSI/CREDIT ONLY',
                'total_checked': len(all_certs),
                'timestamp': datetime.now().isoformat()
            }
        }
        
        with open('certificates-data.json', 'w') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        pd.DataFrame(valid_certs).to_csv('certificates-validi.csv', index=False)
        print(f"\n🎉 {len(valid_certs)} VALID CERTIFICATI salvati in certificates-data.json")

if __name__ == '__main__':
    asyncio.run(main())
