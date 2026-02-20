#!/usr/bin/env python3
"""
CED Scraper v21 - Fix cedola trimestrale, barriera 60%, sottostanti completi, scenario
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
import re
from typing import List, Dict, Optional

RECENT_DAYS = int(os.getenv('RECENT_DAYS', '30'))
MAX_DETAIL_ISIN = int(os.getenv('MAX_DETAIL_ISIN', '50'))

cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)
DATE_FORMAT = '%d/%m/%Y'

def is_valid_underlying(name: str) -> bool:
    """✅ Indici/Commodities/Valute/Tassi/Credit"""
    if not name: return False
    n = name.lower()
    valid = [
        'nikkei', 'sp500', 's&p', 'dow', 'dax', 'cac', 'eurostoxx', 'ftse', 'russell', 'nasdaq',
        'oro', 'gold', 'silver', 'petrolio', 'oil', 'brent', 'wti', 'gas',
        'eur', 'usd', 'gbp', 'chf', 'jpy', 'fx',
        'euribor', 'eonia', 'libor', 'swap', 'yield',
        'credit', 'cln', 'cds', 'ishares', 'etf'
    ]
    return any(kw in n for kw in valid)

def has_stocks(underlyings: List[str]) -> bool:
    """❌ Azioni singole"""
    stocks = ['enel', 'eni', 'intesa', 'unicredit', 'intel', 'asml', 'tesla', 'apple', 'nvidia', 
              'microsoft', 'amazon', 'meta', 'google', 'alibaba', 'bayer', 'volkswagen']
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
                'market': 'SeDeX', 'price': None, 'strike': None,
                'barrier': None, 'barrier_down': None,
                'annual_coupon_yield': None, 'coupon_frequency': 'annual',
                'trigger_autocallable': None, 'underlyings': [],
                'scenario_analysis': None, 'source': 'CED_v21'
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
        
        # ====== 1. SOTTOSTANTI COMPLETI ======
        underlyings = []
        for panel in soup.find_all('div', class_='panel'):
            title_div = panel.find('div', class_='panel-heading')
            if title_div and 'sottostante' in title_div.get_text().lower():
                table = panel.find('table')
                if table and table.find('tbody'):
                    for row in table.find('tbody').find_all('tr'):
                        cols = [c.get_text(strip=True) for c in row.find_all('td')]
                        if cols and cols[0]:
                            underlyings.append(cols[0])
                break
        
        if not underlyings:
            print("⚠️ No sottostanti")
            return False
        
        # Filtri
        if has_stocks(underlyings):
            print(f"❌ Stocks: {underlyings[0][:20]}")
            return False
        if not any(is_valid_underlying(u) for u in underlyings):
            print(f"❌ Invalid: {underlyings[0][:20]}")
            return False
        
        print(f"✅ {len(underlyings)} sottost: {underlyings[0][:15]}")
        
        # ====== 2. TIPO ======
        tipo = 'Certificato'
        h3_type = soup.find('h3', class_='panel-title')
        if h3_type:
            tipo = h3_type.get_text(strip=True).upper()
        
        # ====== 3. SCADENZA ======
        maturity_date = None
        for row in soup.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                if any(kw in label for kw in ['Valutazione finale', 'Data Scadenza', 'DATA SCADENZA']):
                    mat = cells[1].get_text(strip=True)
                    if mat and mat != '01/01/1900':
                        maturity_date = mat
                        break
        
        # ====== 4. BARRIERA (FIX: 60% non 1%) ======
        barrier, barrier_down = None, None
        
        # Cerca nella tabella barriera
        barriera_div = soup.find('div', id='barriera')
        if barriera_div:
            # Cerca tutte le celle
            for cell in barriera_div.find_all('td'):
                text = cell.get_text(strip=True)
                # Match: "60" o "60%" o "60 %" ma NON "1" o "0"
                match = re.search(r'(\d{2,3})(?:\s*%)?', text)
                if match:
                    val = float(match.group(1))
                    if 5 <= val <= 100:  # Range realistico
                        barrier = val
                        barrier_down = True
                        break
        
        # Fallback: cerca nel pannello "Barriera Down"
        if barrier is None:
            for panel in soup.find_all('div', class_='panel'):
                heading = panel.find('h3')
                if heading and 'barriera' in heading.get_text().lower():
                    table = panel.find('table')
                    if table:
                        for cell in table.find_all('td'):
                            match = re.search(r'(\d{2,3})(?:\s*%)?', cell.get_text())
                            if match:
                                val = float(match.group(1))
                                if 5 <= val <= 100:
                                    barrier = val
                                    barrier_down = True
                                    break
        
        # ====== 5. CEDOLA (FIX: trimestrale → annualizza) ======
        coupon, coupon_freq = None, 'annual'
        
        rilevamento_div = soup.find('div', id='rilevamento')
        if rilevamento_div:
            table = rilevamento_div.find('table')
            if table:
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    note_text = ''
                    cedola_val = None
                    
                    for cell in cells:
                        text = cell.get_text(strip=True)
                        # Cerca cedola (es: "2,25" o "2.25%")
                        match = re.search(r'(\d+(?:[.,]\d+)?)\s*%?', text)
                        if match and not cedola_val:
                            cedola_val = float(match.group(1).replace(',', '.'))
                        # Accumula note per capire frequenza
                        note_text += text.lower() + ' '
                    
                    if cedola_val and cedola_val > 0:
                        # Identifica frequenza
                        if any(kw in note_text for kw in ['trimestral', 'quarterly', 'ogni 3 mesi']):
                            coupon = cedola_val * 4  # ✅ Annualizza
                            coupon_freq = 'quarterly'
                        elif any(kw in note_text for kw in ['mensile', 'monthly', 'ogni mese']):
                            coupon = cedola_val * 12
                            coupon_freq = 'monthly'
                        elif any(kw in note_text for kw in ['semestral', 'semiannual', '6 mesi']):
                            coupon = cedola_val * 2
                            coupon_freq = 'semiannual'
                        else:
                            coupon = cedola_val  # Già annuale
                            coupon_freq = 'annual'
                        break
        
        # ====== 6. ANALISI SCENARIO ======
        scenario = None
        
        # Cerca tabella con scenari (Worst/Medium/Best case)
        for table in soup.find_all('table'):
            header_text = ''.join([th.get_text().lower() for th in table.find_all('th')])
            if any(kw in header_text for kw in ['scenario', 'worst', 'best', 'probabile']):
                scenarios = {}
                for row in table.find_all('tr'):
                    cells = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
                    if len(cells) >= 2:
                        label = cells[0].lower()
                        if 'worst' in label or 'peggiore' in label:
                            scenarios['worst_case'] = cells[1]
                        elif 'best' in label or 'migliore' in label:
                            scenarios['best_case'] = cells[1]
                        elif 'medium' in label or 'probabile' in label:
                            scenarios['medium_case'] = cells[1]
                
                if scenarios:
                    scenario = scenarios
                    break
        
        # ====== SALVA TUTTO ======
        cert.update({
            'type': tipo,
            'maturity_date': maturity_date,
            'barrier': barrier,
            'barrier_down': barrier_down,
            'annual_coupon_yield': coupon,
            'coupon_frequency': coupon_freq,
            'underlying_name': underlyings[0] if underlyings else None,
            'underlying_category': 'index',
            'underlyings': underlyings,
            'scenario_analysis': scenario
        })
        
        return True
        
    except Exception as e:
        print(f"💥 {str(e)[:25]}")
        return False

async def main():
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context()
        page = await context.new_page()
        
        all_certs = await scrape_listing(page)
        valid = []
        
        for cert in all_certs[:MAX_DETAIL_ISIN]:
            if await scrape_detail(page, cert):
                valid.append(cert)
            await asyncio.sleep(1.5)
        
        await browser.close()
        
        # SALVA
        output = {
            'success': True,
            'count': len(valid),
            'certificates': valid,
            'metadata': {
                'version': 'v21-complete-fix',
                'criteria': 'INDICI/COMMODITIES/VALUTE/TASSI/CREDIT',
                'fixes': 'Cedola trimestrale→annua, Barriera 60%, Sottostanti completi, Scenario',
                'checked': len(all_certs),
                'timestamp': datetime.now().isoformat()
            }
        }
        
        with open('certificates-data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        pd.DataFrame(valid).to_csv('certificates-validi.csv', index=False)
        print(f"\n🎉 {len(valid)} certificati CORRETTI salvati!")
        
        # Test IT0006773789
        test_cert = next((c for c in valid if c['isin'] == 'IT0006773789'), None)
        if test_cert:
            print(f"\n✅ TEST IT0006773789:")
            print(f"  Cedola annua: {test_cert.get('annual_coupon_yield')}% ({test_cert.get('coupon_frequency')})")
            print(f"  Barriera: {test_cert.get('barrier')}%")
            print(f"  Sottostanti: {len(test_cert.get('underlyings', []))} - {test_cert.get('underlyings', [])[:2]}")
            print(f"  Scenario: {'✅ Presente' if test_cert.get('scenario_analysis') else '❌ Mancante'}")

if __name__ == '__main__':
    asyncio.run(main())
