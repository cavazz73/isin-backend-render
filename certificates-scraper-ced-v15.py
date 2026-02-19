#!/usr/bin/env python3
"""
Scraper CED v18 FINALE
FIX 1: FILTRA solo indici/commodities (NO azioni)
FIX 2: Barriera da tabella HTML popolata (dopo AJAX wait)
FIX 3: Cedola da tabella rilevamento
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

def is_index_commodity_fx(sottostante: str) -> bool:
    """TRUE se indice/commodity/FX - FALSE se azione singola"""
    if not sottostante:
        return False
    s = sottostante.lower()
    
    # ✅ WHITELIST: Solo questi passano
    allowed = [
        # Indici
        'indice', 'index', 'ftse', 'dax', 's&p', 'sp', 'euro stoxx', 'stoxx',
        'nasdaq', 'nikkei', 'hang seng', 'dow jones', 'russell', 'msci',
        # Commodities
        'gold', 'silver', 'oil', 'brent', 'wti', 'copper', 'gas', 'wheat',
        # FX
        'eur/', 'usd/', 'gbp/', 'chf/', 'jpy/', 'fx', 'valuta', 'cambio',
        # Tassi
        'euribor', 'tasso', 'rate', 'eonia', 'sonia', 'irs',
        # Credit
        'credit', 'cln', 'spread'
    ]
    
    if any(k in s for k in allowed):
        return True
    
    # ❌ BLACKLIST: Se contiene nomi aziende → SCARTA
    blacklist = ['enel', 'eni', 'intesa', 'unicredit', 'generali', 'telecom',
                 'mps', 'bpm', 'stmicroelectronics', 'leonardo', 'saipem',
                 'apple', 'microsoft', 'amazon', 'meta', 'alphabet', 'tesla']
    
    if any(az in s for az in blacklist):
        return False
    
    return False

async def scrape_listing(page) -> List[Dict]:
    """Step 1: FILTRA solo indici/commodities/FX"""
    print("📋 LISTING - SOLO indici/commodities/FX")
    await page.goto('https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp', wait_until='networkidle')
    await page.wait_for_timeout(5000)
    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.select('table tr')
    
    certificati = []
    for row in rows:
        cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
        if len(cols) < 7:
            continue
        
        isin = cols[0].strip()
        if not re.match(r'^[A-Z0-9]{12}$', isin):
            continue
        
        try:
            data_str = cols[5].strip()
            data_em = datetime.strptime(data_str, DATE_FORMAT)
            if data_em < cutoff_date:
                continue
            
            sottostante = cols[3].strip()
            
            # ✅ FILTRO: Solo indici/commodities/FX
            if not is_index_commodity_fx(sottostante):
                print(f"  ⏭️  SKIP {isin}: {sottostante[:40]}")
                continue
            
            cert = {
                'isin': isin,
                'name': cols[1].strip(),
                'issuer': cols[2].strip(),
                'type': 'Certificato',
                'underlying': sottostante,
                'underlying_name': sottostante,
                'underlying_category': 'index',  # Semplificato
                'issue_date': data_str,
                'maturity_date': None,
                'market': 'SeDeX',
                'price': None,
                'strike': None,
                'barrier': None,
                'barrier_down': None,
                'annual_coupon_yield': None,
                'coupon_frequency': 'annual',
                'trigger_autocallable': None,
                'underlyings': [],
                'scenario_analysis': None,
                'source': 'CED_v18'
            }
            certificati.append(cert)
            
            if len(certificati) % 20 == 0:
                print(f"  ✅ {len(certificati)} validi")
        
        except (ValueError, IndexError):
            continue
    
    print(f"✅ {len(certificati)} certificati (solo indici/commodities)")
    return certificati[:MAX_DETAIL_ISIN * 2]

async def scrape_detail(page, isin: str) -> Dict:
    """Step 2: Parse BARRIERA da tabella HTML popolata"""
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    print(f"🔍 {isin}")
    
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        
        # WAIT AJAX: Aspetta DIV barriera + rilevamento
        try:
            await page.wait_for_selector('#barriera', timeout=10000)
        except:
            pass
        
        await page.wait_for_timeout(4000)  # Attesa rendering
        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 1. TIPO
        tipo = 'Certificato'
        panel_heading = soup.find('div', class_='panel-heading')
        if panel_heading:
            h3 = panel_heading.find('h3')
            if h3:
                tipo = h3.get_text(strip=True).upper()
        
        # 2. DATA SCADENZA
        maturity_date = None
        for row in soup.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                if 'Data Valutazione finale' in label:
                    maturity_date = cells[1].get_text(strip=True)
                    if maturity_date != '01/01/1900':
                        break
        
        # 3. BARRIERA - Parse da TABELLA HTML (non da JavaScript params)
        barrier = None
        barrier_down = None
        barriera_div = soup.find('div', id='barriera')
        if barriera_div:
            # Cerca TUTTE le celle td dentro il DIV
            cells = barriera_div.find_all('td')
            for cell in cells:
                text = cell.get_text(strip=True)
                # Match "60 %" o "50%"
                match = re.search(r'(\d+(?:[.,]\d+)?)\s*%', text)
                if match:
                    barrier = float(match.group(1).replace(',', '.'))
                    barrier_down = True
                    print(f"  📊 Barriera: {barrier}%")
                    break
        
        # 4. CEDOLA - da tabella rilevamento
        coupon = None
        rilevamento_div = soup.find('div', id='rilevamento')
        if rilevamento_div:
            cells = rilevamento_div.find_all('td')
            for cell in cells:
                text = cell.get_text(strip=True)
                # Match numero seguito da %
                match = re.search(r'^(\d+(?:[.,]\d+)?)\s*%$', text)
                if match:
                    coupon = float(match.group(1).replace(',', '.'))
                    print(f"  💰 Cedola: {coupon}%")
                    break
        
        return {
            'type': tipo,
            'maturity_date': maturity_date,
            'barrier': barrier,
            'barrier_down': barrier_down,
            'annual_coupon_yield': coupon,
            'source': 'CED_v18_final'
        }
    
    except Exception as e:
        print(f"❌ {isin}: {str(e)[:50]}")
        return {}

async def main():
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
            context = await browser.new_context()
            page = await context.new_page()
            
            certificati = await scrape_listing(page)
            
            filled = 0
            for cert in certificati[:MAX_DETAIL_ISIN]:
                detail = await scrape_detail(page, cert['isin'])
                cert.update(detail)
                if detail.get('barrier'):
                    filled += 1
                await asyncio.sleep(1.5)
            
            await browser.close()
            
            # Salva
            pd.DataFrame(certificati).to_json('certificates-recenti.json', orient='records', indent=2)
            pd.DataFrame(certificati).to_csv('certificates-recenti.csv', index=False)
            
            payload = {
                'success': True,
                'count': len(certificati),
                'certificates': certificati,
                'metadata': {'version': 'v18-index-only', 'filled': filled}
            }
            
            with open('certificates-data.json', 'w') as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            
            print(f"✅ {len(certificati)} tot | {filled} con barriera | v18-FINAL")
    
    except Exception as e:
        print(f"⚠️ {e}")
    finally:
        sys.exit(0)

if __name__ == '__main__':
    asyncio.run(main())
