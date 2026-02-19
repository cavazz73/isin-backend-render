#!/usr/bin/env python3
"""
Scraper v19 - NON filtra nel listing, filtra DOPO aver letto i sottostanti VERI
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

def is_equity_stock(name: str) -> bool:
    """TRUE se è un'azione singola (NON un indice)"""
    if not name:
        return False
    n = name.lower()
    
    # Blacklist azioni italiane comuni
    stocks = ['enel', 'eni', 'intesa', 'unicredit', 'generali', 'telecom',
              'mps', 'bpm', 'banco bpm', 'stm', 'leonardo', 'saipem', 'tenaris',
              'azimut', 'bper', 'ferrari', 'campari', 'atlantia', 'poste',
              'amplifon', 'recordati', 'diasorin', 'nexi', 'prysmian',
              'terna', 'snam', 'hera', 'a2a', 'fincantieri', 'iveco']
    
    return any(stock in n for stock in stocks)

async def scrape_listing(page) -> List[Dict]:
    """Prende TUTTI i certificati recenti (senza filtrare)"""
    print("📋 LISTING - Tutti certificati recenti")
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
            
            cert = {
                'isin': isin,
                'name': cols[1].strip(),
                'issuer': cols[2].strip(),
                'type': 'Certificato',
                'underlying': cols[3].strip(),  # Es: "Basket di azioni worst of"
                'underlying_name': None,
                'underlying_category': None,
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
                'source': 'CED_v19'
            }
            certificati.append(cert)
        
        except (ValueError, IndexError):
            continue
    
    print(f"✅ {len(certificati)} certificati totali")
    return certificati[:MAX_DETAIL_ISIN * 2]

async def scrape_detail(page, cert: Dict) -> bool:
    """
    Scrape dettaglio + FILTRA se contiene azioni
    Returns: True se è valido (indici), False se contiene azioni (da scartare)
    """
    isin = cert['isin']
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    print(f"🔍 {isin}", end=" ")
    
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        
        try:
            await page.wait_for_selector('#barriera', timeout=8000)
        except:
            pass
        
        await page.wait_for_timeout(3000)
        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 1. SOTTOSTANTI - Parse tabella "Scheda Sottostante"
        underlyings = []
        for panel in soup.find_all('div', class_='panel'):
            panel_title = panel.find('div', class_='panel-heading')
            if panel_title and 'Scheda Sottostante' in panel_title.get_text():
                table = panel.find('table')
                if table:
                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        for row in rows:
                            cols = [c.get_text(strip=True) for c in row.find_all('td')]
                            if len(cols) > 0:
                                name = cols[0] if len(cols) > 0 else ''
                                underlyings.append(name)
                    break
        
        # 2. CHECK: Se contiene azioni → SCARTA
        has_stocks = any(is_equity_stock(u) for u in underlyings)
        
        if has_stocks:
            print(f"❌ AZIONI: {', '.join(underlyings[:3])}")
            return False
        
        if not underlyings:
            print(f"⚠️  No sottostanti")
            return False
        
        # ✅ OK: È un indice/commodity/basket indici
        print(f"✅ {underlyings[0][:30]}")
        
        # 3. TIPO
        tipo = 'Certificato'
        panel_heading = soup.find('div', class_='panel-heading')
        if panel_heading:
            h3 = panel_heading.find('h3')
            if h3:
                tipo = h3.get_text(strip=True).upper()
        
        # 4. DATA SCADENZA
        maturity_date = None
        for row in soup.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                if 'Data Valutazione finale' in label:
                    maturity_date = cells[1].get_text(strip=True)
                    if maturity_date != '01/01/1900':
                        break
        
        # 5. BARRIERA
        barrier = None
        barrier_down = None
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
        
        # 6. CEDOLA
        coupon = None
        rilevamento_div = soup.find('div', id='rilevamento')
        if rilevamento_div:
            cells = rilevamento_div.find_all('td')
            for cell in cells:
                text = cell.get_text(strip=True)
                match = re.search(r'^(\d+(?:[.,]\d+)?)\s*%$', text)
                if match:
                    coupon = float(match.group(1).replace(',', '.'))
                    break
        
        # Update certificato
        cert.update({
            'type': tipo,
            'maturity_date': maturity_date,
            'barrier': barrier,
            'barrier_down': barrier_down,
            'annual_coupon_yield': coupon,
            'underlying_name': underlyings[0] if underlyings else None,
            'underlying_category': 'index',
            'underlyings': []  # NON mostrare nel frontend
        })
        
        return True
    
    except Exception as e:
        print(f"❌ Error: {str(e)[:30]}")
        return False

async def main():
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
            context = await browser.new_context()
            page = await context.new_page()
            
            all_certs = await scrape_listing(page)
            
            # Scrape details + FILTRA azioni
            valid_certs = []
            for cert in all_certs[:MAX_DETAIL_ISIN]:
                is_valid = await scrape_detail(page, cert)
                if is_valid:
                    valid_certs.append(cert)
                await asyncio.sleep(1.5)
            
            await browser.close()
            
            # Salva
            pd.DataFrame(valid_certs).to_json('certificates-recenti.json', orient='records', indent=2)
            pd.DataFrame(valid_certs).to_csv('certificates-recenti.csv', index=False)
            
            payload = {
                'success': True,
                'count': len(valid_certs),
                'certificates': valid_certs,
                'metadata': {'version': 'v19-filter-after-parse', 'total_checked': len(all_certs[:MAX_DETAIL_ISIN])}
            }
            
            with open('certificates-data.json', 'w') as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            
            print(f"\n✅ {len(valid_certs)} validi (su {len(all_certs[:MAX_DETAIL_ISIN])} controllati) | v19")
    
    except Exception as e:
        print(f"⚠️ {e}")
    finally:
        sys.exit(0)

if __name__ == '__main__':
    asyncio.run(main())
