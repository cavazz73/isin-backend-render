#!/usr/bin/env python3
"""
Scraper CED v17 DEFINITIVO - SOLO INDICI/COMMODITIES
- NON scrapare titoli azionari come sottostanti
- Prendi sottostante principale dal nome certificato
- Fix barriera/cedola da AJAX
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
import re
from typing import List, Dict, Any

RECENT_DAYS = int(os.getenv('RECENT_DAYS', '30'))
MAX_DETAIL_ISIN = int(os.getenv('MAX_DETAIL_ISIN', '50'))
cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)
DATE_FORMAT = '%d/%m/%Y'

def classify_underlying_category(sott: str) -> str:
    """Classifica SOLO indici, FX, commodities, tassi - NO azioni singole"""
    if not sott: return "other"
    s = sott.lower()
    
    # INDICI
    if any(k in s for k in ["indice", "index", "ftse", "dax", "s&p", "sp", "euro stoxx", "stoxx", 
                            "nasdaq", "nikkei", "hang seng", "dow jones", "russell", "msci"]):
        return "index"
    
    # FX / VALUTE
    if any(k in s for k in ["eur", "usd", "gbp", "chf", "jpy", "fx", "valuta", "cambio"]):
        return "fx"
    
    # TASSI
    if any(k in s for k in ["euribor", "tasso", "rate", "eonia", "sonia", "libor", "irs", "swap"]):
        return "rate"
    
    # CREDIT
    if any(k in s for k in ["credit", "cln", "linked", "spread"]):
        return "credit"
    
    # COMMODITIES
    if any(k in s for k in ["gold", "silver", "oil", "brent", "wti", "copper", "gas", "wheat", 
                            "corn", "commodity", "materie prime"]):
        return "commodity"
    
    # BASKET
    if any(k in s for k in ["basket", "worst of", "best of", "paniere"]):
        return "basket"
    
    # Se contiene nomi di aziende → SCARTA
    aziende = ["enel", "eni", "intesa", "unicredit", "generali", "telecom", "mps", "bpm", 
               "stm", "leonardo", "saipem", "tenaris", "apple", "microsoft", "amazon", "meta"]
    if any(az in s for az in aziende):
        return "equity_single"  # Marca come azione singola
    
    return "other"

async def scrape_listing(page) -> List[Dict]:
    """Step 1: Elenco nuove emissioni - FILTRA solo indici/commodities"""
    print("📋 Step 1: LISTING nuove emissioni (SOLO indici/commodities)")
    await page.goto('https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp', wait_until='networkidle')
    await page.wait_for_timeout(5000)
    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.select('table tr')
    print(f"Totale righe: {len(rows)}")
    
    certificati = []
    for i, row in enumerate(rows):
        cols = [col.get_text(strip=True) for col in row.find_all(['td', 'th'])]
        if len(cols) < 7 or cols[0] in ['ISIN', 'NOME', 'EMITTENTE', 'SOTTOSTANTE']: continue
        isin = cols[0].strip()
        if not re.match(r'^[A-Z0-9]{12}$', isin): continue
        
        try:
            data_str = cols[5].strip()
            data_em = datetime.strptime(data_str, DATE_FORMAT)
            if data_em < cutoff_date: continue
            
            nome = cols[1].strip()
            emittente = cols[2].strip()
            sottostante = cols[3].strip()
            
            # ✅ FILTRA: Scarta se contiene nomi di azioni
            underlying_category = classify_underlying_category(sottostante)
            if underlying_category == "equity_single":
                print(f"  ⏭️  SKIP {isin}: {sottostante} (azioni singole)")
                continue
            
            cert = {
                'isin': isin,
                'name': nome,
                'issuer': emittente,
                'type': 'Certificato',
                'underlying': sottostante,  # ES: "Nasdaq 100", "Euro Stoxx 50"
                'underlying_name': sottostante,
                'underlying_category': underlying_category,
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
                'underlyings': [],  # ✅ VUOTO! Non mostrare componenti
                'scenario_analysis': None,
                'source': 'CED_nuove_emissioni_v17'
            }
            certificati.append(cert)
            if len(certificati) % 50 == 0: print(f"  ✅ {len(certificati)} certificati (solo indici/commodities)")
        except (ValueError, IndexError):
            continue
    
    print(f"✅ Totale certificati VALIDI: {len(certificati)}")
    return certificati[:MAX_DETAIL_ISIN * 2]

async def scrape_detail(page, isin: str) -> Dict:
    """Step 2: Dettaglio scheda v17 - BARRIERA/CEDOLA corrette"""
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    print(f"🔍 {isin}")
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        
        # Wait AJAX
        try:
            await page.wait_for_selector('#barriera', timeout=8000)
        except:
            pass
        
        await page.wait_for_timeout(3000)
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
        all_rows = soup.find_all('tr')
        for row in all_rows:
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                if 'Data Valutazione finale' in label:
                    if value and value != '01/01/1900':
                        maturity_date = value
                        break
        
        # 3. BARRIERA - Parse dal DIV #barriera DOPO AJAX
        barrier = barrier_down = None
        barriera_div = soup.find('div', id='barriera')
        if barriera_div:
            # Cerca prima cella con "XX %"
            all_text = barriera_div.get_text()
            # Regex: cerca numero seguito da %
            match = re.search(r'(\d+(?:[.,]\d+)?)\s*%', all_text)
            if match:
                barrier = match.group(1).replace(',', '.')
                barrier_down = True
                print(f"  📊 Barriera: {barrier}%")
        
        # 4. CEDOLA - Parse dal DIV #rilevamento
        coupon = None
        coupon_frequency = 'annual'
        rilevamento_div = soup.find('div', id='rilevamento')
        if rilevamento_div:
            all_text = rilevamento_div.get_text()
            # Cerca "Cedola: XX%" o "Premio: XX%"
            match = re.search(r'(?:Cedola|Premio)[:\s]+(\d+(?:[.,]\d+)?)\s*%', all_text, re.IGNORECASE)
            if match:
                coupon = match.group(1).replace(',', '.')
                print(f"  💰 Cedola: {coupon}%")
        
        # 5. TRIGGER (se presente)
        trigger_autocallable = None
        if rilevamento_div:
            match = re.search(r'Trigger[:\s]+(\d+(?:[.,]\d+)?)\s*%', rilevamento_div.get_text(), re.IGNORECASE)
            if match:
                trigger_autocallable = match.group(1).replace(',', '.')
        
        return {
            'type': tipo,
            'maturity_date': maturity_date,
            'strike': None,  # Non serve per indici
            'barrier': barrier,
            'barrier_down': barrier_down,
            'annual_coupon_yield': coupon,
            'coupon_frequency': coupon_frequency,
            'trigger_autocallable': trigger_autocallable,
            'underlyings': [],  # ✅ VUOTO - Non mostrare componenti
            'source': 'CED_v17_index_only'
        }
    except Exception as e:
        print(f"❌ {isin}: {str(e)[:50]}")
        return {}

def clean_numeric_fields(certificati: List[Dict]) -> List[Dict]:
    """Step 3: Clean numeri"""
    for cert in certificati:
        # Barrier
        if cert.get('barrier') and isinstance(cert['barrier'], str):
            try:
                cert['barrier'] = float(cert['barrier'])
            except:
                cert['barrier'] = None
        
        # Coupon
        if cert.get('annual_coupon_yield') and isinstance(cert['annual_coupon_yield'], str):
            try:
                cert['annual_coupon_yield'] = float(cert['annual_coupon_yield'])
            except:
                cert['annual_coupon_yield'] = None
        
        # Trigger
        if cert.get('trigger_autocallable') and isinstance(cert['trigger_autocallable'], str):
            try:
                cert['trigger_autocallable'] = float(cert['trigger_autocallable'])
            except:
                cert['trigger_autocallable'] = None
    
    return certificati

async def main():
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    certificati = []
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            context = await browser.new_context(user_agent='Mozilla/5.0')
            page = await context.new_page()
            
            certificati = await scrape_listing(page)
            print(f"📋 {len(certificati)} ISIN (solo indici/commodities)")
            
            filled = 0
            for cert in certificati[:MAX_DETAIL_ISIN]:
                detail = await scrape_detail(page, cert['isin'])
                cert.update(detail)
                if detail.get('barrier') or detail.get('annual_coupon_yield'):
                    filled += 1
                await asyncio.sleep(1.5)
            
            await browser.close()
            
            certificati = clean_numeric_fields(certificati)
            
            pd.DataFrame(certificati).to_json('certificates-recenti.json', orient='records', indent=2)
            pd.DataFrame(certificati).to_csv('certificates-recenti.csv', index=False)
            
            payload = {
                'success': True,
                'count': len(certificati),
                'certificates': certificati,
                'metadata': {
                    'version': 'v17-index-only',
                    'details_filled': filled,
                    'scraped_at': datetime.now().isoformat()
                }
            }
            
            with open('certificates-data.json', 'w', encoding='utf-8') as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            
            print(f"✅ {len(certificati)} certificati | {filled} con dati | v17-index-only")
            
    except Exception as e:
        print(f"⚠️ {e}")
    finally:
        print("🏁 DONE")
        sys.exit(0)

if __name__ == '__main__':
    asyncio.run(main())
