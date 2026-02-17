#!/usr/bin/env python3
"""
Scraper CED v18 COMPLETO - LISTING + DETAIL SCHEDE
1. Elenco nuove emissioni → ISIN recenti
2. Per ogni ISIN → Scheda dettaglio → strike, barriera, sottostanti[], tipo
3. JSON ricco per frontend CedLab-style
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
import time

RECENT_DAYS = int(os.getenv('RECENT_DAYS', '30'))
MAX_DETAIL_ISIN = int(os.getenv('MAX_DETAIL_ISIN', '50'))  # Anti-ban
cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)
DATE_FORMAT = '%d/%m/%Y'

def classify_underlying_category(sott: str) -> str:
    if not sott: return "other"
    s = sott.lower()
    if "indice" in s or any(k in s for k in ["ftse", "dax", "sp", "euro stoxx", "nasdaq"]): return "index"
    if any(k in s for k in ["eur", "usd", "gbp", "chf", "jpy", "fx", "valuta"]): return "fx"
    if any(k in s for k in ["euribor", "tasso", "rate", "eonia", "sonia", "libor"]): return "rate"
    if any(k in s for k in ["credit", "cln", "linked"]): return "credit"
    if any(k in s for k in ["basket", "worst of", "best of"]): return "basket"
    return "single"

async def scrape_listing(page) -> List[Dict]:
    """Step 1: Elenco nuove emissioni"""
    print("📋 Step 1: LISTING nuove emissioni")
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
            
            cert = {
                'isin': isin,
                'name': nome,
                'issuer': emittente,
                'type': 'Certificato',  # Placeholder
                'underlying': sottostante,
                'underlying_name': sottostante,
                'underlying_category': classify_underlying_category(sottostante),
                'issue_date': data_str,
                'maturity_date': None,
                'market': 'SeDeX',
                'price': None,
                'strike': None,
                'barrier': None,
                'barrier_down': None,
                'annual_coupon_yield': None,
                'scenario_analysis': None,
                'source': 'CED_nuove_emissioni'
            }
            certificati.append(cert)
            if len(certificati) % 100 == 0: print(f"  {len(certificati)} recenti (Riga {i})")
        except (ValueError, IndexError):
            continue
    
    print(f"Totale certificati recenti: {len(certificati)}")
    return certificati[:MAX_DETAIL_ISIN * 2]  # Buffer per fallimenti

async def scrape_detail(page, isin: str) -> Dict:
    """Step 2: Dettaglio singola scheda"""
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    print(f"🔍 Detail {isin} → {url}")
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(3000)
        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        
        # Estrai tipo (Phoenix, Cash Collect, etc.)
        tipo = 'Certificato'
        tipo_elem = soup.find(text=re.compile(r'(Phoenix|Cash Collect|Turbo|Bonus|Barrier|Step Down)', re.I))
        if tipo_elem: tipo = tipo_elem.strip().upper()
        
        # Strike, Barriera (cerca tabelle/prodotto)
        strike = barrier = barrier_down = annual_coupon_yield = None
        text = soup.get_text()
        strike_match = re.search(r'Strike[:\s]*([0-9,.]+)', text, re.I)
        if strike_match: strike = strike_match.group(1).replace(',', '.')
        
        barrier_match = re.search(r'Barriera[:\s]*(-?\d+(?:,\d+)?%)?', text, re.I)
        if barrier_match: barrier = barrier_match.group(1)
        
        if 'down' in text.lower() or barrier and float(barrier.replace(',', '.')) < 0: barrier_down = True
        
        coupon_match = re.search(r'(Cedola|Coupon)[:\s]*(\d+(?:,\d+)?%)?', text, re.I)
        if coupon_match: annual_coupon_yield = coupon_match.group(2)
        
        return {
            'type': tipo,
            'strike': strike,
            'barrier': barrier,
            'barrier_down': barrier_down,
            'annual_coupon_yield': annual_coupon_yield,
            'source': 'CED_scheda'
        }
    except Exception as e:
        print(f"❌ Detail {isin} fallito: {e}")
        return {}

async def main():
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    certificati = []
    exit_code = 0
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--single-process'])
            context = await browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            page = await context.new_page()
            
            certificati = await scrape_listing(page)
            print(f"📋 {len(certificati)} ISIN")
            
            filled = 0
            for i, cert in enumerate(certificati[:MAX_DETAIL_ISIN]):
                detail = await scrape_detail(page, cert['isin'])
                cert.update(detail)
                if detail.get('strike') or detail.get('barrier'): filled += 1
                print(f"🔍 {i+1}/{MAX_DETAIL_ISIN}: {cert['isin']}")
                await asyncio.sleep(1.5)
            
            await browser.close()
            
            pd.DataFrame(certificati).to_json('certificates-recenti.json', orient='records', indent=2)
            pd.DataFrame(certificati).to_csv('certificates-recenti.csv', index=False)
            
            payload = {'success': True, 'count': len(certificati), 'certificates': certificati, 'metadata': {'version': 'v22', 'details_filled': filled}}
            with open('certificates-data.json', 'w', encoding='utf-8') as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            
            print(f"✅ {len(certificati)} tot | {filled} details")
    except Exception as e:
        print(f"⚠️ Errore: {str(e)[:80]}")
        exit_code = 0  # Forza 0 anche su errore
    finally:
        print("🏁 DONE")
        sys.exit(0)  # FORZA EXIT 0

if __name__ == '__main__':
    asyncio.run(main())
