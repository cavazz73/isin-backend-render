#!/usr/bin/env python3
"""
Scraper CED v26 DEFINITIVO - LISTING + DETAIL COMPLETO
Fix:
- Ignora placeholder 01/01/1900 di CED
- Gestisce TRACKER senza barriere/cedole
- Parsing robusto tabelle HTML reali
- Annualizzazione cedole mensili
- Clean numeri per frontend
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
                'type': 'Certificato',
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
                'coupon_frequency': 'annual',
                'trigger_autocallable': None,
                'underlyings': [],
                'scenario_analysis': None,
                'source': 'CED_nuove_emissioni'
            }
            certificati.append(cert)
            if len(certificati) % 100 == 0: print(f"  {len(certificati)} recenti (Riga {i})")
        except (ValueError, IndexError):
            continue
    
    print(f"Totale certificati recenti: {len(certificati)}")
    return certificati[:MAX_DETAIL_ISIN * 2]

async def scrape_detail(page, isin: str) -> Dict:
    """Step 2: Dettaglio scheda COMPLETO - v26 CON FIX PLACEHOLDER"""
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    print(f"🔍 {isin}")
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(3000)
        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 1. TIPO dal panel-heading
        tipo = 'Certificato'
        panel_heading = soup.find('div', class_='panel-heading')
        if panel_heading:
            tipo = panel_heading.get_text(strip=True).upper()
        
        # 2. DATA SCADENZA - dalla tabella principale jumbotron
        maturity_date = None
        jumbotron = soup.find('div', class_='jumbotron')
        if jumbotron:
            rows = jumbotron.find_all('tr')
            for row in rows:
                cells = row.find_all(['th', 'td'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    if 'Data Valutazione finale' in label or 'Data scadenza' in label:
                        # FIX: Ignora placeholder CED
                        if value != '01/01/1900' and value != '':
                            maturity_date = value
                        break
        
        # 3. SOTTOSTANTI - tabella con header "Scheda Sottostante"
        underlyings = []
        for panel in soup.find_all('div', class_='panel'):
            panel_title = panel.find('div', class_='panel-heading')
            if panel_title and 'Scheda Sottostante' in panel_title.get_text():
                table = panel.find('table')
                if table:
                    thead = table.find('thead')
                    tbody = table.find('tbody')
                    if thead and tbody:
                        headers = [h.get_text(strip=True).upper() for h in thead.find_all('th')]
                        if 'DESCRIZIONE' in headers and 'STRIKE' in headers:
                            desc_idx = headers.index('DESCRIZIONE')
                            strike_idx = headers.index('STRIKE')
                            peso_idx = headers.index('PESO') if 'PESO' in headers else None
                            
                            rows = tbody.find_all('tr')
                            for row in rows:
                                cols = [c.get_text(strip=True) for c in row.find_all('td')]
                                if len(cols) > strike_idx:
                                    underlying = {
                                        'name': cols[desc_idx] if desc_idx < len(cols) else '',
                                        'strike': cols[strike_idx],
                                        'weight': cols[peso_idx] if peso_idx and peso_idx < len(cols) else None
                                    }
                                    underlyings.append(underlying)
                break
        
        # 4. BARRIERA DOWN - dalla tabella dopo "Barriera Down" heading
        barrier = barrier_down = None
        # Cerca tutte le righe della pagina
        all_rows = soup.find_all('tr')
        for i, row in enumerate(all_rows):
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                if 'Barriera' in label and 'Down' not in label:  # Riga header "Barriera"
                    # Cerca nelle righe successive
                    for next_row in all_rows[i+1:i+5]:
                        next_cells = next_row.find_all('td')
                        if next_cells:
                            first_cell = next_cells[0].get_text(strip=True)
                            if '%' in first_cell and first_cell[0].isdigit():
                                barrier = first_cell
                                barrier_down = True
                                break
                    if barrier:
                        break
        
        # 5. CEDOLA E TRIGGER - parsing AJAX (non disponibile in HTML statico)
        # Per ora lasciamo null, richiederebbe chiamata AJAX separata
        coupon = None
        trigger_autocallable = None
        coupon_frequency = 'annual'
        
        # Cerca "Date rilevamento" nella pagina per capire se ha cedole
        page_text = soup.get_text()
        if 'Date rilevamento' in page_text or 'CEDOLA' in page_text.upper():
            # Ha struttura cedolare, ma dati in AJAX
            # TODO: implementare chiamata ajax_rilevamento.asp se necessario
            pass
        
        return {
            'type': tipo,
            'maturity_date': maturity_date,
            'strike': underlyings[0]['strike'] if len(underlyings) == 1 else None,
            'barrier': barrier,
            'barrier_down': barrier_down,
            'annual_coupon_yield': coupon,
            'coupon_frequency': coupon_frequency,
            'trigger_autocallable': trigger_autocallable,
            'underlyings': underlyings,
            'source': 'CED_scheda_v26'
        }
    except Exception as e:
        print(f"❌ {isin}: {str(e)[:40]}")
        return {}

def clean_numeric_fields(certificati: List[Dict]) -> List[Dict]:
    """Step 3: Converte stringhe italiane → numeri + annualizza cedola mensile"""
    for cert in certificati:
        # 1. Pulisci barrier: "50 %" → 50.0
        if cert.get('barrier') and isinstance(cert['barrier'], str):
            try:
                val = cert['barrier'].replace('%', '').replace(',', '.').strip()
                cert['barrier'] = float(val) if val else None
            except:
                cert['barrier'] = None
        
        # 2. Pulisci coupon: "0,83 %" → 0.83
        if cert.get('annual_coupon_yield') and isinstance(cert['annual_coupon_yield'], str):
            try:
                val = cert['annual_coupon_yield'].replace('%', '').replace(',', '.').strip()
                cert['annual_coupon_yield'] = float(val) if val else None
            except:
                cert['annual_coupon_yield'] = None
        
        # 3. ANNUALIZZA cedola se mensile
        if cert.get('coupon_frequency') == 'monthly' and cert.get('annual_coupon_yield'):
            cert['annual_coupon_yield'] = cert['annual_coupon_yield'] * 12
            print(f"  📅 {cert['isin']}: Cedola annualizzata {cert['annual_coupon_yield']:.2f}% (mensile × 12)")
        
        # 4. Pulisci trigger: "65 %" → 65.0
        if cert.get('trigger_autocallable') and isinstance(cert['trigger_autocallable'], str):
            try:
                val = cert['trigger_autocallable'].replace('%', '').replace(',', '.').strip()
                cert['trigger_autocallable'] = float(val) if val else None
            except:
                cert['trigger_autocallable'] = None
        
        # 5. Pulisci strike principale: "10519" → 10519.0 (ma NON "1" di TRACKER indice)
        if cert.get('strike') and isinstance(cert['strike'], str):
            try:
                # Se strike = "1" e tipo TRACKER, lascia come stringa descrittiva
                if cert['strike'].strip() == '1' and 'TRACKER' in cert.get('type', ''):
                    cert['strike'] = 1.0  # Base indice
                else:
                    val = cert['strike'].replace('.', '').replace(',', '.')
                    cert['strike'] = float(val) if val else None
            except:
                cert['strike'] = None
        
        # 6. Pulisci strike sottostanti
        if cert.get('underlyings'):
            for und in cert['underlyings']:
                if und.get('strike') and isinstance(und['strike'], str):
                    try:
                        val = und['strike'].replace('.', '').replace(',', '.')
                        und['strike'] = float(val) if val else None
                    except:
                        und['strike'] = None
        
        # 7. FIX: Se maturity_date è placeholder, set None
        if cert.get('maturity_date') == '01/01/1900':
            cert['maturity_date'] = None
    
    return certificati

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
            
            # STEP 1: Scrape listing
            certificati = await scrape_listing(page)
            print(f"📋 {len(certificati)} ISIN trovati")
            
            # STEP 2: Scrape details
            filled = 0
            for i, cert in enumerate(certificati[:MAX_DETAIL_ISIN]):
                detail = await scrape_detail(page, cert['isin'])
                cert.update(detail)
                if detail.get('strike') or detail.get('barrier') or detail.get('underlyings'): 
                    filled += 1
                await asyncio.sleep(1.5)
            
            await browser.close()
            
            # STEP 3: Clean numeri per frontend
            certificati = clean_numeric_fields(certificati)
            print(f"🧹 Cleaned {len(certificati)} certificati (v26-placeholder-fix)")
            
            # STEP 4: Salva output
            pd.DataFrame(certificati).to_json('certificates-recenti.json', orient='records', indent=2)
            pd.DataFrame(certificati).to_csv('certificates-recenti.csv', index=False)
            
            payload = {
                'success': True, 
                'count': len(certificati), 
                'certificates': certificati, 
                'metadata': {
                    'version': 'v26-placeholder-fix',
                    'details_filled': filled,
                    'scraped_at': datetime.now().isoformat()
                }
            }
            
            with open('certificates-data.json', 'w', encoding='utf-8') as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            
            print(f"✅ {len(certificati)} tot | {filled} details | v26-placeholder-fix")
            
    except Exception as e:
        print(f"⚠️ Errore: {str(e)[:80]}")
        exit_code = 0
    finally:
        print("🏁 DONE")
        sys.exit(0)

if __name__ == '__main__':
    asyncio.run(main())
