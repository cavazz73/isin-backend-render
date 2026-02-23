#!/usr/bin/env python3
"""
CED Scraper v22 - ROBUSTO
- Retry automatico
- Timeout lunghi
- User-agent realistico
- Delay tra richieste
"""

import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# ============ CONFIG ============
RECENT_DAYS = int(os.getenv('RECENT_DAYS', '60'))
MAX_CERTIFICATES = int(os.getenv('MAX_DETAIL_ISIN', '100'))
REQUEST_DELAY = 2.0  # secondi tra richieste
PAGE_TIMEOUT = 60000  # 60 secondi
RETRY_COUNT = 3

cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)

# ============ FILTRI ============
VALID_KEYWORDS = [
    # Indici
    'ftse', 'mib', 'stoxx', 'eurostoxx', 'dax', 'cac', 'ibex', 
    's&p', 'sp500', 'nasdaq', 'dow', 'nikkei', 'hang seng', 'russell', 'msci',
    # Commodities
    'oro', 'gold', 'silver', 'argento', 'petrolio', 'oil', 'brent', 'wti', 
    'gas', 'copper', 'rame', 'platinum', 'palladium',
    # Valute
    'eur/usd', 'usd/jpy', 'forex', 'currency', 'valuta', 'cambio',
    # Tassi
    'euribor', 'libor', 'bund', 'btp', 'treasury', 'swap', 'yield', 'tasso',
    # Credit
    'credit', 'cln', 'cds',
    # Altri
    'index', 'indice', 'basket', 'paniere'
]

STOCK_KEYWORDS = [
    'enel', 'eni', 'intesa', 'unicredit', 'generali', 'ferrari', 'stellantis',
    'stm', 'telecom', 'tim', 'leonardo', 'pirelli', 'moncler', 'campari',
    'tesla', 'apple', 'amazon', 'nvidia', 'microsoft', 'alphabet', 'google',
    'meta', 'netflix', 'amd', 'intel', 'adobe', 'oracle', 'salesforce',
    'lvmh', 'asml', 'sap', 'siemens', 'allianz', 'basf', 'bayer'
]


def is_valid_underlying(name: str) -> bool:
    """Verifica se sottostante è valido (indice/commodity/valuta/tasso)"""
    if not name:
        return False
    n = name.lower()
    return any(kw in n for kw in VALID_KEYWORDS)


def has_only_stocks(underlyings: List[str]) -> bool:
    """Verifica se contiene SOLO azioni singole"""
    if not underlyings:
        return False
    
    all_text = ' '.join(underlyings).lower()
    
    # Se ha keyword validi, non è solo stocks
    if any(kw in all_text for kw in VALID_KEYWORDS):
        return False
    
    # Se ha azioni singole e nessun keyword valido
    return any(stock in all_text for stock in STOCK_KEYWORDS)


def parse_number(text: str) -> Optional[float]:
    """Parse numero italiano"""
    if not text or text.strip().upper() in ['N.A.', 'N.D.', '-', '', 'N/A']:
        return None
    try:
        cleaned = re.sub(r'[EUR€%\s\xa0]', '', text.strip())
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        val = float(cleaned)
        return round(val, 4) if val else None
    except:
        return None


def parse_date(text: str) -> Optional[str]:
    """Parse data italiana DD/MM/YYYY -> YYYY-MM-DD"""
    if not text or text.strip() in ['', 'N.A.', '01/01/1900']:
        return None
    try:
        if '/' in text:
            parts = text.strip().split('/')
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
    except:
        pass
    return text


async def retry_goto(page, url: str, retries: int = RETRY_COUNT) -> bool:
    """Naviga con retry"""
    for attempt in range(retries):
        try:
            await page.goto(url, wait_until='networkidle', timeout=PAGE_TIMEOUT)
            await page.wait_for_timeout(2000)
            return True
        except Exception as e:
            print(f"  Attempt {attempt+1}/{retries} failed: {str(e)[:30]}")
            if attempt < retries - 1:
                await asyncio.sleep(3)
    return False


async def scrape_listing(page) -> List[Dict]:
    """Scrape lista certificati recenti da CED"""
    print("📋 Fetching listing from CED...")
    
    url = 'https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp'
    
    if not await retry_goto(page, url):
        print("❌ Failed to load listing page")
        return []
    
    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')
    
    certificates = []
    
    # Cerca tabella con ISIN
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all(['td', 'th'])
            if len(cols) < 6:
                continue
            
            col_texts = [col.get_text(strip=True) for col in cols]
            isin = col_texts[0]
            
            # Valida ISIN
            if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                continue
            
            # Parse data emissione
            try:
                date_str = col_texts[5] if len(col_texts) > 5 else ''
                if '/' in date_str:
                    emission_date = datetime.strptime(date_str, '%d/%m/%Y')
                    if emission_date < cutoff_date:
                        continue
            except:
                continue
            
            certificates.append({
                'isin': isin,
                'name': col_texts[1] if len(col_texts) > 1 else '',
                'issuer': col_texts[2] if len(col_texts) > 2 else '',
                'underlying_raw': col_texts[3] if len(col_texts) > 3 else '',
                'issue_date': col_texts[5] if len(col_texts) > 5 else None,
            })
    
    print(f"✅ Found {len(certificates)} recent certificates")
    return certificates[:MAX_CERTIFICATES * 2]


async def scrape_detail(page, cert: Dict) -> Optional[Dict]:
    """Scrape dettagli singolo certificato"""
    isin = cert['isin']
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    if not await retry_goto(page, url):
        return None

    # Aspetta che div#rilevamento sia caricato via AJAX (non sia più "Caricamento")
    try:
        await page.wait_for_function(
            "!document.getElementById('rilevamento').innerText.includes('Caricamento')",
            timeout=15000
        )
    except:
        pass  # Continua comunque

    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')

    result = {
        'isin': isin,
        'name': cert.get('name', ''),
        'type': 'Certificato',
        'issuer': cert.get('issuer', ''),
        'market': 'SeDeX',
        'currency': 'EUR',
        'underlying': '',
        'underlyings': [],
        'issue_date': parse_date(cert.get('issue_date')),
        'maturity_date': None,
        'barrier': None,
        'barrier_down': None,
        'barrier_type': None,
        'coupon': None,
        'annual_coupon_yield': None,
        'coupon_frequency': 'monthly',
        'reference_price': 1000.0,
        'scenario_analysis': None,
        'source': 'CED_v22'
    }

    # === 1. TIPO CERTIFICATO ===
    # Nella pagina reale è il primo <h3> senza classe (non h3.panel-title)
    h3s = soup.find_all('h3')
    if h3s:
        result['type'] = h3s[0].get_text(strip=True)

    # === 2. BARRIERA dallo script JS ===
    # La barriera è passata come parametro JS prima del fetch AJAX:
    # var params = new URLSearchParams({ barriera: "50&nbsp;%", tipo: "DISCRETA", ... })
    for script in soup.find_all('script'):
        t = script.get_text()
        m = re.search(r'barriera:\s*["\']([^"\']+)["\']', t)
        if m:
            val_raw = m.group(1).replace('&nbsp;', '').replace('%', '').strip()
            try:
                val = float(val_raw.replace(',', '.'))
                if 10 <= val <= 95:
                    result['barrier'] = val
                    result['barrier_down'] = val
            except:
                pass
            # Tipo barriera (DISCRETA / CONTINUA / EUROPEA)
            bt = re.search(r'tipo:\s*["\']([^"\']+)["\']', t)
            if bt:
                result['barrier_type'] = bt.group(1)
            break

    # === 3. SCADENZA ed EMISSIONE dalla TABLE principale (TABLE 1) ===
    # Struttura reale: TABLE 1 ha label/valore su ogni riga
    # Labels: ISIN, ALPHA CODE, FASE, MERCATO, DATA EMISSIONE, DATA SCADENZA
    tables = soup.find_all('table')
    if len(tables) >= 2:
        for row in tables[1].find_all('tr'):
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).upper()
                value = cells[1].get_text(strip=True)
                if 'DATA SCADENZA' in label or 'VALUTAZIONE FINALE' in label:
                    d = parse_date(value)
                    if d:
                        result['maturity_date'] = d
                elif 'DATA EMISSIONE' in label:
                    d = parse_date(value)
                    if d:
                        result['issue_date'] = d
                elif label == 'MERCATO':
                    result['market'] = value

    # === 4. EMITTENTE dalla TABLE emittente (TABLE 3) ===
    if len(tables) >= 4:
        first_row = tables[3].find('tr')
        if first_row:
            cells = first_row.find_all(['th', 'td'])
            if cells:
                issuer = cells[0].get_text(strip=True)
                if issuer and len(issuer) < 60:
                    result['issuer'] = issuer

    # === 5. SOTTOSTANTI dalla TABLE sottostanti (TABLE 2) ===
    # Colonne reali: DESCRIZIONE | STRIKE | PESO (no spot, no barrier in pagina)
    underlyings_raw = []
    underlyings_objects = []
    if len(tables) >= 3:
        for row in tables[2].find_all('tr')[1:]:  # salta header
            cells = row.find_all('td')
            if not cells:
                continue
            name = cells[0].get_text(strip=True)
            if not name or name.upper() in ['DESCRIZIONE', 'SOTTOSTANTE', '']:
                continue
            strike_raw = cells[1].get_text(strip=True) if len(cells) > 1 else ''
            strike = parse_number(strike_raw)
            # Calcola barriera come prezzo assoluto: strike * barrier_down%
            barrier_price = round(strike * result['barrier_down'] / 100, 4) if (strike and result['barrier_down']) else 0.0
            underlyings_raw.append(name)
            underlyings_objects.append({
                'name': name,
                'strike': strike or 0.0,
                'spot': 0.0,          # non disponibile in pagina
                'barrier': barrier_price,
                'variation_pct': 0.0,
                'variation_abs': 0.0,
                'trigger_coupon': strike or 0.0,
                'trigger_autocall': strike or 0.0,
                'worst_of': False
            })

    # Fallback da listing
    if not underlyings_objects and cert.get('underlying_raw'):
        underlyings_raw = [cert['underlying_raw']]
        underlyings_objects = [{
            'name': cert['underlying_raw'],
            'strike': 0.0, 'spot': 0.0, 'barrier': 0.0,
            'variation_pct': 0.0, 'variation_abs': 0.0,
            'trigger_coupon': 0.0, 'trigger_autocall': 0.0,
            'worst_of': True
        }]

    # Marca primo come worst_of di default
    if underlyings_objects:
        underlyings_objects[0]['worst_of'] = True

    result['underlyings'] = underlyings_objects
    result['underlying'] = underlyings_raw[0] if underlyings_raw else ''

    # === 6. FILTRO: Solo indici/commodities/valute/tassi ===
    if has_only_stocks(underlyings_raw):
        return None
    if not any(is_valid_underlying(u) for u in underlyings_raw):
        full_text = f"{result['name']} {result['underlying']}".lower()
        if not any(kw in full_text for kw in VALID_KEYWORDS):
            return None

    # === 7. CEDOLA da div#rilevamento (caricato via AJAX) ===
    rilevamento_div = soup.find('div', id='rilevamento')
    if rilevamento_div:
        table = rilevamento_div.find('table')
        if table:
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                row_text = ' '.join([c.get_text(strip=True).lower() for c in cells])
                for cell in cells:
                    text = cell.get_text(strip=True)
                    # % obbligatorio per evitare falsi positivi
                    match = re.search(r'(\d+(?:[.,]\d+)?)\s*%', text)
                    if match:
                        cedola = float(match.group(1).replace(',', '.'))
                        if 0.05 <= cedola <= 30:
                            if 'trimestral' in row_text or 'quarterly' in row_text:
                                result['coupon'] = cedola
                                result['annual_coupon_yield'] = round(cedola * 4, 2)
                                result['coupon_frequency'] = 'quarterly'
                            elif 'mensil' in row_text or 'monthly' in row_text:
                                result['coupon'] = cedola
                                result['annual_coupon_yield'] = round(cedola * 12, 2)
                                result['coupon_frequency'] = 'monthly'
                            elif 'semestral' in row_text or 'semiannual' in row_text:
                                result['coupon'] = cedola
                                result['annual_coupon_yield'] = round(cedola * 2, 2)
                                result['coupon_frequency'] = 'semiannual'
                            else:
                                result['coupon'] = cedola
                                result['annual_coupon_yield'] = cedola
                                result['coupon_frequency'] = 'annual'
                            break
                if result['annual_coupon_yield']:
                    break

    # === 8. SCENARIO ANALYSIS ===
    if result['annual_coupon_yield'] and result['barrier_down']:
        ref_price = result['reference_price']
        annual_yield = result['annual_coupon_yield']
        barrier_pct = result['barrier_down']
        years = 3.0
        if result.get('maturity_date'):
            try:
                mat = datetime.strptime(result['maturity_date'], '%Y-%m-%d')
                years = max(0.1, round((mat - datetime.now()).days / 365.25, 2))
            except:
                pass

        total_coupons = (annual_yield / 100) * 1000 * years
        cert_type_low = result['type'].lower()
        scenarios = []

        for var_pct in [-70, -60, -50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70]:
            barrier_breached = var_pct < -(100 - barrier_pct)
            if barrier_breached:
                redemption = round(max(0, 1000 * (1 + var_pct / 100)), 2)
            elif 'twin win' in cert_type_low:
                redemption = round(1000 * (1 + abs(var_pct) / 100), 2)
            else:
                redemption = 1000.0

            total_received = redemption + total_coupons
            pl_pct = round((total_received - ref_price) / ref_price * 100, 2)
            pl_annual = round(pl_pct / years, 2) if years > 0 else 0.0

            scenarios.append({
                'variation_pct': var_pct,
                'underlying_price': round(1000 * (1 + var_pct / 100), 2),
                'barrier_breached': barrier_breached,
                'redemption': redemption,
                'pl_pct': pl_pct,
                'pl_annual': pl_annual
            })

        result['scenario_analysis'] = {
            'worst_underlying': result['underlying'],
            'purchase_price': ref_price,
            'years_to_maturity': years,
            'scenarios': scenarios
        }

    return result


async def main():
    print("=" * 60)
    print("CED Scraper v22 - ROBUSTO")
    print(f"Filtri: Indici, Commodities, Valute, Tassi, Credit")
    print(f"Ultimi {RECENT_DAYS} giorni, max {MAX_CERTIFICATES} certificati")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='it-IT',
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        
        # 1. Ottieni lista
        listing = await scrape_listing(page)
        
        if not listing:
            print("❌ No certificates found in listing!")
            await browser.close()
            
            output = {
                'success': False,
                'count': 0,
                'certificates': [],
                'metadata': {
                    'error': 'Failed to fetch listing',
                    'timestamp': datetime.now().isoformat()
                }
            }
            with open('certificates-data.json', 'w') as f:
                json.dump(output, f, indent=2)
            
            # File vuoti per evitare errori commit
            pd.DataFrame().to_json('certificates-recenti.json', orient='records')
            pd.DataFrame().to_csv('certificates-recenti.csv', index=False)
            return
        
        # 2. Scrape dettagli
        valid_certs = []
        skipped = 0
        errors = 0
        
        print(f"\n📊 Processing {min(len(listing), MAX_CERTIFICATES)} certificates...\n")
        
        for i, cert in enumerate(listing[:MAX_CERTIFICATES], 1):
            print(f"[{i}/{min(len(listing), MAX_CERTIFICATES)}] {cert['isin']}...", end=" ", flush=True)
            
            try:
                result = await scrape_detail(page, cert)
                
                if result:
                    valid_certs.append(result)
                    barrier = result.get('barrier')
                    coupon = result.get('annual_coupon_yield')
                    print(f"✅ Barrier:{barrier}% Coupon:{coupon}% - {result['underlying'][:25]}")
                else:
                    skipped += 1
                    print("⏭️ Skipped (stocks/invalid)")
                    
            except Exception as e:
                errors += 1
                print(f"❌ Error: {str(e)[:30]}")
            
            await asyncio.sleep(REQUEST_DELAY)
        
        await browser.close()
        
        # 3. Salva output
        output = {
            'success': True,
            'count': len(valid_certs),
            'certificates': valid_certs,
            'metadata': {
                'version': 'v22-robust',
                'source': 'certificatiederivati.it',
                'criteria': 'Indici, Commodities, Valute, Tassi, Credit',
                'recent_days': RECENT_DAYS,
                'processed': min(len(listing), MAX_CERTIFICATES),
                'valid': len(valid_certs),
                'skipped_stocks': skipped,
                'errors': errors,
                'timestamp': datetime.now().isoformat()
            }
        }
        
        with open('certificates-data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        # Salva anche CSV e JSON separati (per il workflow)
        df = pd.DataFrame(valid_certs)
        df.to_json('certificates-recenti.json', orient='records', indent=2)
        df.to_csv('certificates-recenti.csv', index=False)
        
        print("\n" + "=" * 60)
        print("COMPLETED")
        print(f"  ✅ Valid certificates: {len(valid_certs)}")
        print(f"  ⏭️ Skipped (stocks): {skipped}")
        print(f"  ❌ Errors: {errors}")
        print(f"  💾 Saved: certificates-data.json, certificates-recenti.json, certificates-recenti.csv")
        print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
