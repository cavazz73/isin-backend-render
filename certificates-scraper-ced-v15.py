#!/usr/bin/env python3
"""
CED Scraper v16 - FIXED
Scrapes certificates from CED "Tabella Prodotti Banca Generali"

FIXES in v16 vs v15:
- Type detection from certificate name (not copying full name as type)
- Detail page scrapes ALL columns: name, strike, spot, barrier, distance
- Re-enrich ISINs that have incomplete data (underlyings with spot=0)
- Preserve old detail data when merging
- Scenario analysis includes worst_underlying name and purchase_price
- Proper spot prices from detail page
"""

import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# ============ CONFIG ============
RECENT_DAYS = int(os.getenv('RECENT_DAYS', '60'))
MAX_CERTIFICATES = int(os.getenv('MAX_DETAIL_ISIN', '100'))
REQUEST_DELAY = 2.0
PAGE_TIMEOUT = 60000
RETRY_COUNT = 3

cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)

SOURCE_URL = 'https://www.certificatiederivati.it/bs_promo_bgenerali.asp?t=redazione'
SOURCE_NAME = 'CED Banca Generali'

# ============ FILTRI ============
VALID_KEYWORDS = [
    'ftse', 'mib', 'stoxx', 'eurostoxx', 'euro stoxx', 'dax', 'cac', 'ibex',
    's&p', 'sp500', 'sp 500', 'nasdaq', 'dow jones', 'nikkei', 'hang seng',
    'russell', 'msci', 'smi', 'topix', 'kospi', 'sensex',
    'stoxx europe 600', 'eurostoxx bank', 'eurostoxx technolog',
    'eurostoxx insurance', 'eurostoxx utilit', 'eurostoxx oil',
    'eurostoxx basic', 'eurostoxx healthcare', 'eurostoxx health',
    'stoxx europe', 'select dividend',
    'ishares msci', 'ishares china', 'spdr', 'etf',
    'oro', 'gold', 'silver', 'argento', 'petrolio', 'oil', 'brent', 'wti',
    'gas naturale', 'natural gas', 'copper', 'rame', 'platinum', 'platino',
    'palladium', 'palladio', 'crude', 'commodity',
    'eur/usd', 'usd/jpy', 'gbp/usd', 'forex', 'currency', 'valuta', 'cambio',
    'euribor', 'libor', 'bund', 'btp', 'treasury', 'swap', 'yield', 'tasso',
    'estron',
    'credit linked', 'credit link', 'cln', 'cds',
    'index', 'indice', 'basket di indici', 'paniere',
]

STOCK_KEYWORDS = [
    'enel', 'eni', 'intesa sanpaolo', 'unicredit', 'generali', 'ferrari',
    'stellantis', 'stmicroelectronics', 'telecom italia', 'tim', 'leonardo',
    'pirelli', 'moncler', 'campari', 'a2a', 'snam', 'terna', 'poste italiane',
    'mediobanca', 'banco bpm', 'bper banca', 'banca mps', 'saipem',
    'amplifon', 'brunello cucinelli', 'diasorin', 'recordati', 'italgas',
    'nexi', 'prysmian', 'tenaris', 'hera', 'buzzi',
    'tesla', 'apple', 'amazon', 'nvidia', 'microsoft', 'alphabet', 'google',
    'meta', 'netflix', 'amd', 'intel', 'adobe', 'oracle', 'salesforce',
    'paypal', 'qualcomm', 'uber', 'airbnb',
    'lvmh', 'asml', 'sap', 'siemens', 'allianz', 'basf', 'bayer',
    'adidas', 'bmw', 'mercedes', 'volkswagen',
    'axa', 'bnp paribas', 'societe generale', 'credit agricole', 'total',
    'engie', 'danone', 'sanofi', 'kering', 'hermes',
    'shell', 'unilever', 'vodafone', 'barclays', 'hsbc',
    'british petroleum', 'rio tinto', 'anglo american', 'glencore',
    'nestle', 'novartis', 'roche', 'zurich', 'ubs',
    'bbva', 'banco santander', 'iberdrola', 'telefonica',
    'novo nordisk', 'maersk', 'aegon', 'renault',
    'arcelor mittal', 'nike', 'kraft heinz', 'coca cola', 'commerzbank',
    'deutsche bank', 'morgan stanley', 'citigroup', 'goldman sachs',
    'worldline', 'diageo',
]

# ============ CERTIFICATE TYPE DETECTION ============
CERT_TYPE_PATTERNS = [
    ('phoenix memory step down', 'Phoenix Memory Step Down'),
    ('phoenix memory', 'Phoenix Memory'),
    ('phoenix', 'Phoenix'),
    ('cash collect memory', 'Cash Collect Memory'),
    ('cash collect', 'Cash Collect'),
    ('bonus cap', 'Bonus Cap'),
    ('bonus', 'Bonus'),
    ('express', 'Express'),
    ('twin win', 'Twin Win'),
    ('airbag', 'Airbag'),
    ('autocallable', 'Autocallable'),
    ('reverse convertible', 'Reverse Convertible'),
    ('reverse', 'Reverse'),
    ('digital', 'Digital'),
    ('credit linked', 'Credit Linked'),
    ('outperformance', 'Outperformance'),
    ('tracker', 'Tracker'),
    ('capital protected', 'Capital Protected'),
    ('protection', 'Protection'),
]


def detect_certificate_type(name: str) -> str:
    if not name:
        return 'Certificato'
    n = name.lower().strip()
    for pattern, cert_type in CERT_TYPE_PATTERNS:
        if pattern in n:
            return cert_type
    return 'Certificato'


def is_valid_underlying(name: str) -> bool:
    if not name:
        return False
    return any(kw in name.lower().strip() for kw in VALID_KEYWORDS)


def parse_number(text: str) -> Optional[float]:
    if not text or text.strip().upper() in ['N.A.', 'N.D.', '-', '', 'N/A']:
        return None
    try:
        cleaned = re.sub(r'[EUR\u20ac%\s\xa0]', '', text.strip())
        cleaned = cleaned.rstrip('*')
        if not cleaned or cleaned == '0':
            return None
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        val = float(cleaned)
        return round(val, 6) if val else None
    except (ValueError, TypeError):
        return None


def parse_date(text: str) -> Optional[str]:
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


def parse_worst_of(text: str) -> Tuple[str, Optional[float]]:
    if not text:
        return '', None
    match = re.match(r'^(.+?)\s*\(([0-9.,]+)\)\s*$', text.strip(), re.DOTALL)
    if match:
        return match.group(1).strip(), parse_number(match.group(2))
    lines = text.strip().split('\n')
    if len(lines) >= 2:
        name = lines[0].strip()
        strike_match = re.search(r'\(([0-9.,]+)\)', lines[-1])
        if strike_match:
            return name, parse_number(strike_match.group(1))
    return text.strip(), None


def get_freq_multiplier(freq: str) -> Tuple[float, str]:
    f = freq.lower().strip() if freq else ''
    if 'mensil' in f or 'monthly' in f:
        return 12.0, 'monthly'
    elif 'trimestral' in f or 'quarterly' in f:
        return 4.0, 'quarterly'
    elif 'semestral' in f or 'semiannual' in f:
        return 2.0, 'semiannual'
    elif 'annual' in f or 'annuale' in f:
        return 1.0, 'annual'
    return 12.0, 'monthly'


def barrier_as_pct(barrier_abs: Optional[float], strike: Optional[float]) -> Optional[float]:
    if barrier_abs and strike and strike > 0:
        pct = (barrier_abs / strike) * 100
        if 5 <= pct <= 100:
            return round(pct, 2)
    return None


def generate_scenarios(barrier_pct, ask_price, annual_yield, years_to_mat, wo_strike, wo_name=''):
    if not all([barrier_pct, ask_price, years_to_mat, wo_strike]):
        return None
    if ask_price <= 0 or years_to_mat <= 0:
        return None
    nominal = 1000.0
    scenarios = []
    for var in [-70, -60, -50, -40, -30, -20, -10, 0, 10, 20, 30]:
        u_price = round(wo_strike * (1 + var / 100), 2)
        level = round(100 + var, 2)
        if var >= -(100 - barrier_pct):
            total_c = annual_yield * years_to_mat / 100 * nominal
            redemption = round(nominal + total_c, 2)
        else:
            redemption = round(nominal * (1 + var / 100), 2)
        pl_pct = round((redemption - ask_price) / ask_price * 100, 2)
        pl_annual = round(pl_pct / years_to_mat, 2) if years_to_mat > 0 else 0
        scenarios.append({
            'variation_pct': var,
            'underlying_price': u_price,
            'underlying_level': level,
            'redemption': redemption,
            'pl_pct': pl_pct,
            'pl_annual': pl_annual
        })
    return {
        'scenarios': scenarios,
        'years_to_maturity': round(years_to_mat, 2),
        'worst_underlying': wo_name or 'N/A',
        'purchase_price': round(ask_price, 2),
        'barrier_pct': barrier_pct,
        'nominal': nominal
    }


async def retry_goto(page, url: str, retries: int = RETRY_COUNT) -> bool:
    for attempt in range(retries):
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
            await page.wait_for_timeout(2000)
            return True
        except Exception as e:
            print(f"  Attempt {attempt+1}/{retries} failed: {str(e)[:50]}")
            if attempt < retries - 1:
                await asyncio.sleep(3)
    return False


# ================================================================
# STEP 1: Parse the 14-column table
# ================================================================

def parse_table_row(cols) -> Optional[Dict]:
    if len(cols) < 14:
        return None

    for col in cols:
        for br in col.find_all('br'):
            br.replace_with('\n')

    col_texts = [col.get_text(strip=True) for col in cols]

    isin = ''
    a_tag = cols[0].find('a')
    if a_tag:
        href = a_tag.get('href', '')
        m = re.search(r'isin=([A-Z0-9]+)', href)
        if m:
            isin = m.group(1)
        else:
            isin = a_tag.get_text(strip=True)
    else:
        isin = col_texts[0]

    if not re.match(r'^[A-Z]{2}[A-Z0-9]{9,11}$', isin):
        return None

    wo_name, wo_strike = parse_worst_of(col_texts[5])

    return {
        'isin': isin,
        'nome': col_texts[1],
        'emittente': col_texts[2],
        'scadenza': col_texts[3],
        'sottostante': col_texts[4],
        'wo_name': wo_name,
        'wo_strike': wo_strike,
        'ask': parse_number(col_texts[6]),
        'premio': parse_number(col_texts[8]),
        'frequenza': col_texts[9],
        'has_memory': '*' in col_texts[8],
        'barr_premio': parse_number(col_texts[10]),
        'barr_capitale': parse_number(col_texts[11]),
        'divisa': col_texts[12].strip(),
        'mercato': col_texts[13].strip(),
    }


async def scrape_list_page(page) -> List[Dict]:
    print(f"Scraping: {SOURCE_NAME}")
    print(f"URL: {SOURCE_URL}")

    if not await retry_goto(page, SOURCE_URL):
        print("Failed to load page!")
        return []

    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')
    results = []

    for table in soup.find_all('table', class_=re.compile(r'table')):
        thead = table.find('thead')
        if not thead:
            continue
        header_text = thead.get_text().lower()
        if 'isin' not in header_text:
            continue

        print(f"  Found target table with header containing 'isin'")

        for row in table.find_all('tr'):
            cols = row.find_all('td')
            if len(cols) < 14:
                continue
            parsed = parse_table_row(cols)
            if parsed:
                results.append(parsed)

    print(f"  Found {len(results)} certificates")
    return results


# ================================================================
# STEP 2: Scrape detail page for enrichment (FIXED)
# ================================================================

async def scrape_detail(page, isin: str) -> Dict:
    """Scrape certificate detail page for barrier type, issue date, and
    full underlyings table with strike, spot, barrier values."""
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    extra = {'barrier_type': 'European', 'issue_date': None, 'nominal': 1000,
             'strike_date': None, 'final_valuation_date': None, 'underlyings_detail': []}

    if not await retry_goto(page, url):
        return extra

    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')

    # Tipo barriera
    page_text = soup.get_text().lower()
    if 'continua' in page_text:
        extra['barrier_type'] = 'American'
    elif 'discreta' in page_text:
        extra['barrier_type'] = 'European'

    # Extract barrier data from AJAX script params
    # The page has: barriera: "55 %", livello: "622,732", tipo: "DISCRETA", raggiunta: "false"
    for script in soup.find_all('script'):
        script_text = script.get_text()
        if 'barriera' in script_text and 'livello' in script_text:
            # Extract barrier percentage
            barr_match = re.search(r'barriera:\s*"([^"]+)"', script_text)
            if barr_match:
                barr_val = parse_number(barr_match.group(1))
                if barr_val:
                    extra['barrier_pct_from_page'] = barr_val

            # Extract barrier absolute level (for worst-of)
            liv_match = re.search(r'livello:\s*"([^"]+)"', script_text)
            if liv_match:
                liv_val = parse_number(liv_match.group(1))
                if liv_val:
                    extra['barrier_level_abs'] = liv_val

            # Extract barrier type
            tipo_match = re.search(r'tipo:\s*"([^"]+)"', script_text)
            if tipo_match:
                tipo = tipo_match.group(1).strip().upper()
                if 'DISCRETA' in tipo:
                    extra['barrier_type'] = 'European'
                elif 'CONTINUA' in tipo:
                    extra['barrier_type'] = 'American'

            # Extract if barrier was reached
            ragg_match = re.search(r'raggiunta:\s*"([^"]+)"', script_text)
            if ragg_match:
                extra['barrier_reached'] = ragg_match.group(1).strip().lower() == 'true'
            break

    # Data emissione + altri campi dalla tabella dati
    for row in soup.find_all('tr'):
        cells = row.find_all(['th', 'td'])
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)
            if 'data emissione' in label:
                extra['issue_date'] = parse_date(value)
            elif 'nominale' in label and 'prezzo' not in label:
                nom = parse_number(value)
                if nom:
                    extra['nominal'] = nom
            elif 'data strike' in label:
                extra['strike_date'] = parse_date(value)
            elif 'data valutazione finale' in label or 'valutazione finale' in label:
                extra['final_valuation_date'] = parse_date(value)

    # =============================================
    # Sottostanti - FIXED: extract ALL columns
    # =============================================
    found_underlyings = False

    # Strategy 1: Find table with matching headers
    for table in soup.find_all('table'):
        header_row = table.find('tr')
        if not header_row:
            continue
        headers = [cell.get_text(strip=True).lower() for cell in header_row.find_all(['th', 'td'])]
        header_text = ' '.join(headers)

        if not any(kw in header_text for kw in ['sottostante', 'strike', 'valore iniziale', 'ultimo', 'spot']):
            continue

        col_map = {}
        for i, h in enumerate(headers):
            hl = h.lower()
            if any(kw in hl for kw in ['sottostante', 'nome', 'descrizione']):
                col_map['name'] = i
            elif any(kw in hl for kw in ['valore iniziale', 'strike', 'val. iniz', 'val.iniz']):
                col_map['strike'] = i
            elif any(kw in hl for kw in ['ultimo', 'spot', 'prezzo', 'valore attuale', 'val. att', 'val.att']):
                col_map['spot'] = i
            elif 'barriera' in hl or 'barrier' in hl:
                if 'distanza' not in hl and 'dist' not in hl:
                    col_map['barrier'] = i
            elif 'distanza' in hl or 'dist' in hl:
                col_map['distance'] = i

        if 'name' not in col_map:
            continue

        print(f"    Underlyings table columns: {col_map}")

        for row in table.find_all('tr')[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            name_idx = col_map.get('name', 0)
            if name_idx >= len(cells):
                continue

            name = cells[name_idx].get_text(strip=True)
            if not name or name.lower() in ['sottostante', 'nome', 'descrizione', '']:
                continue

            def safe_get(idx_key):
                idx = col_map.get(idx_key)
                if idx is not None and idx < len(cells):
                    return parse_number(cells[idx].get_text(strip=True))
                return None

            extra['underlyings_detail'].append({
                'name': name,
                'strike': safe_get('strike') or 0,
                'spot': safe_get('spot') or 0,
                'barrier': safe_get('barrier') or 0,
            })
            found_underlyings = True

        if found_underlyings:
            break

    # Strategy 2: Fallback - heading + table
    if not found_underlyings:
        for heading in soup.find_all(['h4', 'h3', 'h5', 'strong', 'b', 'div']):
            if 'sottostant' not in heading.get_text(strip=True).lower():
                continue
            table = heading.find_next('table')
            if not table:
                continue
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                name = cells[0].get_text(strip=True)
                if not name or name.upper() in ['DESCRIZIONE', 'SOTTOSTANTE', 'NOME', '']:
                    continue
                extra['underlyings_detail'].append({
                    'name': name,
                    'strike': parse_number(cells[1].get_text(strip=True)) if len(cells) > 1 else 0,
                    'spot': parse_number(cells[2].get_text(strip=True)) if len(cells) > 2 else 0,
                    'barrier': parse_number(cells[3].get_text(strip=True)) if len(cells) > 3 else 0,
                })
                found_underlyings = True
            break

    return extra


# ================================================================
# STEP 3: Build output in frontend format (FIXED)
# ================================================================

def build_certificate(raw: Dict, detail: Optional[Dict] = None) -> Dict:
    freq_mult, freq_name = get_freq_multiplier(raw.get('frequenza', ''))
    premio = raw.get('premio') or 0
    annual_yield = round(premio * freq_mult, 2)

    barrier_pct = barrier_as_pct(raw.get('barr_capitale'), raw.get('wo_strike'))
    if barrier_pct is None:
        barrier_pct = barrier_as_pct(raw.get('barr_premio'), raw.get('wo_strike'))
    if barrier_pct is None:
        barrier_pct = 0

    maturity_date = parse_date(raw.get('scadenza', ''))
    issue_date = detail.get('issue_date') if detail else None

    years_to_mat = 3.0
    if maturity_date:
        try:
            mat_dt = datetime.strptime(maturity_date, '%Y-%m-%d')
            years_to_mat = max(0.1, (mat_dt - datetime.now()).days / 365.25)
        except:
            pass

    ask_price = raw.get('ask') or 1000.0
    wo_name = raw.get('wo_name', '')
    wo_strike = raw.get('wo_strike')

    sotto_names = [n.strip() for n in raw.get('sottostante', '').split(';') if n.strip()]
    detail_und = detail.get('underlyings_detail', []) if detail else []

    # --- FIX: Detect certificate type properly ---
    cert_type = detect_certificate_type(raw.get('nome', ''))

    # --- Build underlyings list ---
    underlyings = []
    if detail_und:
        for u in detail_und:
            is_worst = wo_name and wo_name.lower().strip() == u['name'].lower().strip()
            u_strike = u.get('strike', 0)
            u_barrier = u.get('barrier', 0)

            if not u_barrier and u_strike and barrier_pct:
                u_barrier = round(u_strike * barrier_pct / 100, 2)

            underlyings.append({
                'name': u['name'],
                'strike': u_strike,
                'barrier': u_barrier,
                'worst_of': is_worst
            })
    else:
        for name in sotto_names:
            is_worst = wo_name and wo_name.lower().strip() == name.lower().strip()
            u_strike = wo_strike if is_worst else 0
            u_barrier = 0
            if is_worst and raw.get('barr_capitale'):
                u_barrier = raw.get('barr_capitale')
            elif is_worst and u_strike and barrier_pct:
                u_barrier = round(u_strike * barrier_pct / 100, 2)
            underlyings.append({
                'name': name, 'strike': u_strike or 0,
                'barrier': u_barrier or 0,
                'worst_of': is_worst
            })

    if not underlyings and wo_name:
        underlyings.append({
            'name': wo_name, 'strike': wo_strike or 0,
            'barrier': raw.get('barr_capitale') or 0,
            'worst_of': True
        })

    market_map = {'CX': 'CERT-X', 'SX': 'SeDeX'}
    market = raw.get('mercato', 'CERT-X')
    market = market_map.get(market, market)

    buffer_from_barrier = round(100 - barrier_pct, 2) if barrier_pct else 0

    # --- FIX: Pass wo_name to scenario ---
    scenario = generate_scenarios(barrier_pct, ask_price, annual_yield, years_to_mat, wo_strike or 0, wo_name)

    return {
        'isin': raw['isin'],
        'name': raw.get('nome', ''),
        'type': cert_type,
        'issuer': raw.get('emittente', ''),
        'market': market,
        'currency': raw.get('divisa', 'EUR'),
        'ask_price': ask_price,
        'nominal': detail.get('nominal', 1000) if detail else 1000,
        'issue_date': issue_date or maturity_date,
        'maturity_date': maturity_date,
        'strike_date': detail.get('strike_date') if detail else None,
        'final_valuation_date': detail.get('final_valuation_date') or maturity_date if detail else maturity_date,
        'barrier_down': barrier_pct,
        'barrier_type': detail.get('barrier_type', 'European') if detail else 'European',
        'coupon': premio,
        'coupon_frequency': freq_name,
        'has_memory': raw.get('has_memory', False),
        'annual_coupon_yield': annual_yield,
        'underlyings': underlyings,
        'buffer_from_barrier': buffer_from_barrier,
        'scenario_analysis': scenario,
        'source': 'CED'
    }


def cert_needs_enrichment(cert: Dict) -> bool:
    """Check if a certificate has incomplete data and needs re-enrichment."""
    underlyings = cert.get('underlyings', [])
    if not underlyings:
        return True
    # If all strikes are 0, data was never properly scraped from detail page
    all_strikes_zero = all(u.get('strike', 0) == 0 for u in underlyings)
    return all_strikes_zero


# ================================================================
# MAIN
# ================================================================

async def main():
    print("=" * 60)
    print("CED Scraper v16 - FIXED")
    print(f"Filtri: Indici, Commodities, Valute, Tassi, Credit")
    print(f"Ultimi {RECENT_DAYS} giorni, max {MAX_CERTIFICATES} dettagli")
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

        # === 1. Scrape la tabella prodotti ===
        all_raw = await scrape_list_page(page)

        if not all_raw:
            print("No certificates found in table! Keeping existing data.")
            await browser.close()
            return

        by_isin = {}
        for row in all_raw:
            if row['isin'] not in by_isin:
                by_isin[row['isin']] = row
        print(f"Total unique certificates: {len(by_isin)}")

        # === 2. Filtra sottostanti validi ===
        filtered = {}
        skipped = 0

        for isin, raw in by_isin.items():
            sottostante = raw.get('sottostante', '')
            nome = raw.get('nome', '')
            full_text = f"{sottostante} {nome}".lower()

            sotto_list = [n.strip() for n in sottostante.split(';') if n.strip()]
            has_valid = any(is_valid_underlying(n) for n in sotto_list)

            if has_valid:
                filtered[isin] = raw
            elif any(kw in full_text for kw in VALID_KEYWORDS):
                filtered[isin] = raw
            elif sotto_list:
                skipped += 1

        print(f"After filter: {len(filtered)} valid certificates")
        print(f"Skipped (stocks): {skipped}")

        if not filtered:
            print("No valid certificates after filtering! Keeping existing data.")
            await browser.close()
            return

        # === 3. Determine which ISINs need detail enrichment ===
        existing_certs = {}
        if os.path.exists('certificates-data.json'):
            try:
                with open('certificates-data.json', 'r', encoding='utf-8') as f:
                    old_data = json.load(f)
                if old_data.get('certificates'):
                    for c in old_data['certificates']:
                        existing_certs[c['isin']] = c
            except:
                pass

        # FIX: Enrich NEW ISINs AND ISINs with incomplete data
        isins_to_enrich = []
        for isin in filtered:
            if isin not in existing_certs:
                isins_to_enrich.append(isin)
            elif cert_needs_enrichment(existing_certs[isin]):
                isins_to_enrich.append(isin)

        detail_count = min(len(isins_to_enrich), MAX_CERTIFICATES)
        details = {}

        # Preserve good detail data from previous runs
        saved_details = {}
        for isin, cert in existing_certs.items():
            if cert.get('underlyings') and any(u.get('strike', 0) > 0 for u in cert['underlyings']):
                saved_details[isin] = {
                    'barrier_type': cert.get('barrier_type', 'European'),
                    'issue_date': cert.get('issue_date'),
                    'nominal': cert.get('nominal', 1000),
                    'strike_date': cert.get('strike_date'),
                    'final_valuation_date': cert.get('final_valuation_date'),
                    'underlyings_detail': [
                        {'name': u.get('name', ''), 'strike': u.get('strike', 0),
                         'barrier': u.get('barrier', 0)}
                        for u in cert['underlyings']
                    ]
                }

        if isins_to_enrich:
            new_enrich = sum(1 for i in isins_to_enrich if i not in existing_certs)
            re_enrich = len(isins_to_enrich) - new_enrich
            print(f"\nDetail enrichment: {detail_count} ISINs ({new_enrich} new, {re_enrich} re-enriching incomplete)...\n")

            for i, isin in enumerate(isins_to_enrich[:MAX_CERTIFICATES], 1):
                print(f"[{i}/{detail_count}] {isin}...", end=" ", flush=True)
                try:
                    detail = await scrape_detail(page, isin)
                    details[isin] = detail
                    bt = detail.get('barrier_type', '?')
                    nu = len(detail.get('underlyings_detail', []))
                    has_strikes = any(u.get('strike', 0) > 0 for u in detail.get('underlyings_detail', []))
                    print(f"OK barrier:{bt} und:{nu} strikes:{'yes' if has_strikes else 'no'}")
                except Exception as e:
                    print(f"ERR {str(e)[:40]}")
                    details[isin] = {}
                await asyncio.sleep(REQUEST_DELAY)
        else:
            print(f"\nAll {len(filtered)} ISINs have complete data, skipping detail enrichment")

        await browser.close()

        # === 4. Load existing certificates (ACCUMULATIVE) ===
        max_age = datetime.now() - timedelta(days=730)

        if existing_certs:
            print(f"\nLoaded {len(existing_certs)} existing certificates from previous runs")

        # === 5. Build certificates ===
        new_count = 0
        updated_count = 0
        for isin, raw in filtered.items():
            # FIX: Use fresh detail > saved detail > empty
            detail = details.get(isin) or saved_details.get(isin) or {}
            cert = build_certificate(raw, detail)
            if isin in existing_certs:
                updated_count += 1
            else:
                new_count += 1
            existing_certs[isin] = cert

        print(f"New certificates added: {new_count}")
        print(f"Existing certificates updated: {updated_count}")

        # === 6. Remove expired certificates ===
        purged = 0
        final_certs = {}
        for isin, cert in existing_certs.items():
            mat = cert.get('maturity_date')
            if mat:
                try:
                    mat_dt = datetime.strptime(mat, '%Y-%m-%d')
                    if mat_dt < max_age:
                        purged += 1
                        continue
                except:
                    pass
            final_certs[isin] = cert

        if purged:
            print(f"Purged {purged} certificates (maturity > 2 years ago)")

        certificates = list(final_certs.values())
        certificates.sort(key=lambda c: c.get('annual_coupon_yield', 0), reverse=True)

        # === 7. Save files ===
        output = {
            'success': True,
            'count': len(certificates),
            'certificates': certificates,
            'metadata': {
                'version': 'v16-fixed',
                'source': 'certificatiederivati.it',
                'criteria': 'Indici, Commodities, Valute, Tassi, Credit Linked',
                'max_history_years': 2,
                'new_this_run': new_count,
                'updated_this_run': updated_count,
                'purged_expired': purged,
                'total_scraped_this_run': len(by_isin),
                'after_filter_this_run': len(filtered),
                'detail_enriched_this_run': len(details),
                'timestamp': datetime.now().isoformat(),
                'scrape_date': datetime.now().strftime('%Y-%m-%d'),
                'categories': len(set(c['type'] for c in certificates)),
                'underlyings_db': len(set(
                    u['name'] for c in certificates for u in c['underlyings']
                )),
            }
        }

        with open('certificates-data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        df = pd.DataFrame(certificates)
        df.to_json('certificates-recenti.json', orient='records', indent=2, force_ascii=False)
        df.to_csv('certificates-recenti.csv', index=False)

        print("\n" + "=" * 60)
        print("COMPLETED (v16 FIXED)")
        print(f"  Total certificates in database: {len(certificates)}")
        print(f"  New added this run: {new_count}")
        print(f"  Updated this run: {updated_count}")
        print(f"  Purged (expired >2y): {purged}")
        print(f"  Skipped (stocks): {skipped}")
        print(f"  Detail pages visited: {len(details)}")
        print(f"  Saved: certificates-data.json, certificates-recenti.json, certificates-recenti.csv")
        print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
