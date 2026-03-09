#!/usr/bin/env python3
"""
CED Scraper v17 - MULTI-SOURCE
Scrapes certificates from:
1. CED Advanced Search (db_bs_ricerca_avanzata.asp) - per each target index
2. CED Banca Generali promo page (bs_promo_bgenerali.asp) - existing source
3. Detail page enrichment for premio, ask, frequenza, underlyings

Copyright (c) 2024-2026 Mutna S.R.L.S. - All Rights Reserved
"""

import asyncio
import json
import os
import re
import urllib.parse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# ============ CONFIG ============
MAX_DETAIL_ISIN = int(os.getenv('MAX_DETAIL_ISIN', '200'))
REQUEST_DELAY = 2.0
PAGE_TIMEOUT = 60000
RETRY_COUNT = 3

SEARCH_URL = 'https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp?db=2'
BG_PROMO_URL = 'https://www.certificatiederivati.it/bs_promo_bgenerali.asp?t=redazione'
DETAIL_URL = 'https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin='

# Target indices/sottostanti to search in CED advanced search
# Must match dropdown options; matching uses word boundaries
TARGET_SOTTOSTANTI = [
    # Major indices
    'Euro Stoxx 50',
    'FTSE Mib',
    'Dax',
    'Nasdaq 100',
    'Nikkei 225',
    'S&P 500',
    'SPDR S&P 500',
    'Russell 2000',
    'MSCI World',
    'SMI Swiss',
    'Hang Seng',
    'Hang Seng China',
    'CAC 40',
    'IBEX 35',
    # Sector indices
    'Eurostoxx Banks',
    'Eurostoxx Insurance',
    'Eurostoxx Utilities',
    'Eurostoxx Oil',
    'Eurostoxx HealthCare',
    'Eurostoxx Select Dividend',
    'Stoxx Europe 600 Health',
    'Stoxx Europe 600 Auto',
    # China
    'iShares China Large',
    # Rates
    'Euribor',
    'ESTRON',
    # Commodities - use precise names
    'Oro',
    'Gold Spot',
    'Petrolio WTI',
    'Brent Crude',
    'WTI Crude',
    'Argento',
    'Silver Spot',
    'Natural Gas',
    'Rame',
    'Copper',
]

# ============ CERTIFICATE TYPE DETECTION ============
CERT_TYPE_PATTERNS = [
    ('phoenix memory step down', 'Phoenix Memory Step Down'),
    ('phoenix memory airbag', 'Phoenix Memory Airbag'),
    ('phoenix memory darwin', 'Phoenix Memory Darwin'),
    ('phoenix memory convertible', 'Phoenix Memory Convertible'),
    ('phoenix memory maxi coupon', 'Phoenix Memory Maxi Coupon'),
    ('phoenix memory', 'Phoenix Memory'),
    ('phoenix', 'Phoenix'),
    ('cash collect memory basket star', 'Cash Collect Memory Basket Star'),
    ('cash collect memory airbag', 'Cash Collect Memory Airbag'),
    ('cash collect memory callable', 'Cash Collect Memory Callable'),
    ('cash collect memory', 'Cash Collect Memory'),
    ('cash collect', 'Cash Collect'),
    ('fixed cash collect', 'Fixed Cash Collect'),
    ('all coupon cash collect', 'All Coupon Cash Collect'),
    ('bonus outperformance', 'Bonus Outperformance'),
    ('bonus cap', 'Bonus Cap'),
    ('bonus', 'Bonus'),
    ('express maxi coupon', 'Express Maxi Coupon'),
    ('express', 'Express'),
    ('equity protection', 'Equity Protection'),
    ('equity premium', 'Equity Premium'),
    ('buy on dips', 'Buy On Dips'),
    ('butterfly', 'Butterfly'),
    ('twin win', 'Twin Win'),
    ('airbag', 'Airbag'),
    ('autocallable', 'Autocallable'),
    ('reverse convertible', 'Reverse Convertible'),
    ('digital', 'Digital'),
    ('credit linked', 'Credit Linked'),
    ('outperformance', 'Outperformance'),
    ('protect outperformance', 'Protect Outperformance'),
    ('tracker', 'Tracker'),
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


# ============ UNDERLYING FILTER (BG source) ============
# CED Search is already pre-filtered by TARGET_SOTTOSTANTI (server-side).
# BG Promo page returns ALL certificates including single stocks - we filter locally.

VALID_KEYWORDS = [
    # Indices
    'ftse', 'mib', 'stoxx', 'eurostoxx', 'euro stoxx', 'dax', 'cac', 'ibex',
    's&p', 'sp500', 'sp 500', 'nasdaq', 'dow jones', 'nikkei', 'hang seng',
    'russell', 'msci', 'smi', 'topix', 'kospi', 'sensex',
    'stoxx europe 600', 'eurostoxx bank', 'eurostoxx technolog',
    'eurostoxx insurance', 'eurostoxx utilit', 'eurostoxx oil',
    'eurostoxx basic', 'eurostoxx healthcare', 'eurostoxx health',
    'stoxx europe', 'select dividend',
    # ETF su indici
    'ishares msci', 'ishares china', 'spdr', 'etf',
    # Commodities
    'oro', 'gold', 'silver', 'argento', 'petrolio', 'oil', 'brent', 'wti',
    'gas naturale', 'natural gas', 'copper', 'rame', 'platinum', 'platino',
    'palladium', 'palladio', 'crude', 'commodity',
    # Forex
    'eur/usd', 'usd/jpy', 'gbp/usd', 'forex', 'currency', 'valuta', 'cambio',
    # Tassi
    'euribor', 'libor', 'bund', 'btp', 'treasury', 'swap', 'yield', 'tasso',
    'estron',
    # Credit (no 'credit' generico - matcha Unicredit/Credit Agricole!)
    'credit linked', 'credit link', 'cln', 'cds',
    # Generic
    'index', 'indice', 'basket di indici', 'paniere',
]


def is_valid_underlying(name: str) -> bool:
    """Check if underlying matches indices/commodities/rates/credit linked."""
    if not name:
        return False
    return any(kw in name.lower().strip() for kw in VALID_KEYWORDS)


def filter_bg_by_underlying(bg_raw: Dict[str, Dict]) -> Tuple[Dict[str, Dict], int]:
    """Filter BG results to keep only valid underlyings (indices, commodities, etc.)."""
    filtered = {}
    skipped = 0
    for isin, raw in bg_raw.items():
        sottostante = raw.get('sottostante', '')
        nome = raw.get('nome', '')
        full_text = f"{sottostante} {nome}".lower()

        sotto_list = [n.strip() for n in sottostante.split(';') if n.strip()]
        has_valid = any(is_valid_underlying(n) for n in sotto_list)

        if has_valid:
            filtered[isin] = raw
        elif any(kw in full_text for kw in VALID_KEYWORDS):
            filtered[isin] = raw
        else:
            skipped += 1

    return filtered, skipped


# ============ HELPER FUNCTIONS ============

def parse_number(text: str) -> Optional[float]:
    if not text or text.strip().upper() in ['N.A.', 'N.D.', '-', '', 'N/A', '0']:
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
    match = re.match(r'^(.+?)\(([0-9.,]+)\)\s*$', text.strip())
    if match:
        name = match.group(1).strip()
        strike = parse_number(match.group(2))
        return name, strike
    return text.strip(), None


def get_freq_multiplier(freq_text: str) -> Tuple[float, str]:
    f = freq_text.lower().strip() if freq_text else ''
    if 'mensil' in f or 'month' in f:
        return 12.0, 'monthly'
    elif 'trimest' in f or 'quarter' in f:
        return 4.0, 'quarterly'
    elif 'semest' in f or 'semi' in f:
        return 2.0, 'semiannual'
    elif 'annual' in f or 'year' in f or 'ann' in f:
        return 1.0, 'annual'
    return 12.0, 'monthly'


def barrier_as_pct(barrier_abs, strike) -> Optional[float]:
    if barrier_abs and strike and strike > 0:
        pct = round((barrier_abs / strike) * 100, 2)
        if 1 <= pct <= 100:
            return pct
    return None


async def retry_goto(page, url, retries=RETRY_COUNT):
    for attempt in range(retries):
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
            return True
        except Exception as e:
            if attempt < retries - 1:
                print(f"  Retry {attempt+1}/{retries} for {url[:60]}...")
                await asyncio.sleep(2)
            else:
                print(f"  FAILED after {retries} attempts: {str(e)[:50]}")
                return False


# ================================================================
# SOURCE 1: BG Promo Page (existing, proven)
# ================================================================

def parse_bg_table_row(cols) -> Optional[Dict]:
    """Parse a row from the BG promo table (14 columns)."""
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
        'source': 'BG_PROMO',
    }


async def scrape_bg_promo(page) -> Dict[str, Dict]:
    """Scrape BG promo page - returns dict of ISIN -> raw data."""
    print(f"\n--- Source 1: BG Promo Page ---")
    if not await retry_goto(page, BG_PROMO_URL):
        print("  Failed to load BG page!")
        return {}

    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')
    results = {}

    for table in soup.find_all('table', class_=re.compile(r'table')):
        thead = table.find('thead')
        if not thead:
            continue
        if 'isin' not in thead.get_text().lower():
            continue
        for row in table.find_all('tr'):
            cols = row.find_all('td')
            if len(cols) < 14:
                continue
            parsed = parse_bg_table_row(cols)
            if parsed and parsed['isin'] not in results:
                results[parsed['isin']] = parsed

    print(f"  Found {len(results)} certificates from BG promo")
    return results


# ================================================================
# SOURCE 2: CED Advanced Search (NEW)
# ================================================================

async def scrape_ced_search(page) -> Dict[str, Dict]:
    """Search CED using direct URL navigation (bypasses fragile form interaction).
    
    Step 1: Load search page once, find the sottostante dropdown, extract all option values
    Step 2: Match our TARGET_SOTTOSTANTI against actual dropdown options
    Step 3: For each match, navigate directly to results URL with sottostanteC parameter
    """
    print(f"\n--- Source 2: CED Advanced Search ---")
    all_results = {}

    # Step 1: Load search page and find the CORRECT sottostante dropdown
    if not await retry_goto(page, SEARCH_URL):
        print("  Failed to load CED search page!")
        return {}

    await asyncio.sleep(2)

    # Identify all selects with their names and option counts
    selects_info = await page.evaluate('''() => {
        const selects = document.querySelectorAll('select');
        return Array.from(selects).map((sel, idx) => ({
            index: idx,
            name: sel.name || '',
            id: sel.id || '',
            optionCount: sel.options.length,
            firstOptions: Array.from(sel.options).slice(0, 5).map(o => o.text.trim()),
        }));
    }''')

    print(f"  Found {len(selects_info)} select elements:")
    for si in selects_info:
        print(f"    [{si['index']}] name='{si['name']}' id='{si['id']}' options={si['optionCount']} first={si['firstOptions'][:3]}")

    # Find the sottostante dropdown - by name or by being the largest select
    sotto_select_name = None
    for si in selects_info:
        name_lower = si['name'].lower()
        if 'sottostante' in name_lower:
            sotto_select_name = si['name']
            print(f"  -> Identified sottostante dropdown: name='{si['name']}' with {si['optionCount']} options")
            break

    if not sotto_select_name:
        # Fallback: pick the select with the MOST options (sottostante has thousands)
        if selects_info:
            biggest = max(selects_info, key=lambda s: s['optionCount'])
            if biggest['optionCount'] > 100:
                sotto_select_name = biggest['name']
                print(f"  -> Fallback: using largest select name='{sotto_select_name}' with {biggest['optionCount']} options")

    if not sotto_select_name:
        print("  Could not identify sottostante dropdown!")
        return {}

    # Step 2: Extract ALL option values from sottostante dropdown
    all_options = await page.evaluate(f'''() => {{
        const sel = document.querySelector('select[name="{sotto_select_name}"]');
        if (!sel) return [];
        return Array.from(sel.options).map(o => ({{
            value: o.value,
            text: o.text.trim()
        }})).filter(o => o.value && o.text && o.text !== '---');
    }}''')

    print(f"  Total sottostante options: {len(all_options)}")

    # Step 3: Match our targets against actual options
    # Skip derivatives (Call/Put), ETFs (Amundi/iShares/SPDR), and FBK products
    SKIP_PREFIXES = ['call ', 'put ', 'fbk ', 'amundi ', 'xtrackers ', 'global x ']

    matched = []
    for target in TARGET_SOTTOSTANTI:
        target_lower = target.lower().strip()
        target_words = target_lower.split()

        best_match = None

        for opt in all_options:
            opt_lower = opt['text'].lower().strip()

            # Skip obvious derivatives and ETFs
            if any(opt_lower.startswith(skip) for skip in SKIP_PREFIXES):
                continue
            # Skip options with dates (likely derivatives): contain patterns like 19DEC25, MAR26
            if re.search(r'\d{2}[A-Z]{3}\d{2}', opt['text']):
                continue
            # Skip options with strike prices like B40000, B3500
            if re.search(r'\bB\d{3,}', opt['text']):
                continue

            # Exact match (case insensitive)
            if opt_lower == target_lower:
                best_match = {'target': target, 'value': opt['value'], 'label': opt['text']}
                break

            # Option starts with target (e.g. "Euro Stoxx 50" matches "Euro Stoxx 50 ...")
            if opt_lower.startswith(target_lower + ' ') or opt_lower.startswith(target_lower):
                if not best_match or len(opt_lower) < len(best_match['label']):
                    best_match = {'target': target, 'value': opt['value'], 'label': opt['text']}
                continue

            # Word-boundary match (prevents "oro" matching "clorox", "gold" matching "anglogold")
            if re.search(r'\b' + re.escape(target_lower) + r'\b', opt_lower):
                if not best_match:
                    best_match = {'target': target, 'value': opt['value'], 'label': opt['text']}
                continue

        if not best_match:
            # Last resort: word-by-word match (all words present)
            for opt in all_options:
                opt_lower = opt['text'].lower().strip()
                if any(opt_lower.startswith(skip) for skip in SKIP_PREFIXES):
                    continue
                if re.search(r'\d{2}[A-Z]{3}\d{2}', opt['text']):
                    continue
                if all(w in opt_lower for w in target_words):
                    best_match = {'target': target, 'value': opt['value'], 'label': opt['text']}
                    break

        if best_match:
            matched.append(best_match)
        else:
            print(f"  [{target}] No match in dropdown options")

    print(f"  Matched {len(matched)}/{len(TARGET_SOTTOSTANTI)} targets")

    # Step 4: For each matched target, navigate to results URL directly
    BASE_RESULTS_URL = 'https://www.certificatiederivati.it/db_bs_estrazione_ricerca.asp'

    for m in matched:
        try:
            # Build direct URL - bypass form completely
            params = {
                'db': '2',  # investment certificates
                'nome': '',
                sotto_select_name: m['value'],
                'emittente': '',
                'FiltroDal': '2020-1-1',
                'FiltroAl': '2099-12-31',
                'divisa': '',
                'fase': '',
            }
            url = f"{BASE_RESULTS_URL}?{urllib.parse.urlencode(params)}"

            if not await retry_goto(page, url):
                print(f"  [{m['label']}] Failed to load results page")
                continue

            await asyncio.sleep(1)

            # Parse results
            html = await page.content()
            soup = BeautifulSoup(html, 'lxml')

            # Find result count
            count_text = soup.get_text()
            count_match = re.search(r'(\d+)\s*Certificat', count_text)
            total = int(count_match.group(1)) if count_match else 0

            # Parse results table
            found = 0
            for table in soup.find_all('table'):
                header = table.get_text().lower()
                if 'isin' not in header or 'nome' not in header:
                    continue

                rows = table.find_all('tr')
                for row in rows[1:]:  # skip header
                    cells = row.find_all('td')
                    if len(cells) < 6:
                        continue

                    cell_texts = [c.get_text(strip=True) for c in cells]
                    isin = cell_texts[0].strip()

                    # Extract ISIN from link if present
                    a_tag = cells[0].find('a')
                    if a_tag:
                        href = a_tag.get('href', '')
                        isin_match = re.search(r'isin=([A-Z0-9]+)', href)
                        if isin_match:
                            isin = isin_match.group(1)

                    if not re.match(r'^[A-Z]{2}[A-Z0-9]{9,11}$', isin):
                        continue

                    nome = cell_texts[1] if len(cell_texts) > 1 else ''
                    emittente = cell_texts[2] if len(cell_texts) > 2 else ''
                    sottostante = cell_texts[3] if len(cell_texts) > 3 else m['label']
                    strike = parse_number(cell_texts[4]) if len(cell_texts) > 4 else None
                    barriera = parse_number(cell_texts[5]) if len(cell_texts) > 5 else None
                    scadenza = cell_texts[7] if len(cell_texts) > 7 else ''

                    if isin not in all_results:
                        all_results[isin] = {
                            'isin': isin,
                            'nome': nome,
                            'emittente': emittente,
                            'scadenza': scadenza,
                            'sottostante': sottostante,
                            'wo_name': '',
                            'wo_strike': strike,
                            'ask': None,
                            'premio': None,
                            'frequenza': '',
                            'has_memory': 'memory' in nome.lower(),
                            'barr_premio': barriera,
                            'barr_capitale': barriera,
                            'divisa': 'EUR',
                            'mercato': 'CERT-X',
                            'source': 'CED_SEARCH',
                            'search_strike': strike,
                            'search_barrier': barriera,
                        }
                        found += 1

                break  # found the right table

            # Check for pagination (multiple pages of results)
            pages_found = 1
            page_links = soup.find_all('a', href=re.compile(r'p=\d+'))
            if page_links:
                page_nums = set()
                for pl in page_links:
                    pm = re.search(r'p=(\d+)', pl.get('href', ''))
                    if pm:
                        page_nums.add(int(pm.group(1)))
                if page_nums:
                    max_page = min(max(page_nums), 5)  # cap at 5 pages per target
                    pages_found = max_page

                    # Fetch additional pages
                    for pg in range(2, max_page + 1):
                        params_pg = dict(params)
                        params_pg['p'] = str(pg)
                        url_pg = f"{BASE_RESULTS_URL}?{urllib.parse.urlencode(params_pg)}"
                        if not await retry_goto(page, url_pg):
                            break
                        await asyncio.sleep(1)

                        html_pg = await page.content()
                        soup_pg = BeautifulSoup(html_pg, 'lxml')

                        for table in soup_pg.find_all('table'):
                            hdr = table.get_text().lower()
                            if 'isin' not in hdr or 'nome' not in hdr:
                                continue
                            for row in table.find_all('tr')[1:]:
                                cells = row.find_all('td')
                                if len(cells) < 6:
                                    continue
                                ct = [c.get_text(strip=True) for c in cells]
                                isin2 = ct[0].strip()
                                a2 = cells[0].find('a')
                                if a2:
                                    href2 = a2.get('href', '')
                                    im2 = re.search(r'isin=([A-Z0-9]+)', href2)
                                    if im2:
                                        isin2 = im2.group(1)
                                if not re.match(r'^[A-Z]{2}[A-Z0-9]{9,11}$', isin2):
                                    continue
                                if isin2 not in all_results:
                                    all_results[isin2] = {
                                        'isin': isin2,
                                        'nome': ct[1] if len(ct) > 1 else '',
                                        'emittente': ct[2] if len(ct) > 2 else '',
                                        'scadenza': ct[7] if len(ct) > 7 else '',
                                        'sottostante': ct[3] if len(ct) > 3 else m['label'],
                                        'wo_name': '',
                                        'wo_strike': parse_number(ct[4]) if len(ct) > 4 else None,
                                        'ask': None,
                                        'premio': None,
                                        'frequenza': '',
                                        'has_memory': 'memory' in (ct[1] if len(ct) > 1 else '').lower(),
                                        'barr_premio': parse_number(ct[5]) if len(ct) > 5 else None,
                                        'barr_capitale': parse_number(ct[5]) if len(ct) > 5 else None,
                                        'divisa': 'EUR',
                                        'mercato': 'CERT-X',
                                        'source': 'CED_SEARCH',
                                        'search_strike': parse_number(ct[4]) if len(ct) > 4 else None,
                                        'search_barrier': parse_number(ct[5]) if len(ct) > 5 else None,
                                    }
                                    found += 1
                            break

            print(f"  [{m['label']}] {found} new certificates (pages: {pages_found})")

        except Exception as e:
            print(f"  [{m['target']}] Error: {str(e)[:80]}")

        await asyncio.sleep(REQUEST_DELAY)

    print(f"\n  Total from CED search: {len(all_results)} unique certificates")
    return all_results


# ================================================================
# DETAIL PAGE ENRICHMENT (enhanced)
# ================================================================

async def scrape_detail(page, isin: str, debug: bool = False) -> Dict:
    """Scrape certificate detail page for full data."""
    url = DETAIL_URL + isin
    extra = {
        'barrier_type': 'European', 'issue_date': None, 'nominal': 1000,
        'strike_date': None, 'final_valuation_date': None,
        'underlyings_detail': [],
        'ask_price': None, 'premio': None, 'frequenza': None,
        'divisa': 'EUR', 'mercato': 'CERT-X',
    }

    if not await retry_goto(page, url):
        return extra

    # Wait for page to settle (AJAX content)
    await page.wait_for_timeout(3000)

    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')
    page_text = soup.get_text().lower()

    if debug:
        tables = soup.find_all('table')
        print(f"\n    DEBUG {isin}: {len(tables)} tables found")
        for i, t in enumerate(tables[:8]):
            rows = t.find_all('tr')
            first_cells = []
            if rows:
                first_cells = [c.get_text(strip=True)[:30] for c in rows[0].find_all(['th', 'td'])[:4]]
            print(f"    Table[{i}]: {len(rows)} rows, headers: {first_cells}")

    # Barrier type
    if 'continua' in page_text:
        extra['barrier_type'] = 'American'
    elif 'discreta' in page_text:
        extra['barrier_type'] = 'European'

    # Extract from AJAX script params
    for script in soup.find_all('script'):
        script_text = script.get_text()
        if 'barriera' in script_text and 'livello' in script_text:
            barr_match = re.search(r'barriera:\s*"([^"]+)"', script_text)
            if barr_match:
                extra['barrier_pct_from_page'] = parse_number(barr_match.group(1))
            tipo_match = re.search(r'tipo:\s*"([^"]+)"', script_text)
            if tipo_match:
                tipo = tipo_match.group(1).strip().upper()
                if 'DISCRETA' in tipo:
                    extra['barrier_type'] = 'European'
                elif 'CONTINUA' in tipo:
                    extra['barrier_type'] = 'American'
            break

    # Extract key fields from data tables (ISIN, dates, nominal, etc.)
    for row in soup.find_all('tr'):
        cells = row.find_all(['th', 'td'])
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)
            if 'data emissione' in label:
                extra['issue_date'] = parse_date(value)
            elif 'nominale' in label and 'prezzo' not in label:
                v = parse_number(value)
                if v and v > 0:
                    extra['nominal'] = v
            elif 'data strike' in label or 'data fissazione' in label:
                extra['strike_date'] = parse_date(value)
            elif 'valutazione finale' in label:
                extra['final_valuation_date'] = parse_date(value)
            elif 'lettera' in label or 'ask' in label:
                extra['ask_price'] = parse_number(value)
            elif 'premio' in label and 'barr' not in label:
                extra['premio'] = parse_number(value)
            elif 'cedola' in label or 'coupon' in label:
                if not extra['premio']:
                    extra['premio'] = parse_number(value)
            elif 'frequenza' in label:
                extra['frequenza'] = value
            elif 'divisa' in label or 'valuta' in label:
                if value.strip().upper() in ['EUR', 'USD', 'GBP', 'CHF']:
                    extra['divisa'] = value.strip().upper()
            elif 'mercato' in label:
                if 'cert' in value.lower() or 'sedex' in value.lower():
                    extra['mercato'] = value.strip()

    # =============================================
    # UNDERLYINGS EXTRACTION
    # =============================================
    found_underlyings = False

    # Strategy 0 (NEW): Find table by DESCRIZIONE+STRIKE header pattern
    # CED detail pages use: DESCRIZIONE | STRIKE | PESO (not "sottostante")
    for table in soup.find_all('table'):
        first_row = table.find('tr')
        if not first_row:
            continue
        header_cells = first_row.find_all(['th', 'td'])
        header_texts = [c.get_text(strip=True).lower() for c in header_cells]

        has_desc = any('descrizione' in h or 'sottostante' in h for h in header_texts)
        has_strike = any('strike' in h or 'valore iniziale' in h or 'val. iniz' in h for h in header_texts)

        if not (has_desc and has_strike):
            continue

        if debug:
            print(f"    Strategy 0 MATCH: headers={header_texts}")

        # Map columns
        col_map = {}
        for i, hl in enumerate(header_texts):
            if 'descrizione' in hl or 'sottostante' in hl or 'nome' in hl:
                col_map['name'] = i
            elif 'strike' in hl or 'valore iniziale' in hl or 'val. iniz' in hl or 'val.iniz' in hl:
                col_map['strike'] = i
            elif 'peso' in hl:
                col_map['peso'] = i
            elif ('barriera' in hl or 'barrier' in hl) and 'distanza' not in hl:
                col_map['barrier'] = i

        if 'name' not in col_map:
            continue

        for row in table.find_all('tr')[1:]:  # skip header
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue
            name_idx = col_map.get('name', 0)
            if name_idx >= len(cells):
                continue
            name = cells[name_idx].get_text(strip=True)
            if not name or name.lower() in ['descrizione', 'sottostante', 'nome', '']:
                continue

            def safe_get(idx_key):
                idx = col_map.get(idx_key)
                if idx is not None and idx < len(cells):
                    return parse_number(cells[idx].get_text(strip=True))
                return None

            extra['underlyings_detail'].append({
                'name': name,
                'strike': safe_get('strike') or 0,
                'spot': 0,
                'barrier': safe_get('barrier') or 0,
            })
            found_underlyings = True

        if found_underlyings:
            if debug:
                print(f"    Strategy 0: found {len(extra['underlyings_detail'])} underlyings")
            break

    # Strategy 1: table where the FULL text contains 'sottostant'
    if not found_underlyings:
        for table in soup.find_all('table'):
            full_text = table.get_text().lower()
            if 'sottostant' not in full_text and 'underlying' not in full_text:
                continue
            header_row = table.find('tr')
            if not header_row:
                continue
            headers = header_row.find_all(['th', 'td'])
            header_labels = [h.get_text(strip=True).lower() for h in headers]

            col_map = {}
            for i, hl in enumerate(header_labels):
                if any(kw in hl for kw in ['sottostante', 'nome', 'descrizione', 'underlying']):
                    col_map['name'] = i
                elif any(kw in hl for kw in ['strike', 'valore iniziale', 'val. iniz', 'val.iniz']):
                    col_map['strike'] = i
                elif any(kw in hl for kw in ['ultimo', 'spot', 'prezzo', 'valore attuale', 'val. att']):
                    col_map['spot'] = i
                elif ('barriera' in hl or 'barrier' in hl) and 'distanza' not in hl:
                    col_map['barrier'] = i

            if 'name' not in col_map:
                continue

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

                def safe_get2(idx_key):
                    idx = col_map.get(idx_key)
                    if idx is not None and idx < len(cells):
                        return parse_number(cells[idx].get_text(strip=True))
                    return None

                extra['underlyings_detail'].append({
                    'name': name,
                    'strike': safe_get2('strike') or 0,
                    'spot': safe_get2('spot') or 0,
                    'barrier': safe_get2('barrier') or 0,
                })
                found_underlyings = True
            if found_underlyings:
                break

    # Strategy 2: heading with 'sottostant' then find next table
    if not found_underlyings:
        for heading in soup.find_all(['h3', 'h4', 'h5', 'strong', 'b']):
            heading_text = heading.get_text(strip=True).lower()
            if 'sottostant' not in heading_text:
                continue
            if debug:
                print(f"    Strategy 2: found heading '{heading_text}'")

            # Find next table, skipping ad/layout tables
            for sibling_table in heading.find_all_next('table'):
                rows = sibling_table.find_all('tr')
                if len(rows) < 2:
                    continue  # skip tiny/ad tables

                first_cells = sibling_table.find('tr').find_all(['th', 'td'])
                if len(first_cells) < 2:
                    continue  # not a data table

                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) < 2:
                        continue
                    name = cells[0].get_text(strip=True)
                    if not name or name.upper() in ['DESCRIZIONE', 'SOTTOSTANTE', 'NOME', '']:
                        continue
                    extra['underlyings_detail'].append({
                        'name': name,
                        'strike': parse_number(cells[1].get_text(strip=True)) if len(cells) > 1 else 0,
                        'spot': 0,
                        'barrier': parse_number(cells[3].get_text(strip=True)) if len(cells) > 3 else 0,
                    })
                    found_underlyings = True

                if found_underlyings:
                    break
            break

    if debug and not found_underlyings:
        print(f"    DEBUG: No underlyings found for {isin}")

    return extra


# ================================================================
# SCENARIO ANALYSIS
# ================================================================

def generate_scenarios(barrier_pct, purchase_price, annual_yield, years_to_mat, wo_strike, wo_name=''):
    if not barrier_pct or not wo_strike or wo_strike <= 0 or not purchase_price or purchase_price <= 0:
        return None

    scenarios = []
    total_coupons = annual_yield * years_to_mat / 100 * 1000
    nominal = 1000.0

    for var in [-70, -60, -50, -40, -30, -20, -10, 0, 10, 20, 30]:
        underlying_pct = 100 + var
        underlying_price = round(wo_strike * underlying_pct / 100, 2)

        if underlying_pct <= barrier_pct:
            redemption = round(nominal * underlying_pct / 100, 2)
        else:
            redemption = round(nominal + total_coupons, 2)

        pl = round(redemption - purchase_price, 2)
        pl_pct = round(pl / purchase_price * 100, 2)

        scenarios.append({
            'variation_pct': var,
            'underlying_price': underlying_price,
            'underlying_level': underlying_pct,
            'redemption': redemption,
            'pl_pct': pl_pct,
            'pl_annual': round(pl_pct / max(years_to_mat, 0.1), 2),
        })

    return {
        'scenarios': scenarios,
        'purchase_price': purchase_price,
        'worst_underlying': wo_name or 'N/A',
        'years_to_maturity': round(years_to_mat, 2),
    }


# ================================================================
# BUILD CERTIFICATE (works with both BG and CED search data)
# ================================================================

def build_certificate(raw: Dict, detail: Optional[Dict] = None) -> Dict:
    # Get premio/frequenza - prefer detail page, fallback to raw
    premio = None
    frequenza = ''

    if detail and detail.get('premio'):
        premio = detail['premio']
    elif raw.get('premio'):
        premio = raw['premio']
    else:
        premio = 0

    if detail and detail.get('frequenza'):
        frequenza = detail['frequenza']
    elif raw.get('frequenza'):
        frequenza = raw['frequenza']

    freq_mult, freq_name = get_freq_multiplier(frequenza)
    annual_yield = round(premio * freq_mult, 2) if premio else 0

    # Barrier
    barrier_pct = barrier_as_pct(raw.get('barr_capitale'), raw.get('wo_strike'))
    if barrier_pct is None:
        barrier_pct = barrier_as_pct(raw.get('barr_premio'), raw.get('wo_strike'))
    if barrier_pct is None and detail and detail.get('barrier_pct_from_page'):
        barrier_pct = detail['barrier_pct_from_page']
    if barrier_pct is None:
        # Try from search results
        if raw.get('search_barrier') and raw.get('search_strike') and raw['search_strike'] > 0:
            barrier_pct = barrier_as_pct(raw['search_barrier'], raw['search_strike'])
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

    # Ask price - prefer detail, fallback raw
    ask_price = None
    if detail and detail.get('ask_price'):
        ask_price = detail['ask_price']
    elif raw.get('ask'):
        ask_price = raw['ask']
    else:
        ask_price = 1000.0

    wo_name = raw.get('wo_name', '')
    wo_strike = raw.get('wo_strike')
    cert_type = detect_certificate_type(raw.get('nome', ''))

    # Build underlyings
    sotto_names = [n.strip() for n in raw.get('sottostante', '').split(';') if n.strip()]
    detail_und = detail.get('underlyings_detail', []) if detail else []

    underlyings = []
    if detail_und:
        for u in detail_und:
            is_worst = wo_name and wo_name.lower().strip() == u['name'].lower().strip()
            u_strike = u.get('strike', 0)
            u_barrier = u.get('barrier', 0)
            if not u_barrier and u_strike and barrier_pct:
                u_barrier = round(u_strike * barrier_pct / 100, 2)
            underlyings.append({
                'name': u['name'], 'strike': u_strike,
                'barrier': u_barrier, 'worst_of': is_worst
            })
    elif sotto_names:
        for name in sotto_names:
            is_worst = wo_name and wo_name.lower().strip() == name.lower().strip()
            u_strike = wo_strike if is_worst else 0
            u_barrier = 0
            if is_worst and barrier_pct and u_strike:
                u_barrier = round(u_strike * barrier_pct / 100, 2)
            underlyings.append({
                'name': name, 'strike': u_strike or 0,
                'barrier': u_barrier or 0, 'worst_of': is_worst
            })

    if not underlyings and wo_name:
        underlyings.append({
            'name': wo_name, 'strike': wo_strike or 0,
            'barrier': raw.get('barr_capitale') or 0, 'worst_of': True
        })

    # Market
    market = raw.get('mercato', 'CERT-X')
    market_map = {'CX': 'CERT-X', 'SX': 'SeDeX'}
    market = market_map.get(market, market)
    if detail and detail.get('mercato'):
        market = detail['mercato']

    divisa = raw.get('divisa', 'EUR')
    if detail and detail.get('divisa'):
        divisa = detail['divisa']

    buffer_from_barrier = round(100 - barrier_pct, 2) if barrier_pct else 0
    scenario = generate_scenarios(barrier_pct, ask_price, annual_yield, years_to_mat, wo_strike or 0, wo_name)

    return {
        'isin': raw['isin'],
        'name': raw.get('nome', ''),
        'type': cert_type,
        'issuer': raw.get('emittente', ''),
        'market': market,
        'currency': divisa,
        'ask_price': ask_price,
        'nominal': detail.get('nominal', 1000) if detail else 1000,
        'issue_date': issue_date or maturity_date,
        'maturity_date': maturity_date,
        'strike_date': detail.get('strike_date') if detail else None,
        'final_valuation_date': detail.get('final_valuation_date') or maturity_date if detail else maturity_date,
        'barrier_down': barrier_pct,
        'barrier_type': detail.get('barrier_type', 'European') if detail else 'European',
        'coupon': premio or 0,
        'coupon_frequency': freq_name,
        'has_memory': raw.get('has_memory', False) or ('memory' in raw.get('nome', '').lower()),
        'annual_coupon_yield': annual_yield,
        'underlyings': underlyings,
        'buffer_from_barrier': buffer_from_barrier,
        'scenario_analysis': scenario,
        'source': 'CED',
    }


def cert_needs_enrichment(cert: Dict) -> bool:
    underlyings = cert.get('underlyings', [])
    if not underlyings:
        return True
    all_strikes_zero = all(u.get('strike', 0) == 0 for u in underlyings)
    return all_strikes_zero


# ================================================================
# MAIN
# ================================================================

async def main():
    print("=" * 60)
    print("CED Scraper v17 - MULTI-SOURCE")
    print(f"Sources: BG Promo + CED Advanced Search ({len(TARGET_SOTTOSTANTI)} indices)")
    print(f"Max detail enrichment: {MAX_DETAIL_ISIN}")
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

        # === 1. Scrape BG promo (existing source) ===
        bg_raw = await scrape_bg_promo(page)

        # === 1b. Filter BG by underlying type ===
        bg_results, bg_skipped = filter_bg_by_underlying(bg_raw)
        print(f"  After filter: {len(bg_results)} valid, {bg_skipped} stocks skipped")

        # === 2. Scrape CED advanced search (new source) ===
        ced_results = await scrape_ced_search(page)

        # === 3. Merge results (BG wins on conflicts since it has more data) ===
        all_raw = {}
        for isin, data in ced_results.items():
            all_raw[isin] = data
        for isin, data in bg_results.items():
            all_raw[isin] = data  # BG overwrites CED since it has premio/ask/freq

        print(f"\n=== MERGED: {len(all_raw)} unique certificates ===")
        print(f"  From BG promo: {len(bg_results)}")
        print(f"  From CED search: {len(ced_results)}")
        print(f"  Overlap: {len(set(bg_results.keys()) & set(ced_results.keys()))}")

        if not all_raw:
            print("No certificates found! Keeping existing data.")
            await browser.close()
            return

        # === 4. Filter expired certificates ===
        filtered = {}
        expired = 0
        for isin, raw in all_raw.items():
            scadenza = parse_date(raw.get('scadenza', ''))
            if scadenza:
                try:
                    mat_dt = datetime.strptime(scadenza, '%Y-%m-%d')
                    if mat_dt < datetime.now():
                        expired += 1
                        continue
                except:
                    pass
            filtered[isin] = raw

        if expired:
            print(f"  Filtered out {expired} expired certificates")

        # === 5. Load existing data ===
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

        # === 6. Determine which need detail enrichment ===
        isins_to_enrich = []
        for isin, raw in filtered.items():
            if isin not in existing_certs:
                isins_to_enrich.append(isin)
            elif cert_needs_enrichment(existing_certs[isin]):
                isins_to_enrich.append(isin)
            elif raw.get('source') == 'CED_SEARCH' and not raw.get('premio'):
                # CED search results lack premio - enrich if not already complete
                if not existing_certs[isin].get('coupon'):
                    isins_to_enrich.append(isin)

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
                    'ask_price': cert.get('ask_price'),
                    'premio': cert.get('coupon'),
                    'frequenza': cert.get('coupon_frequency'),
                    'divisa': cert.get('currency', 'EUR'),
                    'mercato': cert.get('market', 'CERT-X'),
                    'underlyings_detail': [
                        {'name': u.get('name', ''), 'strike': u.get('strike', 0),
                         'barrier': u.get('barrier', 0)}
                        for u in cert['underlyings']
                    ]
                }

        detail_count = min(len(isins_to_enrich), MAX_DETAIL_ISIN)
        details = {}

        if isins_to_enrich:
            new_count = sum(1 for i in isins_to_enrich if i not in existing_certs)
            re_count = len(isins_to_enrich) - new_count
            print(f"\n=== Detail enrichment: {detail_count} ISINs ({new_count} new, {re_count} re-enriching) ===\n")

            for i, isin in enumerate(isins_to_enrich[:MAX_DETAIL_ISIN], 1):
                print(f"[{i}/{detail_count}] {isin}...", end=" ", flush=True)
                try:
                    detail = await scrape_detail(page, isin, debug=(i <= 3))
                    details[isin] = detail
                    nu = len(detail.get('underlyings_detail', []))
                    has_strikes = any(u.get('strike', 0) > 0 for u in detail.get('underlyings_detail', []))
                    print(f"OK und:{nu} strikes:{'yes' if has_strikes else 'no'} ask:{detail.get('ask_price')}")
                except Exception as e:
                    print(f"ERR {str(e)[:40]}")
                    details[isin] = {}
                await asyncio.sleep(REQUEST_DELAY)
        else:
            print(f"\nAll certificates have complete data, skipping enrichment")

        await browser.close()

        # === 7. Build certificates ===
        new_count = 0
        updated_count = 0

        for isin, raw in filtered.items():
            detail = details.get(isin) or saved_details.get(isin) or {}
            cert = build_certificate(raw, detail)
            if isin in existing_certs:
                updated_count += 1
            else:
                new_count += 1
            existing_certs[isin] = cert

        # === 8. Remove expired from existing ===
        max_age = datetime.now() - timedelta(days=730)
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

        # === 8b. Filter ALL certificates by valid underlyings ===
        # This cleans up stock-only certs from previous unfiltered runs
        cleaned_certs = {}
        stocks_removed = 0
        for isin, cert in final_certs.items():
            underlyings = cert.get('underlyings', [])
            cert_name = cert.get('name', '')
            und_names = [u.get('name', '') for u in underlyings]
            # Check if any underlying or the cert name matches valid keywords
            full_text = f"{cert_name} {' '.join(und_names)}".lower()
            has_valid = any(is_valid_underlying(n) for n in und_names)
            if not has_valid:
                has_valid = any(kw in full_text for kw in VALID_KEYWORDS)
            if has_valid or not underlyings:
                # Keep: has valid underlying OR no underlyings yet (will be enriched)
                cleaned_certs[isin] = cert
            else:
                stocks_removed += 1

        if stocks_removed:
            print(f"  Cleaned {stocks_removed} stock-only certificates from database")

        certificates = list(cleaned_certs.values())
        certificates.sort(key=lambda c: c.get('annual_coupon_yield', 0), reverse=True)

        # === 9. Save ===
        output = {
            'certificates': certificates,
            'metadata': {
                'version': 'v17-multisource',
                'source': 'certificatiederivati.it',
                'criteria': 'BG Promo + CED Search (indices, commodities, rates)',
                'max_history_years': 2,
                'new_this_run': new_count,
                'updated_this_run': updated_count,
                'purged_expired': purged,
                'stocks_cleaned': stocks_removed,
                'total_from_bg': len(bg_results),
                'total_from_ced_search': len(ced_results),
                'total_merged': len(filtered),
                'detail_enriched_this_run': len(details),
                'timestamp': datetime.now().isoformat(),
                'scrape_date': datetime.now().strftime('%Y-%m-%d'),
                'total_certificates': len(certificates),
            }
        }

        with open('certificates-data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        print(f"\n{'=' * 60}")
        print(f"RESULTS:")
        print(f"  BG Promo: {len(bg_results)} certificates")
        print(f"  CED Search: {len(ced_results)} certificates")
        print(f"  New: {new_count}")
        print(f"  Updated: {updated_count}")
        print(f"  Purged: {purged}")
        print(f"  Stocks cleaned: {stocks_removed}")
        print(f"  TOTAL: {len(certificates)} certificates saved")
        print(f"{'=' * 60}")


if __name__ == '__main__':
    asyncio.run(main())
