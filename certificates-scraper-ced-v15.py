#!/usr/bin/env python3
"""
CED Scraper v15 - COMPLETO
Scrapes certificates from CED (certificatiederivati.it) sponsor pages
which have rich 14-column tables with all the data the frontend needs.

Strategy:
1. Scrape multiple CED "tabelle prodotti" pages (rich tables)
2. Parse all 14 columns per certificate
3. Filter: only indices, commodities, forex, rates, credit (NO single stocks)
4. For filtered certs, visit detail page for enrichment (barrier type, issue date, etc.)
5. Output in EXACT format the frontend expects
"""

import asyncio
import json
import os
import re
import math
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# ============ CONFIG ============
MAX_DETAIL_PAGES = int(os.getenv('MAX_DETAIL_ISIN', '80'))
REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', '1.5'))
PAGE_TIMEOUT = 45000  # 45 seconds
RETRY_COUNT = 2

# Pages to scrape (all have rich 14-column tables)
SCRAPE_PAGES = [
    {
        'url': 'https://www.certificatiederivati.it/bs_promo_bgenerali.asp?t=redazione',
        'name': 'Banca Generali'
    },
    {
        'url': 'https://www.certificatiederivati.it/bs_promo_imi.asp?t=redazione',
        'name': 'Intesa Sanpaolo'
    },
    {
        'url': 'https://www.certificatiederivati.it/landingPageBNPNew.asp',
        'name': 'BNP Paribas'
    },
    {
        'url': 'https://www.certificatiederivati.it/bs_promo_ugc.asp?t=turbo',
        'name': 'UniCredit'
    },
    {
        'url': 'https://www.certificatiederivati.it/landingPageAkros.asp',
        'name': 'Banco BPM'
    },
]

# ============ FILTERS ============
# Keywords that identify INDEX/COMMODITY/FOREX/RATE/CREDIT underlyings
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
    # Rates
    'euribor', 'libor', 'bund', 'btp', 'treasury', 'swap', 'yield', 'tasso',
    'estron',  # ESTRON = Euro Short-Term Rate ON (tasso)
    # Credit
    'credit linked', 'credit', 'cln', 'cds',
    # Generic index
    'index', 'indice', 'basket di indici', 'paniere',
]

# Known single-stock names to EXCLUDE
STOCK_NAMES = [
    'enel', 'eni', 'intesa sanpaolo', 'unicredit', 'generali', 'ferrari',
    'stellantis', 'stmicroelectronics', 'telecom italia', 'tim', 'leonardo',
    'pirelli', 'moncler', 'campari', 'a2a', 'snam', 'terna', 'poste italiane',
    'mediobanca', 'banco bpm', 'bper banca', 'banca mps', 'saipem',
    'amplifon', 'brunello cucinelli', 'diasorin', 'recordati', 'italgas',
    'nexi', 'prysmian', 'tenaris', 'hera', 'buzzi',
    # EU stocks
    'tesla', 'apple', 'amazon', 'nvidia', 'microsoft', 'alphabet', 'google',
    'meta', 'netflix', 'amd', 'intel', 'adobe', 'oracle', 'salesforce',
    'paypal', 'qualcomm', 'uber', 'airbnb', 'snap', 'spotify',
    'lvmh', 'asml', 'sap', 'siemens', 'allianz', 'basf', 'bayer',
    'adidas', 'bmw', 'daimler', 'mercedes', 'volkswagen', 'continental',
    'axa', 'bnp paribas', 'societe generale', 'credit agricole', 'total',
    'engie', 'danone', 'sanofi', "l'oreal", 'kering', 'hermes',
    'shell', 'unilever', 'vodafone', 'barclays', 'hsbc', 'bp',
    'british petroleum', 'rio tinto', 'anglo american', 'glencore',
    'nestlé', 'nestle', 'novartis', 'roche', 'zurich', 'ubs', 'credit suisse',
    'bbva', 'banco santander', 'iberdrola', 'telefonica',
    'novo nordisk', 'maersk', 'ap moeller', 'aegon', 'renault',
    'arcelor mittal', 'nike', 'kraft heinz', 'coca cola', 'commerzbank',
    'deutsche bank', 'morgan stanley', 'citigroup', 'goldman sachs',
    'worldline', 'diageo', 'moncler', 'kering',
]


def is_index_or_commodity(underlying_text: str) -> bool:
    """Check if the underlying is an index, commodity, forex, rate, or credit."""
    if not underlying_text:
        return False
    text = underlying_text.lower().strip()
    return any(kw in text for kw in VALID_KEYWORDS)


def classify_underlyings(sottostante_text: str) -> Tuple[bool, List[str]]:
    """
    Analyze the sottostante/basket column.
    Returns (should_include, list_of_underlying_names).
    Include if AT LEAST ONE underlying is an index/commodity/rate/etc.
    """
    if not sottostante_text:
        return False, []

    # Split by semicolon (CED uses ; as separator in basket)
    names = [n.strip() for n in sottostante_text.split(';') if n.strip()]

    if not names:
        return False, []

    # Check if any underlying matches our valid keywords
    has_valid = any(is_index_or_commodity(n) for n in names)

    return has_valid, names


def parse_italian_number(text: str) -> Optional[float]:
    """Parse Italian-formatted number (1.234,56 -> 1234.56)"""
    if not text or text.strip() in ['', '0', 'N.A.', 'N.D.', '-', 'N/A']:
        return None
    try:
        cleaned = re.sub(r'[€%\s\xa0]', '', text.strip())
        # Remove trailing * (memory indicator)
        cleaned = cleaned.rstrip('*')
        if not cleaned or cleaned == '0':
            return None
        # Handle Italian number format
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        val = float(cleaned)
        return round(val, 6) if val else None
    except (ValueError, TypeError):
        return None


def parse_italian_date(text: str) -> Optional[str]:
    """Parse DD/MM/YYYY -> YYYY-MM-DD"""
    if not text or text.strip() in ['', 'N.A.', '01/01/1900']:
        return None
    try:
        text = text.strip()
        if '/' in text:
            parts = text.split('/')
            if len(parts) == 3:
                d, m, y = parts
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except:
        pass
    return text


def parse_worst_of_column(text: str) -> Tuple[str, Optional[float]]:
    """
    Parse the 'worst of (strike)' column.
    Format: 'Name<br>(strike_value)' or just 'Name\n(strike_value)'
    Returns (name, strike_value)
    """
    if not text:
        return '', None

    # Try to extract name and strike from patterns like "Axa\n(41,39)"
    # or "Axa (41,39)" or just "Axa"
    match = re.match(r'^(.+?)\s*\(([0-9.,]+)\)\s*$', text.strip(), re.DOTALL)
    if match:
        name = match.group(1).strip()
        strike_str = match.group(2)
        strike = parse_italian_number(strike_str)
        return name, strike

    # Try multiline format
    lines = text.strip().split('\n')
    if len(lines) >= 2:
        name = lines[0].strip()
        strike_match = re.search(r'\(([0-9.,]+)\)', lines[-1])
        if strike_match:
            strike = parse_italian_number(strike_match.group(1))
            return name, strike

    return text.strip(), None


def get_frequency_multiplier(freq_text: str) -> Tuple[float, str]:
    """Return (annual_multiplier, frequency_name) from Italian frequency text."""
    freq = freq_text.lower().strip() if freq_text else ''
    if 'mensil' in freq or 'monthly' in freq:
        return 12.0, 'monthly'
    elif 'trimestral' in freq or 'quarterly' in freq:
        return 4.0, 'quarterly'
    elif 'semestral' in freq or 'semiannual' in freq:
        return 2.0, 'semiannual'
    elif 'annual' in freq or 'annuale' in freq:
        return 1.0, 'annual'
    else:
        return 12.0, 'monthly'  # default to monthly (most common)


def calculate_barrier_pct(barrier_value: Optional[float], strike_value: Optional[float]) -> Optional[float]:
    """Calculate barrier as percentage of strike."""
    if barrier_value and strike_value and strike_value > 0:
        pct = (barrier_value / strike_value) * 100
        if 5 <= pct <= 100:  # sanity check
            return round(pct, 2)
    return None


def generate_scenario_analysis(barrier_pct: float, ask_price: float,
                                coupon_annual: float, years_to_maturity: float,
                                worst_of_strike: float) -> Optional[Dict]:
    """Generate basic scenario analysis for the certificate."""
    if not all([barrier_pct, ask_price, years_to_maturity, worst_of_strike]):
        return None
    if ask_price <= 0 or years_to_maturity <= 0:
        return None

    nominal = 1000.0
    scenarios = []

    for var_pct in [-70, -60, -50, -40, -30, -20, -10, 0, 10, 20, 30]:
        underlying_price = round(worst_of_strike * (1 + var_pct / 100), 2)
        barrier_level = worst_of_strike * (barrier_pct / 100)

        if var_pct >= -(100 - barrier_pct):
            # Above barrier: get back nominal + coupons
            total_coupons = coupon_annual * years_to_maturity / 100 * nominal
            redemption = round(nominal + total_coupons, 2)
        else:
            # Below barrier: lose proportionally
            redemption = round(nominal * (1 + var_pct / 100), 2)

        pl_pct = round((redemption - ask_price) / ask_price * 100, 2)
        pl_annual = round(pl_pct / years_to_maturity, 2) if years_to_maturity > 0 else 0

        scenarios.append({
            'variation_pct': var_pct,
            'underlying_price': underlying_price,
            'redemption': redemption,
            'pl_pct': pl_pct,
            'pl_annual': pl_annual
        })

    return {
        'scenarios': scenarios,
        'years_to_maturity': round(years_to_maturity, 2)
    }


async def retry_goto(page, url: str, retries: int = RETRY_COUNT) -> bool:
    """Navigate with retry."""
    for attempt in range(retries):
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
            await page.wait_for_timeout(1500)
            return True
        except Exception as e:
            print(f"  ⚠️  Attempt {attempt+1}/{retries} failed: {str(e)[:50]}")
            if attempt < retries - 1:
                await asyncio.sleep(2)
    return False


def parse_table_row(cols: list) -> Optional[Dict]:
    """
    Parse a single table row from the CED 14-column table.
    Columns: isin, nome, emittente, scadenza, sottostante/basket,
             worst_of(strike), ask, prossima_rilevazione, premio%,
             frequenza, barriera_premio, barriera_capitale, divisa, mercato
    """
    if len(cols) < 12:
        return None

    col_texts = []
    for col in cols:
        # Get text, handling <br> tags by replacing with newline
        for br in col.find_all('br'):
            br.replace_with('\n')
        col_texts.append(col.get_text(strip=True))

    # Extract ISIN from first column (may be inside <a> tag)
    isin = ''
    a_tag = cols[0].find('a')
    if a_tag:
        href = a_tag.get('href', '')
        isin_match = re.search(r'isin=([A-Z0-9]+)', href)
        if isin_match:
            isin = isin_match.group(1)
        else:
            isin = a_tag.get_text(strip=True)
    else:
        isin = col_texts[0]

    # Validate ISIN format
    if not re.match(r'^[A-Z]{2}[A-Z0-9]{9,11}$', isin):
        return None

    # Parse all columns
    nome = col_texts[1] if len(col_texts) > 1 else ''
    emittente = col_texts[2] if len(col_texts) > 2 else ''
    scadenza = col_texts[3] if len(col_texts) > 3 else ''
    sottostante = col_texts[4] if len(col_texts) > 4 else ''

    # Worst of column - get raw text with <br> preserved
    worst_of_text = col_texts[5] if len(col_texts) > 5 else ''
    worst_of_name, worst_of_strike = parse_worst_of_column(worst_of_text)

    ask_text = col_texts[6] if len(col_texts) > 6 else ''
    premio_text = col_texts[8] if len(col_texts) > 8 else ''
    frequenza = col_texts[9] if len(col_texts) > 9 else ''
    barriera_premio_text = col_texts[10] if len(col_texts) > 10 else ''
    barriera_capitale_text = col_texts[11] if len(col_texts) > 11 else ''
    divisa = col_texts[12] if len(col_texts) > 12 else 'EUR'
    mercato = col_texts[13] if len(col_texts) > 13 else 'CERT-X'

    return {
        'isin': isin,
        'nome': nome,
        'emittente': emittente,
        'scadenza': scadenza,
        'sottostante': sottostante,
        'worst_of_name': worst_of_name,
        'worst_of_strike': worst_of_strike,
        'ask': parse_italian_number(ask_text),
        'premio': parse_italian_number(premio_text),
        'frequenza': frequenza,
        'has_memory': '*' in premio_text if premio_text else False,
        'barriera_premio': parse_italian_number(barriera_premio_text),
        'barriera_capitale': parse_italian_number(barriera_capitale_text),
        'divisa': divisa.strip(),
        'mercato': mercato.strip(),
    }


async def scrape_list_page(page, url: str, name: str) -> List[Dict]:
    """Scrape a CED page with a rich certificate table."""
    print(f"\n📋 Scraping: {name} ({url})")

    if not await retry_goto(page, url):
        print(f"  ❌ Failed to load {name}")
        return []

    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')

    results = []

    # Find all tables with certificate data
    for table in soup.find_all('table', class_=re.compile(r'table')):
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 12:
                continue

            parsed = parse_table_row(cols)
            if parsed:
                results.append(parsed)

    print(f"  ✅ Found {len(results)} certificates from {name}")
    return results


async def scrape_detail_page(page, isin: str) -> Dict:
    """Scrape the certificate detail page for additional data."""
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"

    extra = {
        'barrier_type': 'European',
        'issue_date': None,
        'full_underlyings': [],
    }

    if not await retry_goto(page, url):
        return extra

    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')

    # === BARRIER TYPE ===
    # Look for "Barriera Down" section with Tipo (DISCRETA/CONTINUA)
    page_text = soup.get_text().lower()
    if 'continua' in page_text:
        extra['barrier_type'] = 'American'
    elif 'discreta' in page_text:
        extra['barrier_type'] = 'European'

    # === ISSUE DATE ===
    for row in soup.find_all('tr'):
        cells = row.find_all(['th', 'td'])
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)
            if 'data emissione' in label:
                extra['issue_date'] = parse_italian_date(value)
                break

    # Also try from bold/strong pairs
    for tag in soup.find_all(['strong', 'b']):
        text = tag.get_text(strip=True).lower()
        if 'data emissione' in text:
            next_text = tag.next_sibling
            if next_text:
                val = str(next_text).strip()
                if '/' in val:
                    extra['issue_date'] = parse_italian_date(val)

    # === FULL UNDERLYINGS with strikes ===
    # Look for "Scheda Sottostante" section
    for heading in soup.find_all(['h4', 'h3', 'strong', 'b']):
        if 'sottostante' in heading.get_text().lower():
            # Find the next table after this heading
            table = heading.find_next('table')
            if table:
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        name = cells[0].get_text(strip=True)
                        strike_text = cells[1].get_text(strip=True)
                        if name and name.upper() not in ['DESCRIZIONE', 'SOTTOSTANTE', '']:
                            strike = parse_italian_number(strike_text)
                            extra['full_underlyings'].append({
                                'name': name,
                                'strike': strike or 0
                            })
            break

    return extra


def build_frontend_certificate(raw: Dict, detail: Optional[Dict] = None) -> Dict:
    """
    Transform raw scraped data into the EXACT format the frontend expects.
    """
    # Parse frequency
    freq_mult, freq_name = get_frequency_multiplier(raw.get('frequenza', ''))

    # Calculate annual coupon yield
    premio = raw.get('premio') or 0
    annual_yield = round(premio * freq_mult, 2)

    # Calculate barrier percentage
    barrier_value = raw.get('barriera_capitale')
    strike_value = raw.get('worst_of_strike')
    barrier_pct = calculate_barrier_pct(barrier_value, strike_value)

    # If barrier_pct couldn't be calculated from capitale, try premio barrier
    if barrier_pct is None:
        barrier_premio = raw.get('barriera_premio')
        barrier_pct = calculate_barrier_pct(barrier_premio, strike_value)

    # Default to 0 if still None
    if barrier_pct is None:
        barrier_pct = 0

    # Parse dates
    maturity_date = parse_italian_date(raw.get('scadenza', ''))
    issue_date = detail.get('issue_date') if detail else None

    # Calculate years to maturity
    years_to_maturity = 3.0  # default
    if maturity_date:
        try:
            mat_dt = datetime.strptime(maturity_date, '%Y-%m-%d')
            years_to_maturity = max(0.1, (mat_dt - datetime.now()).days / 365.25)
        except:
            pass

    # Ask price
    ask_price = raw.get('ask') or 1000.0

    # Build underlyings array
    sottostante_names = [n.strip() for n in raw.get('sottostante', '').split(';') if n.strip()]
    worst_of_name = raw.get('worst_of_name', '')

    # Use detail page underlyings if available
    detail_underlyings = detail.get('full_underlyings', []) if detail else []

    underlyings = []
    if detail_underlyings:
        for u in detail_underlyings:
            is_worst = worst_of_name.lower().strip() == u['name'].lower().strip() if worst_of_name else False
            u_barrier = 0
            if barrier_pct and u.get('strike'):
                u_barrier = round(u['strike'] * barrier_pct / 100, 2)
            underlyings.append({
                'name': u['name'],
                'strike': u.get('strike', 0),
                'spot': 0,
                'barrier': u_barrier,
                'variation_pct': 0,
                'variation_abs': 0,
                'trigger_coupon': u.get('strike', 0),
                'trigger_autocall': u.get('strike', 0),
                'worst_of': is_worst
            })
    else:
        # Build from list data
        for name in sottostante_names:
            is_worst = worst_of_name.lower().strip() == name.lower().strip() if worst_of_name else False
            u_strike = strike_value if is_worst else 0
            u_barrier = barrier_value if is_worst else 0
            underlyings.append({
                'name': name,
                'strike': u_strike or 0,
                'spot': 0,
                'barrier': u_barrier or 0,
                'variation_pct': 0,
                'variation_abs': 0,
                'trigger_coupon': u_strike or 0,
                'trigger_autocall': u_strike or 0,
                'worst_of': is_worst
            })

    # Ensure at least one underlying
    if not underlyings and worst_of_name:
        underlyings.append({
            'name': worst_of_name,
            'strike': strike_value or 0,
            'spot': 0,
            'barrier': barrier_value or 0,
            'variation_pct': 0,
            'variation_abs': 0,
            'trigger_coupon': strike_value or 0,
            'trigger_autocall': strike_value or 0,
            'worst_of': True
        })

    # Market code mapping
    market_map = {'CX': 'CERT-X', 'SX': 'SeDeX'}
    market = raw.get('mercato', 'CERT-X')
    market = market_map.get(market, market)

    # Buffer from barrier (how far worst-of is from barrier, as %)
    buffer_from_barrier = round(100 - barrier_pct, 2) if barrier_pct else 0
    buffer_from_trigger = 0  # would need spot data to calculate

    # Scenario analysis
    scenario = generate_scenario_analysis(
        barrier_pct, ask_price, annual_yield, years_to_maturity, strike_value or 0
    )

    return {
        'isin': raw['isin'],
        'name': raw.get('nome', ''),
        'type': raw.get('nome', 'Certificato'),
        'issuer': raw.get('emittente', ''),
        'market': market,
        'currency': raw.get('divisa', 'EUR'),
        'bid_price': 0,
        'ask_price': ask_price,
        'reference_price': ask_price,
        'issue_date': issue_date or maturity_date,  # fallback
        'maturity_date': maturity_date,
        'strike_date': None,
        'final_valuation_date': maturity_date,
        'barrier_down': barrier_pct,
        'barrier_type': detail.get('barrier_type', 'European') if detail else 'European',
        'coupon': premio,
        'coupon_frequency': freq_name,
        'has_memory': raw.get('has_memory', False),
        'annual_coupon_yield': annual_yield,
        'underlyings': underlyings,
        'buffer_from_barrier': buffer_from_barrier,
        'buffer_from_trigger': buffer_from_trigger,
        'effective_annual_yield': annual_yield,
        'scenario_analysis': scenario,
        'source': 'CED'
    }


async def main():
    print("=" * 65)
    print("🏦 CED Scraper v15 - COMPLETO")
    print(f"   Filtri: Indici, Commodities, Valute, Tassi, Credit")
    print(f"   Max detail pages: {MAX_DETAIL_PAGES}")
    print(f"   Delay: {REQUEST_DELAY}s")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu']
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            locale='it-IT',
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        # =============================================
        # STEP 1: Scrape all list pages
        # =============================================
        all_raw = {}  # keyed by ISIN to deduplicate

        for source in SCRAPE_PAGES:
            try:
                rows = await scrape_list_page(page, source['url'], source['name'])
                for row in rows:
                    isin = row['isin']
                    if isin not in all_raw:
                        all_raw[isin] = row
                await asyncio.sleep(REQUEST_DELAY)
            except Exception as e:
                print(f"  ❌ Error scraping {source['name']}: {str(e)[:60]}")

        print(f"\n📊 Total unique certificates found: {len(all_raw)}")

        # =============================================
        # STEP 2: Filter for valid underlyings
        # =============================================
        filtered = {}
        skipped_stocks = 0
        skipped_no_data = 0

        for isin, raw in all_raw.items():
            sottostante = raw.get('sottostante', '')
            nome = raw.get('nome', '')
            full_text = f"{sottostante} {nome}".lower()

            # Check if any underlying matches our criteria
            is_valid, names = classify_underlyings(sottostante)

            if is_valid:
                filtered[isin] = raw
            elif sottostante:
                # Double-check in name/type
                if any(kw in full_text for kw in VALID_KEYWORDS):
                    filtered[isin] = raw
                else:
                    skipped_stocks += 1
            else:
                skipped_no_data += 1

        print(f"✅ After filter: {len(filtered)} certificates")
        print(f"⏭️  Skipped (stocks only): {skipped_stocks}")
        print(f"⏭️  Skipped (no data): {skipped_no_data}")

        # =============================================
        # STEP 3: Visit detail pages for enrichment
        # =============================================
        detail_count = min(len(filtered), MAX_DETAIL_PAGES)
        details = {}

        if detail_count > 0:
            print(f"\n🔍 Visiting {detail_count} detail pages for enrichment...\n")

            for i, (isin, raw) in enumerate(list(filtered.items())[:MAX_DETAIL_PAGES], 1):
                print(f"  [{i}/{detail_count}] {isin}...", end=" ", flush=True)
                try:
                    detail = await scrape_detail_page(page, isin)
                    details[isin] = detail
                    print(f"✅ barrier:{detail.get('barrier_type','?')} underlyings:{len(detail.get('full_underlyings',[]))}")
                except Exception as e:
                    print(f"❌ {str(e)[:40]}")
                    details[isin] = {}

                await asyncio.sleep(REQUEST_DELAY)

        await browser.close()

        # =============================================
        # STEP 4: Build final output
        # =============================================
        print(f"\n🔨 Building final output...")

        certificates = []
        for isin, raw in filtered.items():
            detail = details.get(isin, {})
            cert = build_frontend_certificate(raw, detail)
            certificates.append(cert)

        # Sort by annual yield descending
        certificates.sort(key=lambda c: c.get('annual_coupon_yield', 0), reverse=True)

        # =============================================
        # STEP 5: Save output files
        # =============================================
        output = {
            'success': True,
            'count': len(certificates),
            'certificates': certificates,
            'metadata': {
                'scraper_version': 'v15',
                'timestamp': datetime.now().isoformat(),
                'source': 'certificatiederivati.it',
                'criteria': 'Indici, Commodities, Valute, Tassi, Credit (NO single stocks)',
                'total_scraped': len(all_raw),
                'after_filter': len(filtered),
                'detail_pages_visited': len(details),
                'scrape_date': datetime.now().strftime('%Y-%m-%d'),
                'categories': len(set(c['type'] for c in certificates)),
                'underlyings_db': len(set(
                    u['name'] for c in certificates for u in c['underlyings']
                )),
                'issuers': sorted(list(set(c['issuer'] for c in certificates if c['issuer']))),
            }
        }

        # Main JSON output
        with open('certificates-data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        # CSV and JSON for workflow compatibility
        df = pd.DataFrame(certificates)
        df.to_json('certificates-recenti.json', orient='records', indent=2, force_ascii=False)
        df.to_csv('certificates-recenti.csv', index=False)

        # =============================================
        # SUMMARY
        # =============================================
        print("\n" + "=" * 65)
        print("✅ COMPLETED SUCCESSFULLY")
        print(f"   📊 Total scraped:      {len(all_raw)}")
        print(f"   🎯 After filter:       {len(filtered)}")
        print(f"   🔍 Detail enriched:    {len(details)}")
        print(f"   💾 Final certificates: {len(certificates)}")
        print(f"   📁 Files: certificates-data.json, certificates-recenti.json, certificates-recenti.csv")

        if certificates:
            yields = [c['annual_coupon_yield'] for c in certificates if c['annual_coupon_yield'] > 0]
            barriers = [c['barrier_down'] for c in certificates if c['barrier_down'] > 0]
            print(f"\n   📈 Yield range: {min(yields):.2f}% - {max(yields):.2f}%" if yields else "")
            print(f"   🛡️  Barrier range: {min(barriers):.0f}% - {max(barriers):.0f}%" if barriers else "")

            issuers = set(c['issuer'] for c in certificates if c['issuer'])
            print(f"   🏦 Issuers ({len(issuers)}): {', '.join(sorted(issuers)[:8])}...")

        print("=" * 65)


if __name__ == '__main__':
    asyncio.run(main())
