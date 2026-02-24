#!/usr/bin/env python3
"""
CED Scraper v15 - COMPLETO
Scrapes certificates from CED "Tabella Prodotti Banca Generali"
which has a rich 14-column table with all needed data.

Strategy:
1. Scrape the CED Banca Generali table (14 columns, ~120+ certificates)
2. Filter: only indices, commodities, forex, rates, credit (NO single stocks)
3. For filtered certs, visit detail page for enrichment (barrier type, issue date)
4. Output in EXACT format the frontend expects
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

# The ONE verified source page with 14-column table
SOURCE_URL = 'https://www.certificatiederivati.it/bs_promo_bgenerali.asp?t=redazione'
SOURCE_NAME = 'CED Banca Generali'

# ============ FILTRI ============
VALID_KEYWORDS = [
    # Indici
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
    # Credit
    'credit linked', 'credit link', 'cln', 'cds',
    # Generic
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


def is_valid_underlying(name: str) -> bool:
    if not name:
        return False
    n = name.lower().strip()
    return any(kw in n for kw in VALID_KEYWORDS)


def has_only_stocks(underlyings: List[str]) -> bool:
    if not underlyings:
        return False
    all_text = ' '.join(underlyings).lower()
    if any(kw in all_text for kw in VALID_KEYWORDS):
        return False
    return any(stock in all_text for stock in STOCK_KEYWORDS)


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


def generate_scenarios(barrier_pct, ask_price, annual_yield, years_to_mat, wo_strike):
    if not all([barrier_pct, ask_price, years_to_mat, wo_strike]):
        return None
    if ask_price <= 0 or years_to_mat <= 0:
        return None
    nominal = 1000.0
    scenarios = []
    for var in [-70, -60, -50, -40, -30, -20, -10, 0, 10, 20, 30]:
        u_price = round(wo_strike * (1 + var / 100), 2)
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
            'redemption': redemption,
            'pl_pct': pl_pct,
            'pl_annual': pl_annual
        })
    return {'scenarios': scenarios, 'years_to_maturity': round(years_to_mat, 2)}


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
# Columns from the HTML source:
# 0:isin  1:nome  2:emittente  3:scadenza  4:sottostante/basket
# 5:worst_of(strike)  6:ask  7:prossima_rilevazione  8:premio%
# 9:frequenza  10:barriera_premio  11:barriera_capitale  12:divisa  13:mercato

def parse_table_row(cols) -> Optional[Dict]:
    if len(cols) < 14:
        return None

    for col in cols:
        for br in col.find_all('br'):
            br.replace_with('\n')

    col_texts = [col.get_text(strip=True) for col in cols]

    # ISIN dal link <a href="db_bs_scheda_certificato.asp?isin=XXX">
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
        # Verify this is the right table by checking header
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
# STEP 2: Scrape detail page for enrichment
# ================================================================

async def scrape_detail(page, isin: str) -> Dict:
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    extra = {'barrier_type': 'European', 'issue_date': None, 'underlyings_detail': []}

    if not await retry_goto(page, url):
        return extra

    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')

    # Tipo barriera (DISCRETA = European, CONTINUA = American)
    page_text = soup.get_text().lower()
    if 'continua' in page_text:
        extra['barrier_type'] = 'American'
    elif 'discreta' in page_text:
        extra['barrier_type'] = 'European'

    # Data emissione dalla tabella dati
    for row in soup.find_all('tr'):
        cells = row.find_all(['th', 'td'])
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)
            if 'data emissione' in label:
                extra['issue_date'] = parse_date(value)
                break

    # Sottostanti con strike dalla sezione "Scheda Sottostante"
    for heading in soup.find_all(['h4', 'h3', 'strong', 'b']):
        if 'sottostante' in heading.get_text().lower():
            table = heading.find_next('table')
            if table:
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        name = cells[0].get_text(strip=True)
                        strike_text = cells[1].get_text(strip=True)
                        if name and name.upper() not in ['DESCRIZIONE', 'SOTTOSTANTE', '']:
                            strike = parse_number(strike_text)
                            extra['underlyings_detail'].append({
                                'name': name, 'strike': strike or 0
                            })
            break

    return extra


# ================================================================
# STEP 3: Build output in frontend format
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

    underlyings = []
    if detail_und:
        for u in detail_und:
            is_worst = wo_name and wo_name.lower().strip() == u['name'].lower().strip()
            u_barrier = round(u['strike'] * barrier_pct / 100, 2) if barrier_pct and u.get('strike') else 0
            underlyings.append({
                'name': u['name'], 'strike': u.get('strike', 0), 'spot': 0,
                'barrier': u_barrier, 'variation_pct': 0, 'variation_abs': 0,
                'trigger_coupon': u.get('strike', 0), 'trigger_autocall': u.get('strike', 0),
                'worst_of': is_worst
            })
    else:
        for name in sotto_names:
            is_worst = wo_name and wo_name.lower().strip() == name.lower().strip()
            u_strike = wo_strike if is_worst else 0
            u_barrier = raw.get('barr_capitale') if is_worst else 0
            underlyings.append({
                'name': name, 'strike': u_strike or 0, 'spot': 0,
                'barrier': u_barrier or 0, 'variation_pct': 0, 'variation_abs': 0,
                'trigger_coupon': u_strike or 0, 'trigger_autocall': u_strike or 0,
                'worst_of': is_worst
            })

    if not underlyings and wo_name:
        underlyings.append({
            'name': wo_name, 'strike': wo_strike or 0, 'spot': 0,
            'barrier': raw.get('barr_capitale') or 0,
            'variation_pct': 0, 'variation_abs': 0,
            'trigger_coupon': wo_strike or 0, 'trigger_autocall': wo_strike or 0,
            'worst_of': True
        })

    market_map = {'CX': 'CERT-X', 'SX': 'SeDeX'}
    market = raw.get('mercato', 'CERT-X')
    market = market_map.get(market, market)

    buffer_from_barrier = round(100 - barrier_pct, 2) if barrier_pct else 0

    scenario = generate_scenarios(barrier_pct, ask_price, annual_yield, years_to_mat, wo_strike or 0)

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
        'issue_date': issue_date or maturity_date,
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
        'buffer_from_trigger': 0,
        'effective_annual_yield': annual_yield,
        'scenario_analysis': scenario,
        'source': 'CED'
    }


# ================================================================
# MAIN
# ================================================================

async def main():
    print("=" * 60)
    print("CED Scraper v15 - COMPLETO")
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
            print("No certificates found in table!")
            await browser.close()
            output = {
                'success': False, 'count': 0, 'certificates': [],
                'metadata': {'error': 'No certificates found', 'timestamp': datetime.now().isoformat()}
            }
            with open('certificates-data.json', 'w') as f:
                json.dump(output, f, indent=2)
            pd.DataFrame().to_json('certificates-recenti.json', orient='records')
            pd.DataFrame().to_csv('certificates-recenti.csv', index=False)
            return

        # Deduplica per ISIN
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
            print("No valid certificates after filtering!")
            await browser.close()
            output = {
                'success': False, 'count': 0, 'certificates': [],
                'metadata': {'error': 'No valid certificates after filtering', 'timestamp': datetime.now().isoformat()}
            }
            with open('certificates-data.json', 'w') as f:
                json.dump(output, f, indent=2)
            pd.DataFrame().to_json('certificates-recenti.json', orient='records')
            pd.DataFrame().to_csv('certificates-recenti.csv', index=False)
            return

        # === 3. Visita pagine dettaglio ===
        detail_count = min(len(filtered), MAX_CERTIFICATES)
        details = {}

        print(f"\nVisiting {detail_count} detail pages...\n")

        for i, (isin, raw) in enumerate(list(filtered.items())[:MAX_CERTIFICATES], 1):
            print(f"[{i}/{detail_count}] {isin}...", end=" ", flush=True)
            try:
                detail = await scrape_detail(page, isin)
                details[isin] = detail
                bt = detail.get('barrier_type', '?')
                nu = len(detail.get('underlyings_detail', []))
                print(f"OK barrier:{bt} und:{nu}")
            except Exception as e:
                print(f"ERR {str(e)[:40]}")
                details[isin] = {}
            await asyncio.sleep(REQUEST_DELAY)

        await browser.close()

        # === 4. Costruisci output finale ===
        certificates = []
        for isin, raw in filtered.items():
            detail = details.get(isin, {})
            cert = build_certificate(raw, detail)
            certificates.append(cert)

        certificates.sort(key=lambda c: c.get('annual_coupon_yield', 0), reverse=True)

        # === 5. Salva files ===
        output = {
            'success': True,
            'count': len(certificates),
            'certificates': certificates,
            'metadata': {
                'version': 'v15',
                'source': 'certificatiederivati.it',
                'criteria': 'Indici, Commodities, Valute, Tassi, Credit',
                'recent_days': RECENT_DAYS,
                'total_scraped': len(by_isin),
                'after_filter': len(filtered),
                'detail_enriched': len(details),
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
        print("COMPLETED")
        print(f"  Valid certificates: {len(certificates)}")
        print(f"  Skipped (stocks): {skipped}")
        print(f"  Detail enriched: {len(details)}")
        print(f"  Saved: certificates-data.json, certificates-recenti.json, certificates-recenti.csv")
        print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
