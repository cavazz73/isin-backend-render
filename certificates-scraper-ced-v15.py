#!/usr/bin/env python3
"""
CED Scraper v14 - FIXED & COMPLETE
Fixes:
  - barrier → barrier_down (allineato al frontend)
  - underlyings come oggetti con strike/spot/barrier/variation_pct
  - scenario_analysis calcolato correttamente
  - regex barriera e cedola robuste (solo %)
  - last_price da CED → reference_price
  - buffer_from_barrier e buffer_from_trigger calcolati
  - coupon mensile + annual_coupon_yield entrambi presenti
"""

import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# ============ CONFIG ============
RECENT_DAYS      = int(os.getenv('RECENT_DAYS', '60'))
MAX_CERTIFICATES = int(os.getenv('MAX_DETAIL_ISIN', '100'))
REQUEST_DELAY    = 2.0
PAGE_TIMEOUT     = 60000
RETRY_COUNT      = 3

cutoff_date = datetime.now() - timedelta(days=RECENT_DAYS)

# ============ FILTRI SOTTOSTANTI ============
VALID_KEYWORDS = [
    'ftse', 'mib', 'stoxx', 'eurostoxx', 'dax', 'cac', 'ibex',
    's&p', 'sp500', 'nasdaq', 'dow', 'nikkei', 'hang seng', 'russell', 'msci',
    'oro', 'gold', 'silver', 'argento', 'petrolio', 'oil', 'brent', 'wti',
    'gas', 'copper', 'rame', 'platinum', 'palladium',
    'eur/usd', 'usd/jpy', 'forex', 'currency', 'valuta', 'cambio',
    'euribor', 'libor', 'bund', 'btp', 'treasury', 'swap', 'yield', 'tasso',
    'credit', 'cln', 'cds',
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
    if not name:
        return False
    n = name.lower()
    return any(kw in n for kw in VALID_KEYWORDS)


def has_only_stocks(underlyings: List[str]) -> bool:
    if not underlyings:
        return False
    all_text = ' '.join(underlyings).lower()
    if any(kw in all_text for kw in VALID_KEYWORDS):
        return False
    return any(stock in all_text for stock in STOCK_KEYWORDS)


# ============ PARSING UTILS ============

def parse_number(text: str) -> Optional[float]:
    """Parse numero in formato italiano o internazionale."""
    if not text:
        return None
    text = text.strip()
    if text.upper() in ['N.A.', 'N.D.', '-', '', 'N/A', '--']:
        return None
    try:
        cleaned = re.sub(r'[EUR€$£\s\xa0]', '', text)
        # Formato italiano 1.234,56
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        # Solo virgola 1234,56
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        val = float(cleaned)
        return round(val, 4) if val != 0 else None
    except:
        return None


def parse_percentage(text: str) -> Optional[float]:
    """
    Estrae percentuale SOLO da testo che contiene '%'.
    Non cattura date, ID o altri numeri generici.
    """
    if not text or '%' not in text:
        return None
    match = re.search(r'(\d{1,3}(?:[.,]\d+)?)\s*%', text)
    if not match:
        return None
    try:
        val = float(match.group(1).replace(',', '.'))
        return val if val > 0 else None
    except:
        return None


def parse_date(text: str) -> Optional[str]:
    """Parse data italiana DD/MM/YYYY → YYYY-MM-DD."""
    if not text:
        return None
    text = text.strip()
    if text in ['', 'N.A.', '01/01/1900', '01/01/1970']:
        return None
    try:
        if '/' in text:
            parts = text.split('/')
            if len(parts) == 3 and len(parts[2]) == 4:
                return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
    except:
        pass
    return None


def years_until(date_str: Optional[str]) -> float:
    """Anni rimanenti fino a una data YYYY-MM-DD."""
    if not date_str:
        return 3.0
    try:
        maturity = datetime.strptime(date_str, '%Y-%m-%d')
        delta    = maturity - datetime.now()
        years    = delta.days / 365.25
        return max(0.1, round(years, 2))
    except:
        return 3.0


# ============ CALCOLO SCENARIO ANALYSIS ============

def calculate_scenarios(
    cert_type: str,
    worst_of: Dict,
    reference_price: float,
    annual_coupon_yield: float,
    years: float
) -> List[Dict]:
    """
    Calcola scenari a scadenza sul worst-of underlying.
    P&L include cedole accumulate + rimborso capitale.
    """
    strike  = worst_of.get('strike', 0)
    barrier = worst_of.get('barrier', 0)

    if strike <= 0:
        return []

    # Cedole totali su base 1000 nominale
    total_coupons   = (annual_coupon_yield / 100) * 1000 * years
    cert_type_low   = cert_type.lower()
    variations      = [-70, -60, -50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70]
    scenarios       = []

    for var_pct in variations:
        price_at_maturity = strike * (1 + var_pct / 100)
        barrier_breached  = (barrier > 0) and (price_at_maturity < barrier)

        if barrier_breached:
            # Perdita proporzionale
            redemption = round(max(0, (price_at_maturity / strike) * 1000), 2)
        else:
            if 'twin win' in cert_type_low:
                # Guadagna sia al rialzo che al ribasso
                redemption = round(1000 * (1 + abs(var_pct) / 100), 2)
            elif 'bonus' in cert_type_low:
                # Rimborso pieno + bonus (semplificato: pieno)
                redemption = 1000.0
            else:
                # Phoenix, Cash Collect, Express, ecc.
                redemption = 1000.0

        # P&L = (rimborso + cedole totali - prezzo acquisto) / prezzo acquisto
        total_received = redemption + total_coupons
        pl_pct         = round((total_received - reference_price) / reference_price * 100, 2)
        pl_annual      = round(pl_pct / years, 2) if years > 0 else 0.0

        scenarios.append({
            "variation_pct":    var_pct,
            "underlying_price": round(price_at_maturity, 2),
            "barrier_breached": barrier_breached,
            "redemption":       redemption,
            "pl_pct":           pl_pct,
            "pl_annual":        pl_annual
        })

    return scenarios


# ============ NAVIGAZIONE ============

async def retry_goto(page, url: str, retries: int = RETRY_COUNT) -> bool:
    for attempt in range(retries):
        try:
            await page.goto(url, wait_until='networkidle', timeout=PAGE_TIMEOUT)
            await page.wait_for_timeout(2000)
            return True
        except Exception as e:
            print(f"  Attempt {attempt+1}/{retries} failed: {str(e)[:50]}")
            if attempt < retries - 1:
                await asyncio.sleep(3)
    return False


# ============ SCRAPE LISTING ============

async def scrape_listing(page) -> List[Dict]:
    print("Fetching listing from CED...")
    url = 'https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp'

    if not await retry_goto(page, url):
        print("Failed to load listing page")
        return []

    html  = await page.content()
    soup  = BeautifulSoup(html, 'html.parser')
    certs = []

    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            cols  = row.find_all(['td', 'th'])
            if len(cols) < 6:
                continue
            texts = [c.get_text(strip=True) for c in cols]
            isin  = texts[0]

            if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                continue

            try:
                date_str = texts[5] if len(texts) > 5 else ''
                if '/' in date_str:
                    emission_date = datetime.strptime(date_str, '%d/%m/%Y')
                    if emission_date < cutoff_date:
                        continue
            except:
                continue

            certs.append({
                'isin':           isin,
                'name':           texts[1] if len(texts) > 1 else '',
                'issuer':         texts[2] if len(texts) > 2 else '',
                'underlying_raw': texts[3] if len(texts) > 3 else '',
                'issue_date':     texts[5] if len(texts) > 5 else None,
            })

    print(f"Found {len(certs)} recent certificates")
    return certs[:MAX_CERTIFICATES * 2]


# ============ HELPER COLONNE ============

def _find_col(headers: List[str], keywords: List[str]) -> Optional[int]:
    for i, h in enumerate(headers):
        if any(kw in h for kw in keywords):
            return i
    return None


def _get_cell_number(cells, idx: Optional[int]) -> Optional[float]:
    if idx is None or idx >= len(cells):
        return None
    return parse_number(cells[idx].get_text(strip=True))


# ============ SCRAPE DETAIL ============

async def scrape_detail(page, cert: Dict) -> Optional[Dict]:
    isin = cert['isin']
    url  = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"

    if not await retry_goto(page, url):
        return None

    html     = await page.content()
    soup     = BeautifulSoup(html, 'html.parser')
    all_text = soup.get_text()

    result = {
        'isin':                 isin,
        'name':                 cert.get('name', ''),
        'type':                 'Certificato',
        'issuer':               cert.get('issuer', ''),
        'market':               'SeDeX',
        'currency':             'EUR',
        'reference_price':      None,
        'bid_price':            None,
        'ask_price':            None,
        'last_price':           None,
        'barrier_down':         None,   # % — campo corretto per il frontend
        'barrier_type':         'European',
        'coupon':               None,   # per periodo
        'coupon_frequency':     'monthly',
        'annual_coupon_yield':  None,
        'underlyings':          [],
        'buffer_from_barrier':  None,
        'buffer_from_trigger':  None,
        'effective_annual_yield': None,
        'issue_date':           parse_date(cert.get('issue_date')),
        'maturity_date':        None,
        'scenario_analysis':    None,
        'source':               'certificatiederivati.it',
        'scraped_at':           datetime.now().isoformat()
    }

    # ── 1. TIPO ────────────────────────────────────────────────────
    h3 = soup.find('h3', class_='panel-title')
    if h3:
        result['type'] = h3.get_text(strip=True)

    # ── 2. PREZZI CERTIFICATO ─────────────────────────────────────
    for panel in soup.find_all('div', class_='panel'):
        heading_el = panel.find(['h3', 'div'], class_=['panel-title', 'panel-heading'])
        if not heading_el:
            continue
        heading = heading_el.get_text(strip=True).lower()
        if not any(kw in heading for kw in ['prezzi', 'quotazioni', 'price', 'mercato']):
            continue

        for row in panel.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)
            num   = parse_number(value)
            if num is None:
                continue

            if any(k in label for k in ['bid', 'acquisto', 'denaro']):
                result['bid_price'] = num
            elif any(k in label for k in ['ask', 'lettera', 'vendita']):
                result['ask_price'] = num
            elif any(k in label for k in ['last', 'ultimo', 'ult.', 'chiusura', 'close']):
                result['last_price'] = num
                if result['reference_price'] is None:
                    result['reference_price'] = num
            elif any(k in label for k in ['riferimento', 'ref', 'ufficiale', 'officiale']):
                result['reference_price'] = num
        break

    # Fallback reference_price
    if result['reference_price'] is None:
        if result['bid_price'] and result['ask_price']:
            result['reference_price'] = round((result['bid_price'] + result['ask_price']) / 2, 4)
        elif result['last_price']:
            result['reference_price'] = result['last_price']
        else:
            result['reference_price'] = 1000.0

    # ── 3. SCADENZA ───────────────────────────────────────────────
    for row in soup.find_all('tr'):
        cells = row.find_all(['th', 'td'])
        if len(cells) < 2:
            continue
        label = cells[0].get_text(strip=True).lower()
        value = cells[1].get_text(strip=True)
        if any(kw in label for kw in ['scadenza', 'valutazione finale', 'maturity', 'rimborso finale']):
            d = parse_date(value)
            if d:
                result['maturity_date'] = d
                break

    # ── 4. SOTTOSTANTI con prezzi ─────────────────────────────────
    raw_underlying_names = []
    underlyings_objects  = []

    for panel in soup.find_all('div', class_='panel'):
        heading_el = panel.find(['h3', 'div'], class_=['panel-title', 'panel-heading'])
        if not heading_el:
            continue
        if 'sottostante' not in heading_el.get_text(strip=True).lower():
            continue

        table = panel.find('table')
        if not table:
            continue

        # Leggi header
        headers     = []
        header_row  = table.find('tr')
        if header_row:
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(['th', 'td'])]

        col_name    = _find_col(headers, ['descrizione', 'nome', 'sottostante', 'underlying', 'titolo'])
        col_strike  = _find_col(headers, ['strike', 'prezzo iniziale', 'iniziale', 'riferimento iniziale'])
        col_spot    = _find_col(headers, ['spot', 'prezzo corrente', 'corrente', 'ultimo', 'last', 'attuale'])
        col_barrier = _find_col(headers, ['barriera', 'barrier', 'livello barriera', 'livello'])
        col_var     = _find_col(headers, ['var', 'variazione', 'performance', 'perf'])

        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if not cells:
                continue

            name_idx = col_name if col_name is not None else 0
            name     = cells[name_idx].get_text(strip=True) if name_idx < len(cells) else ''
            if not name or name.upper() in ['DESCRIZIONE', 'SOTTOSTANTE', 'NOME', '']:
                continue

            raw_underlying_names.append(name)

            strike  = _get_cell_number(cells, col_strike)
            spot    = _get_cell_number(cells, col_spot)
            barrier = _get_cell_number(cells, col_barrier)
            var_pct = _get_cell_number(cells, col_var)

            # Calcola var% se mancante
            if var_pct is None and strike and spot and strike > 0:
                var_pct = round((spot - strike) / strike * 100, 2)

            # Normalizza barrier: se <= 100 è una %, altrimenti è prezzo assoluto
            if barrier is not None:
                if barrier <= 100 and strike and strike > 0:
                    barrier_price = round(strike * barrier / 100, 4)
                else:
                    barrier_price = barrier
            else:
                # Default: 50% dello strike
                barrier_price = round(strike * 0.50, 4) if strike else None

            underlyings_objects.append({
                'name':             name,
                'strike':           strike  or 0.0,
                'spot':             spot    or 0.0,
                'barrier':          barrier_price or 0.0,
                'variation_pct':    round(var_pct, 2) if var_pct is not None else 0.0,
                'variation_abs':    round(var_pct, 2) if var_pct is not None else 0.0,
                'trigger_coupon':   strike  or 0.0,
                'trigger_autocall': strike  or 0.0,
                'worst_of':         False
            })

        break

    # Fallback da listing
    if not underlyings_objects and cert.get('underlying_raw'):
        raw_underlying_names = [cert['underlying_raw']]
        underlyings_objects  = [{
            'name':             cert['underlying_raw'],
            'strike':           0.0, 'spot': 0.0, 'barrier': 0.0,
            'variation_pct':    0.0, 'variation_abs': 0.0,
            'trigger_coupon':   0.0, 'trigger_autocall': 0.0,
            'worst_of':         True
        }]

    # ── 5. FILTRO ────────────────────────────────────────────────
    if has_only_stocks(raw_underlying_names):
        return None
    if underlyings_objects and not any(is_valid_underlying(u['name']) for u in underlyings_objects):
        full_text = f"{result['name']} {cert.get('underlying_raw', '')}".lower()
        if not any(kw in full_text for kw in VALID_KEYWORDS):
            return None

    # Marca worst-of (peggiore variazione %)
    if underlyings_objects:
        worst_idx = min(range(len(underlyings_objects)),
                        key=lambda i: underlyings_objects[i]['variation_pct'])
        underlyings_objects[worst_idx]['worst_of'] = True

    result['underlyings'] = underlyings_objects

    # ── 6. BARRIER_DOWN % ────────────────────────────────────────
    # Strategia A: div#barriera (con %)
    # Strategia B: panel heading barriera (con %)
    # Strategia C: calcolo da worst-of (barrier_price / strike)
    barrier_down = None

    # A) div#barriera
    barriera_div = soup.find('div', id='barriera')
    if barriera_div:
        for cell in barriera_div.find_all(['td', 'span', 'div', 'p']):
            txt = cell.get_text(strip=True)
            val = parse_percentage(txt)
            if val and 10 <= val <= 95:
                barrier_down = val
                break
        # Fallback: numero solo (senza %)
        if barrier_down is None:
            for cell in barriera_div.find_all(['td', 'span', 'div']):
                txt = cell.get_text(strip=True)
                m   = re.match(r'^(\d{2,3})$', txt)
                if m:
                    val = float(m.group(1))
                    if 10 <= val <= 95:
                        barrier_down = val
                        break

    # B) Panel heading "barriera"
    if barrier_down is None:
        for panel in soup.find_all('div', class_='panel'):
            heading_el = panel.find(['h3', 'div'], class_=['panel-title', 'panel-heading'])
            if not heading_el or 'barriera' not in heading_el.get_text(strip=True).lower():
                continue
            for cell in panel.find_all('td'):
                txt = cell.get_text(strip=True)
                val = parse_percentage(txt)
                if val and 10 <= val <= 95:
                    barrier_down = val
                    break
                m = re.match(r'^(\d{2,3})$', txt)
                if m:
                    val = float(m.group(1))
                    if 10 <= val <= 95:
                        barrier_down = val
                        break
            if barrier_down:
                break

    # C) Calcola da worst-of
    if barrier_down is None and underlyings_objects:
        worst = next((u for u in underlyings_objects if u['worst_of']), underlyings_objects[0])
        s = worst.get('strike', 0)
        b = worst.get('barrier', 0)
        if s > 0 and b > 0:
            barrier_down = round(b / s * 100, 1)

    result['barrier_down'] = barrier_down

    # Tipo barriera
    for row in soup.find_all('tr'):
        cells = row.find_all(['th', 'td'])
        if len(cells) < 2:
            continue
        label = cells[0].get_text(strip=True).lower()
        if 'tipo' in label and 'barriera' in label:
            val = cells[1].get_text(strip=True)
            if val and len(val) < 50:
                result['barrier_type'] = val
            break

    # ── 7. CEDOLA ────────────────────────────────────────────────
    # SOLO valori con '%' per evitare numeri casuali
    coupon_val   = None
    coupon_freq  = 'monthly'
    annual_yield = None

    # A) div#rilevamento
    rilevamento_div = soup.find('div', id='rilevamento')
    if rilevamento_div:
        for row in rilevamento_div.find_all('tr'):
            cells    = row.find_all(['th', 'td'])
            row_text = ' '.join([c.get_text(strip=True).lower() for c in cells])
            for cell in cells:
                txt = cell.get_text(strip=True)
                val = parse_percentage(txt)
                if val and 0.01 <= val <= 30:
                    coupon_val = val
                    if any(k in row_text for k in ['mensil', 'monthly']):
                        coupon_freq  = 'monthly'
                        annual_yield = round(val * 12, 2)
                    elif any(k in row_text for k in ['trimestral', 'quarterly']):
                        coupon_freq  = 'quarterly'
                        annual_yield = round(val * 4, 2)
                    elif any(k in row_text for k in ['semestral', 'semiannual']):
                        coupon_freq  = 'semiannual'
                        annual_yield = round(val * 2, 2)
                    elif any(k in row_text for k in ['annual', 'annuo', 'annuale']):
                        coupon_freq  = 'annual'
                        annual_yield = val
                    else:
                        coupon_freq  = 'monthly'
                        annual_yield = round(val * 12, 2)
                    break
            if coupon_val:
                break

    # B) Panel cedola/premio
    if coupon_val is None:
        for panel in soup.find_all('div', class_='panel'):
            heading_el = panel.find(['h3', 'div'], class_=['panel-title', 'panel-heading'])
            if not heading_el:
                continue
            heading = heading_el.get_text(strip=True).lower()
            if not any(k in heading for k in ['cedola', 'premio', 'coupon', 'rilevamento']):
                continue
            for cell in panel.find_all('td'):
                txt = cell.get_text(strip=True)
                val = parse_percentage(txt)
                if val and 0.01 <= val <= 30:
                    coupon_val   = val
                    coupon_freq  = 'monthly'
                    annual_yield = round(val * 12, 2)
                    break
            if coupon_val:
                break

    # C) Fallback regex su testo intero
    if coupon_val is None:
        for pattern in [
            r'cedola[^%\d]*(\d+[.,]\d+)\s*%',
            r'premio[^%\d]*(\d+[.,]\d+)\s*%',
            r'coupon[^%\d]*(\d+[.,]\d+)\s*%',
        ]:
            m = re.search(pattern, all_text.lower())
            if m:
                val = float(m.group(1).replace(',', '.'))
                if 0.01 <= val <= 30:
                    coupon_val   = val
                    coupon_freq  = 'monthly'
                    annual_yield = round(val * 12, 2)
                    break

    result['coupon']              = coupon_val
    result['coupon_frequency']    = coupon_freq
    result['annual_coupon_yield'] = annual_yield

    # ── 8. BUFFER ────────────────────────────────────────────────
    if underlyings_objects:
        worst   = next((u for u in underlyings_objects if u['worst_of']), underlyings_objects[0])
        spot    = worst.get('spot',    0)
        barrier = worst.get('barrier', 0)
        strike  = worst.get('strike',  0)

        if spot > 0 and barrier > 0:
            result['buffer_from_barrier'] = round((spot - barrier) / spot * 100, 2)
        if spot > 0 and strike > 0:
            result['buffer_from_trigger'] = round((spot - strike) / spot * 100, 2)

    result['effective_annual_yield'] = annual_yield

    # ── 9. SCENARIO ANALYSIS ─────────────────────────────────────
    if underlyings_objects and annual_yield and result['reference_price']:
        worst  = next((u for u in underlyings_objects if u['worst_of']), underlyings_objects[0])
        years  = years_until(result['maturity_date'])

        scenarios = calculate_scenarios(
            cert_type           = result['type'],
            worst_of            = worst,
            reference_price     = result['reference_price'],
            annual_coupon_yield = annual_yield,
            years               = years
        )

        if scenarios:
            result['scenario_analysis'] = {
                'worst_underlying': worst.get('name', 'N/A'),
                'purchase_price':   result['reference_price'],
                'years_to_maturity': years,
                'scenarios':        scenarios
            }

    return result


# ============ MAIN ============

async def main():
    print("=" * 60)
    print("CED Scraper v14 - FIXED & COMPLETE")
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
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='it-IT',
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        listing = await scrape_listing(page)

        if not listing:
            print("No certificates found in listing!")
            await browser.close()
            _save_output([], 0, 0, error='Failed to fetch listing')
            return

        valid_certs = []
        skipped     = 0
        errors      = 0
        total       = min(len(listing), MAX_CERTIFICATES)

        print(f"\nProcessing {total} certificates...\n")

        for i, cert in enumerate(listing[:MAX_CERTIFICATES], 1):
            print(f"[{i}/{total}] {cert['isin']}...", end=" ", flush=True)
            try:
                result = await scrape_detail(page, cert)
                if result:
                    valid_certs.append(result)
                    bd = result.get('barrier_down')
                    cy = result.get('annual_coupon_yield')
                    sc = 'scenario:yes' if result.get('scenario_analysis') else 'scenario:NO'
                    print(f"OK  barrier:{bd}% coupon:{cy}% {sc}  {result.get('underlyings', [{}])[0].get('name','')[:20]}")
                else:
                    skipped += 1
                    print("SKIP (stocks/invalid)")
            except Exception as e:
                errors += 1
                print(f"ERR {str(e)[:60]}")

            await asyncio.sleep(REQUEST_DELAY)

        await browser.close()
        _save_output(valid_certs, skipped, errors)

        print("\n" + "=" * 60)
        print(f"  Valid:   {len(valid_certs)}")
        print(f"  Skipped: {skipped}")
        print(f"  Errors:  {errors}")
        print(f"  Saved:   certificates-data.json")
        print("=" * 60)


def _save_output(certs: List[Dict], skipped: int, errors: int, error: str = None):
    output = {
        'success':      not bool(error),
        'count':        len(certs),
        'certificates': certs,
        'metadata': {
            'version':        'v14-fixed',
            'source':         'certificatiederivati.it',
            'criteria':       'Indici, Commodities, Valute, Tassi, Credit',
            'recent_days':    RECENT_DAYS,
            'processed':      len(certs) + skipped + errors,
            'valid':          len(certs),
            'skipped_stocks': skipped,
            'errors':         errors,
            'timestamp':      datetime.now().isoformat(),
            **({"error": error} if error else {})
        }
    }
    with open('certificates-data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)


if __name__ == '__main__':
    asyncio.run(main())
