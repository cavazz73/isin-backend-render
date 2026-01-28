#!/usr/bin/env python3
"""
CED Scraper v8 - Click corretto su "Avvia Ricerca"
"""

import asyncio
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Sottostanti da cercare (value esatti dal dropdown)
TARGET_UNDERLYINGS = [
    ("FTSEMIB Index", "FTSE MIB"),
    ("SX5E Index", "Euro Stoxx 50"),
    ("DAX Index", "DAX"),
    ("SPX Index", "S&P 500"),
    ("NDX Index", "Nasdaq 100"),
    ("NKY Index", "Nikkei 225"),
    ("INDU Index", "Dow Jones"),
    ("CAC Index", "CAC40"),
    ("HSI Index", "Hang Seng"),
    ("GOLDS Comdty", "Gold"),
    ("XAG Curncy", "Silver"),
    ("CO1 comdty", "Brent"),
    ("NG1 Comdty", "Natural Gas"),
    ("EUR001M Index", "Euribor 1M"),
]


class CEDScraperV8:
    def __init__(self):
        self.certificates = {}
        self.stats = {'found': 0, 'detailed': 0, 'errors': 0, 'by_underlying': {}}

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def parse_date(self, s):
        if not s or s in ['-', 'N/A', '']:
            return None
        try:
            p = s.strip().split('/')
            if len(p) == 3:
                return f"{p[2]}-{p[1].zfill(2)}-{p[0].zfill(2)}"
        except:
            pass
        return None

    def parse_num(self, s):
        if not s or s in ['-', 'N/A', '']:
            return None
        try:
            c = re.sub(r'[^\d,.\-]', '', str(s))
            if ',' in c:
                c = c.replace('.', '').replace(',', '.')
            v = float(c)
            return v if v != 0 else None
        except:
            return None

    async def search_underlying(self, page, value, label):
        """Cerca certificati per sottostante"""
        self.log(f"Ricerca: {label}")
        
        try:
            # Vai a ricerca avanzata
            await page.goto('https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp', 
                           wait_until='networkidle', timeout=60000)
            await asyncio.sleep(2)
            
            # Seleziona sottostante
            await page.select_option('select#sottostante', value=value)
            await asyncio.sleep(1)
            
            # CLICK CORRETTO: bottone "Avvia Ricerca"
            await page.click('input[value="Avvia Ricerca"]')
            await page.wait_for_load_state('networkidle', timeout=60000)
            await asyncio.sleep(2)
            
            # Estrai certificati
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            found = 0
            
            # Cerca link alle schede certificato
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if 'scheda' in href.lower() and 'isin=' in href.lower():
                    match = re.search(r'isin=([A-Z0-9]{12})', href, re.I)
                    if match:
                        isin = match.group(1).upper()
                        if isin not in self.certificates:
                            name = link.get_text(strip=True)
                            if 'TURBO' in name.upper() or 'LEVA' in name.upper():
                                continue
                            self.certificates[isin] = {
                                'isin': isin,
                                'name': name,
                                'underlying_category': label
                            }
                            found += 1
            
            self.stats['by_underlying'][label] = found
            self.log(f"  Trovati: {found}")
            return found
            
        except Exception as e:
            self.log(f"  Errore: {str(e)[:60]}")
            return 0

    async def get_detail(self, page, isin, data):
        """Estrae dettagli scheda"""
        url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
        
        try:
            await page.goto(url, wait_until='networkidle', timeout=45000)
            await asyncio.sleep(2)
            
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            detail = {
                'isin': isin,
                'name': data.get('name', ''),
                'type': '',
                'issuer': '',
                'market': '',
                'currency': 'EUR',
                'underlying_category': data.get('underlying_category', ''),
                'issue_date': None,
                'maturity_date': None,
                'strike_date': None,
                'barrier_down': None,
                'barrier_type': None,
                'coupon': None,
                'annual_coupon_yield': None,
                'bid_price': None,
                'ask_price': None,
                'underlyings': []
            }
            
            # Tipo da h2/h3
            for h in soup.find_all(['h2', 'h3']):
                txt = h.get_text(strip=True)
                if txt and len(txt) > 3 and 'Scheda' not in txt and 'CED' not in txt:
                    detail['type'] = txt
                    break
            
            # Dati dalle tabelle
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        lbl = cells[0].get_text(strip=True).lower()
                        val = cells[1].get_text(strip=True)
                        
                        if 'emissione' in lbl and 'data' in lbl:
                            detail['issue_date'] = self.parse_date(val)
                        elif 'scadenza' in lbl:
                            detail['maturity_date'] = self.parse_date(val)
                        elif 'strike' in lbl and 'data' in lbl:
                            detail['strike_date'] = self.parse_date(val)
                        elif 'barriera' in lbl and '%' in val:
                            detail['barrier_down'] = self.parse_num(val)
                        elif 'tipo barriera' in lbl:
                            detail['barrier_type'] = val
                        elif 'cedola' in lbl:
                            detail['coupon'] = self.parse_num(val)
                        elif 'rendimento' in lbl:
                            detail['annual_coupon_yield'] = self.parse_num(val)
                        elif 'denaro' in lbl or lbl == 'bid':
                            detail['bid_price'] = self.parse_num(val)
                        elif 'lettera' in lbl or lbl == 'ask':
                            detail['ask_price'] = self.parse_num(val)
                        elif 'emittente' in lbl:
                            detail['issuer'] = val
                        elif 'mercato' in lbl:
                            detail['market'] = val
            
            # Sottostanti
            for table in soup.find_all('table'):
                headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
                if any('sottostante' in h or 'descrizione' in h for h in headers):
                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if cells:
                            und = {'name': cells[0].get_text(strip=True), 'strike': None, 'spot': None, 'barrier': None}
                            for i, h in enumerate(headers):
                                if i < len(cells):
                                    v = cells[i].get_text(strip=True)
                                    if 'strike' in h:
                                        und['strike'] = self.parse_num(v)
                                    elif 'spot' in h or 'ultimo' in h:
                                        und['spot'] = self.parse_num(v)
                                    elif 'barriera' in h:
                                        und['barrier'] = self.parse_num(v)
                            if und['name']:
                                detail['underlyings'].append(und)
            
            if not detail['underlyings']:
                detail['underlyings'] = [{'name': detail['underlying_category']}]
            
            self.stats['detailed'] += 1
            return detail
            
        except Exception as e:
            self.log(f"Errore {isin}: {str(e)[:40]}")
            self.stats['errors'] += 1
            return {**data, 'type': 'Unknown'}

    async def run(self):
        self.log("=" * 60)
        self.log("CED SCRAPER V8")
        self.log("=" * 60)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
            ctx = await browser.new_context(viewport={'width': 1920, 'height': 1080})
            page = await ctx.new_page()
            
            try:
                # Fase 1: Ricerca
                self.log("\n📋 FASE 1: Ricerca per sottostante")
                for value, label in TARGET_UNDERLYINGS:
                    await self.search_underlying(page, value, label)
                    await asyncio.sleep(2)
                
                self.stats['found'] = len(self.certificates)
                self.log(f"\n✅ Certificati unici: {len(self.certificates)}")
                
                # Fase 2: Dettagli
                if self.certificates:
                    self.log(f"\n📊 FASE 2: Dettagli")
                    results = []
                    items = list(self.certificates.items())
                    for i, (isin, data) in enumerate(items):
                        self.log(f"[{i+1}/{len(items)}] {isin}")
                        detail = await self.get_detail(page, isin, data)
                        results.append(detail)
                        await asyncio.sleep(1)
                    
                    self.certificates = {r['isin']: r for r in results}
                
            finally:
                await browser.close()
        
        return {
            'metadata': {
                'version': '8.0',
                'source': 'certificatiederivati.it',
                'timestamp': datetime.now().isoformat(),
                'total_certificates': len(self.certificates),
                'stats': self.stats
            },
            'certificates': list(self.certificates.values())
        }


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='certificates-data.json')
    args = parser.parse_args()
    
    scraper = CEDScraperV8()
    results = await scraper.run()
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETATO")
    print(f"{'='*60}")
    print(f"Certificati: {len(results['certificates'])}")
    print(f"Stats: {results['metadata']['stats']}")

if __name__ == '__main__':
    asyncio.run(main())
