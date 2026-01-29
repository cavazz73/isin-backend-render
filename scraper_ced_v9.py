#!/usr/bin/env python3
"""
CED Scraper v9 - Estrazione dati completa
Fix: parsing robusto dei dettagli certificato
"""

import asyncio
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

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


class CEDScraperV9:
    def __init__(self):
        self.certificates = {}
        self.stats = {'found': 0, 'detailed': 0, 'errors': 0, 'by_underlying': {}}

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def parse_date(self, s):
        """Parse date DD/MM/YYYY -> YYYY-MM-DD"""
        if not s or s.strip() in ['-', 'N/A', '', 'n.d.']:
            return None
        try:
            # Prova DD/MM/YYYY
            match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', s)
            if match:
                d, m, y = match.groups()
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
            # Prova YYYY-MM-DD
            match = re.search(r'(\d{4})-(\d{2})-(\d{2})', s)
            if match:
                return match.group(0)
        except:
            pass
        return None

    def parse_num(self, s):
        """Parse number with Italian format"""
        if not s or s.strip() in ['-', 'N/A', '', 'n.d.']:
            return None
        try:
            # Rimuovi tutto tranne numeri, virgola, punto, segno
            c = re.sub(r'[^\d,.\-]', '', str(s))
            if not c:
                return None
            # Formato italiano: 1.234,56 -> 1234.56
            if ',' in c:
                c = c.replace('.', '').replace(',', '.')
            v = float(c)
            return v if v != 0 else None
        except:
            return None

    def parse_percent(self, s):
        """Parse percentage"""
        if not s:
            return None
        num = self.parse_num(s)
        # Se il valore originale aveva %, il numero è già la percentuale
        # Se > 1 e non aveva %, potrebbe essere già percentuale
        return num

    async def search_underlying(self, page, value, label):
        """Cerca certificati per sottostante"""
        self.log(f"Ricerca: {label}")
        
        try:
            await page.goto('https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp', 
                           wait_until='networkidle', timeout=60000)
            await asyncio.sleep(2)
            
            await page.select_option('select#sottostante', value=value)
            await asyncio.sleep(1)
            
            await page.click('input[value="Avvia Ricerca"]')
            await page.wait_for_load_state('networkidle', timeout=60000)
            await asyncio.sleep(2)
            
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            found = 0
            
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

    async def get_detail(self, page, isin, base_data):
        """Estrae dettagli scheda - VERSIONE MIGLIORATA"""
        url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
        
        try:
            await page.goto(url, wait_until='networkidle', timeout=45000)
            await asyncio.sleep(3)  # Attendi caricamento JS
            
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Struttura compatibile con frontend v11
            detail = {
                'isin': isin,
                'name': base_data.get('name', ''),
                'type': '',
                'issuer': '',
                'market': '',
                'currency': 'EUR',
                'underlying_name': '',
                'underlying_category': base_data.get('underlying_category', ''),
                'underlyings': [],
                'issue_date': None,
                'maturity_date': None,
                'strike_date': None,
                'barrier': None,
                'barrier_down': None,
                'barrier_type': None,
                'coupon': None,
                'coupon_monthly': None,
                'trigger_coupon': None,
                'trigger_autocall': None,
                'annual_coupon_yield': None,
                'effective_annual_yield': None,
                'price': None,
                'last_price': None,
                'emission_price': 100,
                'bid_price': None,
                'ask_price': None,
                'nominal': 1000,
                'buffer_from_barrier': None,
                'autocallable': False,
                'memory_effect': False,
                'scenario_analysis': None,
                'scraped': True,
                'timestamp': datetime.now().isoformat()
            }
            
            # === TIPO CERTIFICATO ===
            # Cerca in h2, h3, o span con classe specifica
            for tag in soup.find_all(['h2', 'h3', 'h4']):
                txt = tag.get_text(strip=True)
                if txt and len(txt) > 2 and len(txt) < 100:
                    if any(kw in txt.lower() for kw in ['cash collect', 'phoenix', 'bonus', 'athena', 'autocall', 'express', 'certificate', 'benchmark', 'capitale']):
                        detail['type'] = txt
                        break
            
            # === ESTRAI DA TUTTE LE TABELLE ===
            all_text = soup.get_text(' ', strip=True).lower()
            
            # Trova tutte le coppie label-value nelle tabelle
            for table in soup.find_all('table'):
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    
                    # Prova coppie di celle adiacenti
                    for i in range(len(cells) - 1):
                        label = cells[i].get_text(strip=True).lower()
                        value = cells[i + 1].get_text(strip=True)
                        
                        if not label or not value:
                            continue
                        
                        # Emittente
                        if 'emittente' in label:
                            detail['issuer'] = value
                        
                        # Mercato
                        elif 'mercato' in label:
                            detail['market'] = value
                        
                        # Date
                        elif 'data emissione' in label or 'emissione' in label and 'data' in label:
                            detail['issue_date'] = self.parse_date(value)
                        elif 'data scadenza' in label or 'scadenza' in label:
                            detail['maturity_date'] = self.parse_date(value)
                        elif 'data strike' in label or 'strike' in label and 'data' in label:
                            detail['strike_date'] = self.parse_date(value)
                        elif 'valutazione finale' in label:
                            if not detail['maturity_date']:
                                detail['maturity_date'] = self.parse_date(value)
                        
                        # Barriera
                        elif 'barriera' in label and 'tipo' not in label:
                            val = self.parse_percent(value)
                            if val:
                                detail['barrier'] = val
                                detail['barrier_down'] = val
                        elif 'tipo barriera' in label:
                            detail['barrier_type'] = value
                        
                        # Cedola/Premio
                        elif 'cedola' in label or 'premio' in label:
                            val = self.parse_percent(value)
                            if val:
                                detail['coupon'] = val
                                detail['coupon_monthly'] = val
                        
                        # Rendimento
                        elif 'rendimento' in label and 'annuo' in label:
                            val = self.parse_percent(value)
                            if val:
                                detail['annual_coupon_yield'] = val
                                detail['effective_annual_yield'] = val
                        elif 'rendimento' in label:
                            val = self.parse_percent(value)
                            if val and not detail['annual_coupon_yield']:
                                detail['annual_coupon_yield'] = val
                        
                        # Trigger
                        elif 'trigger' in label and 'cedola' in label:
                            detail['trigger_coupon'] = self.parse_percent(value)
                        elif 'trigger' in label and 'autocall' in label:
                            detail['trigger_autocall'] = self.parse_percent(value)
                        
                        # Prezzi
                        elif label in ['bid', 'denaro'] or 'prezzo denaro' in label:
                            detail['bid_price'] = self.parse_num(value)
                        elif label in ['ask', 'lettera'] or 'prezzo lettera' in label:
                            detail['ask_price'] = self.parse_num(value)
                        elif 'ultimo' in label or 'last' in label:
                            detail['last_price'] = self.parse_num(value)
                        elif 'prezzo' in label and 'emissione' in label:
                            detail['emission_price'] = self.parse_num(value) or 100
            
            # === ESTRAI SOTTOSTANTI ===
            for table in soup.find_all('table'):
                headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
                
                # Cerca tabella sottostanti
                if any(h in ['sottostante', 'descrizione', 'underlying'] for h in headers):
                    data_rows = table.find_all('tr')[1:]  # Skip header
                    
                    for row in data_rows:
                        cells = row.find_all('td')
                        if not cells:
                            continue
                        
                        und = {
                            'name': cells[0].get_text(strip=True),
                            'strike': None,
                            'spot': None,
                            'barrier': None,
                            'worst_of': False
                        }
                        
                        # Mappa headers ai valori
                        for idx, header in enumerate(headers):
                            if idx < len(cells):
                                cell_val = cells[idx].get_text(strip=True)
                                if 'strike' in header:
                                    und['strike'] = self.parse_num(cell_val)
                                elif 'spot' in header or 'ultimo' in header or 'last' in header:
                                    und['spot'] = self.parse_num(cell_val)
                                elif 'barriera' in header:
                                    und['barrier'] = self.parse_num(cell_val)
                        
                        if und['name']:
                            detail['underlyings'].append(und)
                    
                    if detail['underlyings']:
                        detail['underlying_name'] = detail['underlyings'][0]['name']
                    break
            
            # Fallback: usa underlying_category
            if not detail['underlyings']:
                detail['underlyings'] = [{
                    'name': base_data.get('underlying_category', 'N/A'),
                    'worst_of': False
                }]
                detail['underlying_name'] = base_data.get('underlying_category', '')
            
            # === AUTOCALLABLE / MEMORY ===
            if 'autocall' in all_text or 'rimborso anticipato' in all_text:
                detail['autocallable'] = True
            if 'memory' in all_text or 'effetto memoria' in all_text:
                detail['memory_effect'] = True
            
            # === PREZZO ===
            if detail['bid_price'] and detail['ask_price']:
                detail['price'] = (detail['bid_price'] + detail['ask_price']) / 2
            elif detail['last_price']:
                detail['price'] = detail['last_price']
            
            # === BUFFER FROM BARRIER ===
            if detail['barrier_down'] and detail['barrier_down'] > 0:
                detail['buffer_from_barrier'] = 100 - detail['barrier_down']
            
            self.stats['detailed'] += 1
            return detail
            
        except Exception as e:
            self.log(f"Errore {isin}: {str(e)[:50]}")
            self.stats['errors'] += 1
            return {
                **base_data,
                'type': 'Unknown',
                'scraped': False,
                'timestamp': datetime.now().isoformat()
            }

    async def run(self):
        self.log("=" * 60)
        self.log("CED SCRAPER V9 - PRODUCTION")
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
                        await asyncio.sleep(1.5)
                    
                    self.certificates = {r['isin']: r for r in results}
                
            finally:
                await browser.close()
        
        # Statistiche finali
        certs_list = list(self.certificates.values())
        with_barrier = sum(1 for c in certs_list if c.get('barrier_down'))
        with_coupon = sum(1 for c in certs_list if c.get('coupon'))
        with_issuer = sum(1 for c in certs_list if c.get('issuer'))
        
        return {
            'metadata': {
                'version': '9.0',
                'source': 'certificatiederivati.it',
                'timestamp': datetime.now().isoformat(),
                'total_certificates': len(self.certificates),
                'with_barrier': with_barrier,
                'with_coupon': with_coupon,
                'with_issuer': with_issuer,
                'stats': self.stats
            },
            'certificates': certs_list
        }


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='certificates-data.json')
    args = parser.parse_args()
    
    scraper = CEDScraperV9()
    results = await scraper.run()
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETATO")
    print(f"{'='*60}")
    print(f"Certificati: {len(results['certificates'])}")
    print(f"Con barriera: {results['metadata']['with_barrier']}")
    print(f"Con cedola: {results['metadata']['with_coupon']}")
    print(f"Con emittente: {results['metadata']['with_issuer']}")

if __name__ == '__main__':
    asyncio.run(main())
