#!/usr/bin/env python3
"""
CertificatiEDerivati.it Scraper - PRODUCTION
Usa la Ricerca Avanzata con filtri server-side per sottostanti specifici.
"""

import asyncio
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Sottostanti da cercare (filtro server-side)
TARGET_UNDERLYINGS = [
    # Indici
    "FTSE Mib", "Euro Stoxx 50", "Eurostoxx 50", "DAX", "S&P 500",
    "Nasdaq 100", "NASDAQ 100", "Dow Jones", "CAC40", "IBEX 35",
    "Nikkei", "Hang Seng", "MSCI", "Russell",
    "Basket di indici", "Basket di indici worst of", "Basket di Indici Equipesato",
    # Commodities
    "Gold", "Oro", "Argento", "Silver", "Petrolio", "Oil", "WTI", "Brent",
    "Rame", "Copper", "Gas", "Natural Gas", "Platino", "Palladio",
    "Basket commodity", "Basket metalli",
    # Tassi/Credit
    "Euribor", "Credit Linked", "Basket obbligazioni", "BTP", "Bund",
    # Valute
    "EUR/USD", "EUR/GBP", "EUR/JPY", "USD/JPY"
]

class CEDFinalScraper:
    def __init__(self):
        self.certificates = []
        self.stats = {'found': 0, 'detailed': 0, 'errors': 0, 'by_type': {}}
        
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
            return float(c) if float(c) != 0 else None
        except:
            return None

    async def search_by_underlying(self, page, underlying):
        """Cerca certificati per un sottostante specifico"""
        self.log(f"Ricerca: {underlying}")
        
        url = "https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp"
        await page.goto(url, wait_until='networkidle', timeout=60000)
        await asyncio.sleep(2)
        
        # Seleziona il sottostante nel dropdown
        try:
            # Trova e seleziona dal dropdown sottostante
            await page.select_option('select[name="sottostante"]', label=underlying)
            await asyncio.sleep(1)
            
            # Click su Cerca
            await page.click('input[type="submit"], button[type="submit"]')
            await page.wait_for_load_state('networkidle', timeout=30000)
            await asyncio.sleep(2)
            
        except Exception as e:
            self.log(f"  Sottostante non trovato nel dropdown: {underlying}")
            return []
        
        # Estrai risultati
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        results = []
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cols = row.find_all('td')
                if len(cols) >= 5:
                    isin = cols[0].get_text(strip=True)
                    if len(isin) == 12 and isin.isalnum():
                        # Escludi Turbo e Leva
                        nome = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                        if 'TURBO' in nome.upper() or 'LEVA' in nome.upper():
                            continue
                            
                        results.append({
                            'isin': isin,
                            'name': nome,
                            'issuer': cols[2].get_text(strip=True) if len(cols) > 2 else "",
                            'underlying': underlying,
                            'market': cols[4].get_text(strip=True) if len(cols) > 4 else ""
                        })
        
        self.log(f"  Trovati: {len(results)}")
        return results

    async def get_detail(self, page, cert):
        """Estrae dettagli completi dalla scheda certificato"""
        isin = cert['isin']
        url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
        
        try:
            await page.goto(url, wait_until='networkidle', timeout=45000)
            await asyncio.sleep(3)
            
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            data = {
                'isin': isin,
                'name': cert['name'],
                'type': '',
                'issuer': cert['issuer'],
                'market': cert.get('market', ''),
                'currency': 'EUR',
                'underlying_category': cert['underlying'],
                'bid_price': None,
                'ask_price': None,
                'issue_date': None,
                'maturity_date': None,
                'strike_date': None,
                'barrier_down': None,
                'barrier_type': 'European',
                'coupon': None,
                'annual_coupon_yield': None,
                'underlyings': []
            }
            
            # Tipo certificato
            for h in soup.find_all(['h2', 'h3']):
                txt = h.get_text(strip=True)
                if txt and len(txt) > 3 and 'Sottoscrivi' not in txt and 'Scheda' not in txt and 'CED' not in txt:
                    data['type'] = txt
                    break
            
            # Dati dalle tabelle
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        lbl = cells[0].get_text(strip=True).lower()
                        val = cells[1].get_text(strip=True)
                        
                        if 'data emissione' in lbl:
                            data['issue_date'] = self.parse_date(val)
                        elif 'data scadenza' in lbl:
                            data['maturity_date'] = self.parse_date(val)
                        elif 'data strike' in lbl:
                            data['strike_date'] = self.parse_date(val)
                        elif 'valutazione finale' in lbl and not data['maturity_date']:
                            data['maturity_date'] = self.parse_date(val)
                        elif 'barriera' in lbl and '%' in val:
                            data['barrier_down'] = self.parse_num(val)
                        elif 'cedola' in lbl or 'premio' in lbl:
                            data['coupon'] = self.parse_num(val)
                        elif 'rendimento' in lbl and 'annuo' in lbl:
                            data['annual_coupon_yield'] = self.parse_num(val)
                        elif 'nominale' in lbl:
                            data['nominal'] = self.parse_num(val) or 1000
            
            # Sottostanti
            for table in soup.find_all('table'):
                hdrs = [th.get_text(strip=True).lower() for th in table.find_all('th')]
                if any(h in ['descrizione', 'sottostante'] for h in hdrs):
                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if cells:
                            und = {'name': cells[0].get_text(strip=True), 'strike': None, 'spot': None, 'barrier': None}
                            for i, h in enumerate(hdrs):
                                if i < len(cells):
                                    v = cells[i].get_text(strip=True)
                                    if 'strike' in h:
                                        und['strike'] = self.parse_num(v)
                                    elif 'spot' in h or 'ultimo' in h:
                                        und['spot'] = self.parse_num(v)
                                    elif 'barriera' in h:
                                        und['barrier'] = self.parse_num(v)
                            if und['name']:
                                data['underlyings'].append(und)
            
            if not data['underlyings']:
                data['underlyings'] = [{'name': cert['underlying'], 'strike': None, 'spot': None, 'barrier': None}]
            
            # Categorizza
            if data['type']:
                self.stats['by_type'][data['type']] = self.stats['by_type'].get(data['type'], 0) + 1
            
            self.stats['detailed'] += 1
            return data
            
        except Exception as e:
            self.log(f"  Errore dettaglio {isin}: {str(e)[:50]}")
            self.stats['errors'] += 1
            return {**cert, 'type': 'Unknown', 'underlyings': [{'name': cert['underlying']}]}

    async def run(self, max_per_underlying=50):
        """Esecuzione principale"""
        self.log("=" * 60)
        self.log("CED SCRAPER - PRODUCTION")
        self.log("=" * 60)
        
        all_certs = []
        seen_isins = set()
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
            ctx = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
            )
            page = await ctx.new_page()
            
            try:
                # FASE 1: Raccogli ISIN per ogni sottostante
                self.log("\n📋 FASE 1: Ricerca per sottostante")
                for underlying in TARGET_UNDERLYINGS:
                    try:
                        results = await self.search_by_underlying(page, underlying)
                        for r in results[:max_per_underlying]:
                            if r['isin'] not in seen_isins:
                                seen_isins.add(r['isin'])
                                all_certs.append(r)
                        await asyncio.sleep(2)
                    except Exception as e:
                        self.log(f"  Skip {underlying}: {str(e)[:30]}")
                
                self.stats['found'] = len(all_certs)
                self.log(f"\n✅ Trovati {len(all_certs)} certificati unici")
                
                # FASE 2: Dettagli per ogni certificato
                self.log(f"\n📊 FASE 2: Estrazione dettagli")
                for i, cert in enumerate(all_certs):
                    self.log(f"[{i+1}/{len(all_certs)}] {cert['isin']}")
                    detail = await self.get_detail(page, cert)
                    self.certificates.append(detail)
                    await asyncio.sleep(1.5)
                    
            finally:
                await browser.close()
        
        return self.output()

    def output(self):
        return {
            'metadata': {
                'scraper_version': '3.0-final',
                'source': 'certificatiederivati.it/ricerca_avanzata',
                'timestamp': datetime.now().isoformat(),
                'scrape_date': datetime.now().strftime('%Y-%m-%d'),
                'total_certificates': len(self.certificates),
                'stats': self.stats
            },
            'certificates': self.certificates
        }


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--max', type=int, default=30, help='Max per sottostante')
    parser.add_argument('--output', default='certificates-data.json')
    args = parser.parse_args()
    
    scraper = CEDFinalScraper()
    results = await scraper.run(max_per_underlying=args.max)
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETATO")
    print(f"{'='*60}")
    print(f"Certificati trovati: {results['metadata']['stats']['found']}")
    print(f"Dettagli estratti: {results['metadata']['stats']['detailed']}")
    print(f"Errori: {results['metadata']['stats']['errors']}")
    print(f"Output: {args.output}")

if __name__ == '__main__':
    asyncio.run(main())
