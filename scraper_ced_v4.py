#!/usr/bin/env python3
"""
CED Scraper v4 - Semplificato
Scarica TUTTI i certificati dalla ricerca, filtra localmente
"""

import asyncio
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Keywords per tenere (indici, commodities, tassi, valute)
KEEP_KEYWORDS = [
    'indic', 'index', 'mib', 'dax', 'stoxx', 'nasdaq', 's&p', 'dow', 'ftse', 'cac', 'ibex',
    'gold', 'oro', 'silver', 'argento', 'oil', 'petrolio', 'wti', 'brent', 'copper', 'rame',
    'gas', 'commodity', 'metall', 'platino', 'palladio',
    'euribor', 'libor', 'tasso', 'rate', 'btp', 'bund', 'treasury', 'credit', 'obbligazioni',
    'eur/usd', 'eur/gbp', 'usd/jpy', 'forex', 'valuta', 'currency'
]

# Keywords per escludere (azioni singole)
EXCLUDE_KEYWORDS = ['basket di azioni', 'singolo sottostante', 'worst of']


class CEDScraperV4:
    def __init__(self):
        self.certificates = []
        self.stats = {'total': 0, 'kept': 0, 'excluded': 0, 'detailed': 0, 'errors': 0}

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def should_keep(self, name, underlying):
        """Ritorna True se il certificato è su indici/commodities/tassi/valute"""
        text = f"{name} {underlying}".lower()
        
        # Escludi se contiene keywords negative
        for kw in EXCLUDE_KEYWORDS:
            if kw in text:
                return False
        
        # Tieni se contiene keywords positive
        for kw in KEEP_KEYWORDS:
            if kw in text:
                return True
        
        return False

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

    async def get_all_certificates(self, page):
        """Scarica lista completa dalla ricerca (senza filtri)"""
        self.log("Caricamento ricerca avanzata...")
        
        # Vai alla ricerca avanzata
        await page.goto('https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp', 
                       wait_until='networkidle', timeout=60000)
        await asyncio.sleep(3)
        
        # Clicca Cerca senza filtri per avere tutto
        try:
            # Cerca il bottone submit
            submit = await page.query_selector('input[type="submit"], input[value="Cerca"], button:has-text("Cerca")')
            if submit:
                await submit.click()
                await page.wait_for_load_state('networkidle', timeout=60000)
                await asyncio.sleep(3)
        except Exception as e:
            self.log(f"Click cerca: {e}")
        
        # Prova anche URL diretto con tutti i risultati
        try:
            await page.goto('https://www.certificatiederivati.it/db_bs_estrazione_ricerca.asp', 
                           wait_until='networkidle', timeout=60000)
            await asyncio.sleep(3)
        except:
            pass
        
        all_certs = []
        page_num = 1
        max_pages = 30
        
        while page_num <= max_pages:
            self.log(f"Pagina {page_num}...")
            
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Trova certificati nella pagina
            found_in_page = 0
            for table in soup.find_all('table'):
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        isin = cols[0].get_text(strip=True)
                        if len(isin) == 12 and isin.isalnum() and isin not in [c['isin'] for c in all_certs]:
                            name = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                            # Escludi Turbo/Leva
                            if 'TURBO' in name.upper() or 'LEVA' in name.upper():
                                continue
                            
                            issuer = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                            underlying = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                            market = cols[4].get_text(strip=True) if len(cols) > 4 else ""
                            
                            all_certs.append({
                                'isin': isin,
                                'name': name,
                                'issuer': issuer,
                                'underlying': underlying,
                                'market': market
                            })
                            found_in_page += 1
            
            self.log(f"  Trovati: {found_in_page}")
            
            if found_in_page == 0:
                break
            
            # Prossima pagina
            try:
                next_link = await page.query_selector('a:has-text("Succ"), a:has-text(">>"), a:has-text("Next")')
                if next_link:
                    await next_link.click()
                    await page.wait_for_load_state('networkidle', timeout=30000)
                    await asyncio.sleep(2)
                    page_num += 1
                else:
                    break
            except:
                break
        
        self.stats['total'] = len(all_certs)
        return all_certs

    async def get_detail(self, page, cert):
        """Estrae dettagli dalla scheda"""
        isin = cert['isin']
        url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
        
        try:
            await page.goto(url, wait_until='networkidle', timeout=45000)
            await asyncio.sleep(2)
            
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            data = {
                'isin': isin,
                'name': cert['name'],
                'type': '',
                'issuer': cert['issuer'],
                'market': cert.get('market', ''),
                'currency': 'EUR',
                'issue_date': None,
                'maturity_date': None,
                'strike_date': None,
                'barrier_down': None,
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
                        elif 'barriera' in lbl:
                            data['barrier_down'] = self.parse_num(val)
                        elif 'cedola' in lbl or 'premio' in lbl:
                            data['coupon'] = self.parse_num(val)
                        elif 'rendimento' in lbl:
                            data['annual_coupon_yield'] = self.parse_num(val)
            
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
                data['underlyings'] = [{'name': cert['underlying']}]
            
            self.stats['detailed'] += 1
            return data
            
        except Exception as e:
            self.log(f"Errore {isin}: {str(e)[:40]}")
            self.stats['errors'] += 1
            return {**cert, 'type': 'Unknown', 'underlyings': [{'name': cert['underlying']}]}

    async def run(self):
        self.log("=" * 60)
        self.log("CED SCRAPER V4")
        self.log("=" * 60)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
            ctx = await browser.new_context(viewport={'width': 1920, 'height': 1080})
            page = await ctx.new_page()
            
            try:
                # Fase 1: Lista
                all_certs = await self.get_all_certificates(page)
                self.log(f"\nTotale scaricati: {len(all_certs)}")
                
                # Fase 2: Filtra
                filtered = [c for c in all_certs if self.should_keep(c['name'], c['underlying'])]
                self.stats['kept'] = len(filtered)
                self.stats['excluded'] = len(all_certs) - len(filtered)
                self.log(f"Dopo filtro: {len(filtered)} (esclusi: {self.stats['excluded']})")
                
                # Fase 3: Dettagli
                self.log(f"\nEstrazione dettagli...")
                for i, cert in enumerate(filtered):
                    self.log(f"[{i+1}/{len(filtered)}] {cert['isin']}")
                    detail = await self.get_detail(page, cert)
                    self.certificates.append(detail)
                    await asyncio.sleep(1)
                    
            finally:
                await browser.close()
        
        return {
            'metadata': {
                'version': '4.0',
                'source': 'certificatiederivati.it',
                'timestamp': datetime.now().isoformat(),
                'total_certificates': len(self.certificates),
                'stats': self.stats
            },
            'certificates': self.certificates
        }


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='certificates-data.json')
    args = parser.parse_args()
    
    scraper = CEDScraperV4()
    results = await scraper.run()
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ Output: {args.output}")
    print(f"Certificati: {len(results['certificates'])}")

if __name__ == '__main__':
    asyncio.run(main())
