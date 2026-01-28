#!/usr/bin/env python3
"""
CED Scraper v5 - Nuove Emissioni + Debug
"""

import asyncio
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


class CEDScraperV5:
    def __init__(self):
        self.certificates = []
        self.stats = {'found': 0, 'detailed': 0, 'errors': 0}

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

    async def scrape_nuove_emissioni(self, page):
        """Scrape dalla pagina Nuove Emissioni"""
        self.log("Caricamento Nuove Emissioni...")
        
        await page.goto('https://www.certificatiederivati.it/nuove_emissioni.asp', 
                       wait_until='networkidle', timeout=60000)
        await asyncio.sleep(3)
        
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Debug: salva HTML
        with open('debug_nuove_emissioni.html', 'w') as f:
            f.write(html)
        self.log("Salvato debug_nuove_emissioni.html")
        
        # Debug: mostra struttura tabelle
        tables = soup.find_all('table')
        self.log(f"Trovate {len(tables)} tabelle")
        
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            self.log(f"  Tabella {i}: {len(rows)} righe")
            if rows:
                # Mostra prima riga
                first_row = rows[0]
                cells = first_row.find_all(['td', 'th'])
                cell_texts = [c.get_text(strip=True)[:30] for c in cells[:6]]
                self.log(f"    Prima riga: {cell_texts}")
        
        # Cerca ISIN in tutta la pagina
        isin_pattern = re.compile(r'[A-Z]{2}[A-Z0-9]{9}[0-9]')
        all_isins = isin_pattern.findall(html)
        unique_isins = list(set(all_isins))
        self.log(f"\nISIN trovati nel HTML: {len(unique_isins)}")
        if unique_isins[:5]:
            self.log(f"  Esempi: {unique_isins[:5]}")
        
        # Cerca link a schede certificato
        cert_links = soup.find_all('a', href=re.compile(r'scheda_certificato.*isin='))
        self.log(f"Link a schede certificato: {len(cert_links)}")
        
        certificates = []
        
        # Metodo 1: Estrai da link
        for link in cert_links:
            href = link.get('href', '')
            isin_match = re.search(r'isin=([A-Z0-9]{12})', href)
            if isin_match:
                isin = isin_match.group(1)
                name = link.get_text(strip=True)
                if isin not in [c['isin'] for c in certificates]:
                    certificates.append({
                        'isin': isin,
                        'name': name,
                        'issuer': '',
                        'underlying': ''
                    })
        
        # Metodo 2: Se pochi, cerca nelle righe tabella
        if len(certificates) < 10:
            self.log("Pochi certificati dai link, provo parsing tabelle...")
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    row_text = row.get_text()
                    
                    # Cerca ISIN nella riga
                    isin_in_row = isin_pattern.findall(row_text)
                    for isin in isin_in_row:
                        if isin not in [c['isin'] for c in certificates]:
                            # Estrai altri dati dalla riga
                            name = ""
                            issuer = ""
                            for cell in cells:
                                txt = cell.get_text(strip=True)
                                if len(txt) > 20 and 'certificate' in txt.lower():
                                    name = txt
                                elif txt in ['Unicredit', 'BNP', 'Société', 'Vontobel', 'Leonteq', 'Intesa']:
                                    issuer = txt
                            
                            certificates.append({
                                'isin': isin,
                                'name': name,
                                'issuer': issuer,
                                'underlying': ''
                            })
        
        self.log(f"\nCertificati estratti: {len(certificates)}")
        return certificates

    async def scrape_ricerca(self, page):
        """Prova anche la ricerca avanzata"""
        self.log("\nProvo Ricerca Avanzata...")
        
        await page.goto('https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp', 
                       wait_until='networkidle', timeout=60000)
        await asyncio.sleep(3)
        
        # Salva HTML per debug
        html = await page.content()
        with open('debug_ricerca.html', 'w') as f:
            f.write(html)
        self.log("Salvato debug_ricerca.html")
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Trova form e bottone cerca
        forms = soup.find_all('form')
        self.log(f"Form trovati: {len(forms)}")
        
        # Cerca dropdown sottostante
        selects = soup.find_all('select')
        self.log(f"Select trovati: {len(selects)}")
        for sel in selects:
            name = sel.get('name', sel.get('id', 'unknown'))
            options = sel.find_all('option')
            self.log(f"  {name}: {len(options)} opzioni")
            if options and len(options) > 1:
                # Mostra prime 5 opzioni
                for opt in options[1:6]:
                    self.log(f"    - {opt.get('value', '')}: {opt.get_text(strip=True)[:40]}")
        
        # Clicca cerca senza filtri
        try:
            # Screenshot prima
            await page.screenshot(path='debug_before_search.png')
            
            # Trova e clicca il bottone
            submit = await page.query_selector('input[type="submit"]')
            if submit:
                self.log("Clicco Cerca...")
                await submit.click()
                await page.wait_for_load_state('networkidle', timeout=60000)
                await asyncio.sleep(3)
                
                # Screenshot dopo
                await page.screenshot(path='debug_after_search.png')
                
                # Salva HTML risultati
                html = await page.content()
                with open('debug_risultati.html', 'w') as f:
                    f.write(html)
                self.log("Salvato debug_risultati.html")
                
                # Conta ISIN
                isin_pattern = re.compile(r'[A-Z]{2}[A-Z0-9]{9}[0-9]')
                isins = list(set(isin_pattern.findall(html)))
                self.log(f"ISIN nei risultati: {len(isins)}")
                
        except Exception as e:
            self.log(f"Errore ricerca: {e}")
        
        return []

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
                'name': cert.get('name', ''),
                'type': '',
                'issuer': cert.get('issuer', ''),
                'market': '',
                'currency': 'EUR',
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
            
            # Tipo certificato da h2/h3
            for h in soup.find_all(['h2', 'h3']):
                txt = h.get_text(strip=True)
                if txt and len(txt) > 3 and 'Scheda' not in txt:
                    data['type'] = txt
                    break
            
            # Dati dalle tabelle
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        lbl = cells[0].get_text(strip=True).lower()
                        val = cells[1].get_text(strip=True)
                        
                        if 'emissione' in lbl and 'data' in lbl:
                            data['issue_date'] = self.parse_date(val)
                        elif 'scadenza' in lbl:
                            data['maturity_date'] = self.parse_date(val)
                        elif 'strike' in lbl and 'data' in lbl:
                            data['strike_date'] = self.parse_date(val)
                        elif 'barriera' in lbl and '%' in val:
                            data['barrier_down'] = self.parse_num(val)
                        elif 'tipo barriera' in lbl:
                            data['barrier_type'] = val
                        elif 'cedola' in lbl:
                            data['coupon'] = self.parse_num(val)
                        elif 'rendimento' in lbl:
                            data['annual_coupon_yield'] = self.parse_num(val)
                        elif lbl == 'bid' or 'denaro' in lbl:
                            data['bid_price'] = self.parse_num(val)
                        elif lbl == 'ask' or 'lettera' in lbl:
                            data['ask_price'] = self.parse_num(val)
                        elif 'emittente' in lbl:
                            data['issuer'] = val
                        elif 'mercato' in lbl:
                            data['market'] = val
            
            # Sottostanti
            for table in soup.find_all('table'):
                headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
                if any('sottostante' in h or 'descrizione' in h for h in headers):
                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if cells:
                            und = {
                                'name': cells[0].get_text(strip=True),
                                'strike': None,
                                'spot': None,
                                'barrier': None
                            }
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
                                data['underlyings'].append(und)
            
            self.stats['detailed'] += 1
            return data
            
        except Exception as e:
            self.log(f"Errore dettaglio {isin}: {str(e)[:50]}")
            self.stats['errors'] += 1
            return None

    async def run(self):
        self.log("=" * 60)
        self.log("CED SCRAPER V5 - DEBUG")
        self.log("=" * 60)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
            ctx = await browser.new_context(viewport={'width': 1920, 'height': 1080})
            page = await ctx.new_page()
            
            try:
                # Prova Nuove Emissioni
                certs = await self.scrape_nuove_emissioni(page)
                
                # Prova anche Ricerca
                await self.scrape_ricerca(page)
                
                self.stats['found'] = len(certs)
                
                # Estrai dettagli
                if certs:
                    self.log(f"\nEstrazione dettagli per {len(certs)} certificati...")
                    for i, cert in enumerate(certs[:50]):  # Max 50 per test
                        self.log(f"[{i+1}/{min(len(certs), 50)}] {cert['isin']}")
                        detail = await self.get_detail(page, cert)
                        if detail:
                            self.certificates.append(detail)
                        await asyncio.sleep(1)
                
            finally:
                await browser.close()
        
        return {
            'metadata': {
                'version': '5.0-debug',
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
    
    scraper = CEDScraperV5()
    results = await scraper.run()
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETATO")
    print(f"{'='*60}")
    print(f"Certificati: {len(results['certificates'])}")
    print(f"Output: {args.output}")
    print(f"\nFile debug generati:")
    print(f"  - debug_nuove_emissioni.html")
    print(f"  - debug_ricerca.html")
    print(f"  - debug_risultati.html")
    print(f"  - debug_before_search.png")
    print(f"  - debug_after_search.png")

if __name__ == '__main__':
    asyncio.run(main())
