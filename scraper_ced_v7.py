#!/usr/bin/env python3
"""
CED Scraper v7 - Debug completo
"""

import asyncio
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


async def main():
    print("=" * 60)
    print("CED SCRAPER V7 - DEBUG")
    print("=" * 60)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        ctx = await browser.new_context(viewport={'width': 1920, 'height': 1080})
        page = await ctx.new_page()
        
        # 1. Vai alla ricerca
        print("\n1. Carico ricerca avanzata...")
        await page.goto('https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp', 
                       wait_until='networkidle', timeout=60000)
        await asyncio.sleep(3)
        
        # 2. Screenshot prima
        await page.screenshot(path='debug_1_before.png', full_page=True)
        print("   Salvato: debug_1_before.png")
        
        # 3. Seleziona FTSE MIB
        print("\n2. Seleziono FTSEMIB Index...")
        try:
            # Prova select_option
            await page.select_option('select#sottostante', value='FTSEMIB Index')
            print("   select_option OK")
        except Exception as e:
            print(f"   select_option fallito: {e}")
            # Prova con JavaScript
            try:
                await page.evaluate('''
                    document.querySelector('select#sottostante').value = 'FTSEMIB Index';
                ''')
                print("   JS setValue OK")
            except Exception as e2:
                print(f"   JS fallito: {e2}")
        
        await asyncio.sleep(1)
        await page.screenshot(path='debug_2_selected.png', full_page=True)
        print("   Salvato: debug_2_selected.png")
        
        # 4. Trova e clicca il bottone cerca
        print("\n3. Cerco bottone submit...")
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Mostra tutti i bottoni/input submit
        for inp in soup.find_all('input'):
            print(f"   Input: type={inp.get('type')} value={inp.get('value')} name={inp.get('name')}")
        
        for btn in soup.find_all('button'):
            print(f"   Button: {btn.get_text(strip=True)[:30]}")
        
        # Clicca submit
        print("\n4. Clicco submit...")
        try:
            await page.click('input[type="submit"]')
            await page.wait_for_load_state('networkidle', timeout=60000)
            print("   Click OK")
        except Exception as e:
            print(f"   Click fallito: {e}")
        
        await asyncio.sleep(3)
        await page.screenshot(path='debug_3_results.png', full_page=True)
        print("   Salvato: debug_3_results.png")
        
        # 5. Salva HTML risultati
        html = await page.content()
        with open('debug_results.html', 'w', encoding='utf-8') as f:
            f.write(html)
        print("   Salvato: debug_results.html")
        
        # 6. Analizza risultati
        print("\n5. Analisi risultati...")
        print(f"   URL corrente: {page.url}")
        print(f"   Lunghezza HTML: {len(html)} bytes")
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Conta tabelle
        tables = soup.find_all('table')
        print(f"   Tabelle trovate: {len(tables)}")
        
        # Cerca ISIN
        isin_pattern = re.compile(r'[A-Z]{2}[A-Z0-9]{9}[0-9]')
        all_isins = list(set(isin_pattern.findall(html)))
        print(f"   ISIN trovati: {len(all_isins)}")
        if all_isins[:5]:
            print(f"   Esempi: {all_isins[:5]}")
        
        # Cerca link scheda
        links = soup.find_all('a', href=re.compile(r'scheda', re.I))
        print(f"   Link 'scheda': {len(links)}")
        for link in links[:5]:
            print(f"      {link.get('href', '')[:60]}")
        
        # Cerca testo "certificat"
        cert_text = soup.find_all(string=re.compile(r'certificat', re.I))
        print(f"   Testi con 'certificat': {len(cert_text)}")
        
        # Mostra titolo pagina
        title = soup.find('title')
        print(f"   Titolo: {title.get_text() if title else 'N/A'}")
        
        # Cerca messaggi di errore o "nessun risultato"
        for text in ['nessun', 'errore', 'error', 'no result', '0 risultat']:
            found = soup.find_all(string=re.compile(text, re.I))
            if found:
                print(f"   Trovato '{text}': {len(found)} volte")
        
        await browser.close()
    
    # Output JSON vuoto per non far fallire il workflow
    output = {
        'metadata': {
            'version': '7.0-debug',
            'timestamp': datetime.now().isoformat(),
            'total_certificates': 0
        },
        'certificates': []
    }
    
    with open('certificates-data.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    print("\n" + "=" * 60)
    print("DEBUG COMPLETATO - Controlla gli artifacts")
    print("=" * 60)

if __name__ == '__main__':
    asyncio.run(main())
