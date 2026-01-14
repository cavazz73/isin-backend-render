#!/usr/bin/env python3
"""
LIVE ISIN SEARCH SCRAPER
On-demand scraping for any ISIN not in local database
Usage: python3 live_search.py --isin IT0006771510
"""

import asyncio
import json
import re
import sys
import argparse
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime

def clean_text(text):
    """Clean and normalize text"""
    return re.sub(r'\s+', ' ', text).strip() if text else "N/A"

def parse_float(text):
    """Parse float from various formats"""
    if not text:
        return None
    text = str(text).upper().replace('EUR', '').replace('€', '').replace('%', '').strip()
    clean = re.sub(r'[^\d,.-]', '', text).replace(',', '.')
    try:
        return float(clean)
    except:
        return None

# ==========================================
# SCRAPER: CERTIFICATI
# ==========================================
async def scrape_certificate(page, isin):
    """Scrape certificate from certificatiederivati.it"""
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        
        # Check if redirected to home (= not found)
        if "home.asp" in page.url:
            return None
        
        # Wait for content
        try:
            await page.wait_for_selector("text=Scheda", timeout=5000)
        except:
            pass
        
        await asyncio.sleep(1)
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        # Check if valid certificate page
        if not soup.find(string=re.compile("Emittente|ISIN", re.IGNORECASE)):
            return None
        
        # Extract name
        name_elem = soup.find('font', size='+1')
        if not name_elem:
            name_elem = soup.find('td', class_='titolo_scheda')
        name = clean_text(name_elem.get_text()) if name_elem else f"Certificate {isin}"
        
        # Extract issuer
        def get_issuer():
            section = soup.find('h3', string=re.compile('Scheda Emittente', re.IGNORECASE))
            if section:
                parent = section.find_parent('div')
                if parent:
                    table = parent.find('table')
                    if table:
                        for td in table.find_all('td'):
                            text = td.get_text(strip=True)
                            if text and 2 < len(text) < 50 and 'Rating' not in text and ':' not in text:
                                if any(c.isalpha() for c in text):
                                    return text
            return "N/A"
        
        # Extract price
        def get_price():
            patterns = ['Prezzo emissione', 'Ultimo', 'Valore', 'Prezzo']
            for pattern in patterns:
                th = soup.find('th', string=re.compile(pattern, re.IGNORECASE))
                if th:
                    row = th.find_parent('tr')
                    if row:
                        td = row.find('td')
                        if td:
                            price = parse_float(td.get_text())
                            if price:
                                return price
            return None
        
        # Extract barrier
        def get_barrier():
            section = soup.find('h3', string=re.compile('Barriera', re.IGNORECASE))
            if section:
                panel = section.find_parent('div', class_='panel')
                if panel:
                    div = panel.find('div', id='barriera')
                    if div:
                        for td in div.find_all('td'):
                            text = td.get_text(strip=True)
                            match = re.search(r'(\d+)\s*%', text)
                            if match:
                                return int(match.group(1))
            return None
        
        # Extract coupon
        def get_coupon():
            div = soup.find('div', id='rilevamento')
            if div:
                table = div.find('table')
                if table:
                    tbody = table.find('tbody')
                    if tbody:
                        row = tbody.find('tr')
                        if row:
                            for td in row.find_all('td'):
                                text = td.get_text(strip=True)
                                match = re.search(r'(\d+[.,]\d+)\s*%', text)
                                if match:
                                    return float(match.group(1).replace(',', '.'))
            return None
        
        issuer = get_issuer()
        price = get_price()
        barrier = get_barrier()
        coupon = get_coupon()
        
        return {
            "isin": isin,
            "name": name,
            "type": "CERTIFICATE",
            "symbol": isin,
            "issuer": issuer,
            "market": "SeDeX",
            "currency": "EUR",
            "price": price,
            "last_price": price,
            "barrier": barrier,
            "barrier_down": barrier,
            "coupon": coupon,
            "source": "certificatiederivati.it (Live)",
            "scraped": True,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        # Silent fail - return None
        return None

# ==========================================
# SCRAPER: BONDS
# ==========================================
async def scrape_bond(page, isin):
    """Scrape bond from Borsa Italiana or other sources"""
    # Try Borsa Italiana MOT
    url = f"https://www.borsaitaliana.it/borsa/obbligazioni/mot/ricerca.html?textToSearch={isin}"
    
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)
        
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        # Check if found
        title = soup.find('h1')
        if not title:
            return None
        
        name = clean_text(title.get_text())
        
        # Extract price
        price = None
        price_elem = soup.select_one('.price') or soup.select_one('[class*="quotation"]')
        if price_elem:
            price = parse_float(price_elem.get_text())
        
        # Determine bond type
        bond_type = "BOND"
        if "BTP" in name.upper() or "REPUBBLICA" in name.upper():
            bond_type = "BTP/GOV"
        elif "CORPORATE" in name.upper():
            bond_type = "CORPORATE_BOND"
        
        return {
            "isin": isin,
            "name": name,
            "type": bond_type,
            "symbol": isin,
            "market": "MOT",
            "currency": "EUR",
            "price": price,
            "last_price": price,
            "source": "Borsa Italiana (Live)",
            "scraped": True,
            "timestamp": datetime.now().isoformat()
        }
        
    except:
        return None

# ==========================================
# ORCHESTRATOR
# ==========================================
async def hunt_isin(isin):
    """Search for ISIN across all sources"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        page.set_default_timeout(20000)
        
        result = None
        
        # 1. Try Certificates first (most common)
        result = await scrape_certificate(page, isin)
        
        # 2. If not found, try Bonds
        if not result:
            result = await scrape_bond(page, isin)
        
        await browser.close()
        
        # OUTPUT: Pure JSON to stdout
        if result:
            print(json.dumps(result, ensure_ascii=False))
        else:
            print(json.dumps({"error": "not_found", "isin": isin}))

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Live ISIN search')
    parser.add_argument('--isin', required=True, help='ISIN code to search')
    args = parser.parse_args()
    
    # Validate ISIN format
    isin = args.isin.upper().strip()
    if not re.match(r'^[A-Z]{2}[A-Z0-9]{9}\d$', isin):
        print(json.dumps({"error": "invalid_isin", "isin": isin}))
        sys.exit(1)
    
    # Run search
    try:
        asyncio.run(hunt_isin(isin))
    except Exception as e:
        print(json.dumps({"error": "script_error", "message": str(e)}))
        sys.exit(1)
