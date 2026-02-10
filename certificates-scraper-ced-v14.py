#!/usr/bin/env python3
"""
Certificates Scraper v14 - BORSA ITALIANA
ISIN verificati + attesa caricamento completo
"""

import json
import re
import time
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# ISIN VERIFICATI su Indici/Commodities
VERIFIED_ISINS = [
    "XS2209061329",  # IS EP CP EURO STOXX 50
    "IT0005525610",  # Uc Cc Ftse Mib/Eurostoxx50
    "IT0005498297",  # Uc Cc Ftse Mib/Eurostoxx50
    "CH1283542608",  # Lq Exp Euro Stoxx 50/Ftse Mib
    "XS1385761249",  # Bpa Exp Euro Stoxx 50/Ftse Mib
    "DE000HV4K3K8",
    "DE000HV4K3L6",
    "DE000HV4K3M4",
    "IT0005521411",
    "XS2314073839",
    "XS2314074050",
    "XS2394956712",
    "XS2314660502",
    "NL0015436031",
    "NL0015436072",
    "DE000SF5ACX5",
    "GB00B15KXV33",
    "JE00B1VS3770",
    "GB00B15KXQ89",
    "XS1073722347",
]

CONFIG = {
    "timeout": 45000,
    "output_file": "certificates-data.json"
}


def parse_number(text):
    if not text or text.strip().upper() in ['N.A.', 'N.D.', '-', '', 'N/A', '--']:
        return None
    try:
        cleaned = re.sub(r'[EUR€%\s\xa0]', '', text.strip())
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        return round(float(cleaned), 4)
    except:
        return None


def parse_date(text):
    if not text or 'N.A' in text or 'Open' in text or '--' in text:
        return None
    try:
        if '/' in text:
            p = text.strip().split('/')
            if len(p) == 3:
                return f"{p[2]}-{p[1].zfill(2)}-{p[0].zfill(2)}"
    except:
        pass
    return text


def get_certificate(page, isin):
    """Estrae dati certificato da Borsa Italiana"""
    
    urls = [
        f"https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{isin}.html",
        f"https://www.borsaitaliana.it/borsa/cw-e-certificates/eurotlx/scheda/{isin}.html",
        f"https://www.borsaitaliana.it/borsa/cw-e-certificates/eurotlx/scheda/{isin}-ETLX.html",
        f"https://www.borsaitaliana.it/borsa/etf/scheda/{isin}.html"
    ]
    
    for url in urls:
        try:
            page.goto(url, timeout=CONFIG["timeout"], wait_until="networkidle")
            time.sleep(2)  # Aspetta caricamento JS
            
            html = page.content()
            
            if "Page Not Found" in html or "404" in html:
                continue
            
            soup = BeautifulSoup(html, 'html.parser')
            
            cert = {
                "isin": isin,
                "name": "",
                "type": "",
                "issuer": "",
                "market": "",
                "currency": "EUR",
                "underlying": "",
                "strike": None,
                "barrier_pct": None,
                "expiry_date": None,
                "reference_price": None,
                "bid_price": None,
                "ask_price": None,
                "source": "borsaitaliana.it",
                "url": url,
                "scraped_at": datetime.now().isoformat()
            }
            
            # NOME - cerca h1 con classe specifica o il primo h1 significativo
            h1 = soup.find('h1', class_='t-text')
            if not h1:
                h1 = soup.find('h1')
            
            if h1:
                name = h1.get_text(strip=True)
                # Ignora nomi generici
                if name and name not in ['Strumenti SeDeX', 'Strumenti EuroTLX', 'CW e Certificati']:
                    cert["name"] = name
            
            # Se nome non trovato, prova dal breadcrumb o title
            if not cert["name"]:
                # Prova span con nome strumento
                name_span = soup.find('span', class_='t-text')
                if name_span:
                    cert["name"] = name_span.get_text(strip=True)
            
            if not cert["name"]:
                title = soup.find('title')
                if title:
                    title_text = title.get_text(strip=True)
                    # Estrai nome dal title "NOME: scheda completa..."
                    if ':' in title_text:
                        cert["name"] = title_text.split(':')[0].strip()
                    elif ' - ' in title_text:
                        cert["name"] = title_text.split(' - ')[0].strip()
            
            # Cerca dati nelle tabelle
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all(['th', 'td'])
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        
                        # Prezzo riferimento
                        if 'riferimento' in label or 'reference' in label:
                            p = parse_number(value)
                            if p and p > 0:
                                cert["reference_price"] = p
                        elif 'ultimo' in label or 'last' in label:
                            if not cert["reference_price"]:
                                p = parse_number(value)
                                if p and p > 0:
                                    cert["reference_price"] = p
                        elif 'close' in label:
                            if not cert["reference_price"]:
                                p = parse_number(value)
                                if p and p > 0:
                                    cert["reference_price"] = p
                        
                        # Altri dati
                        elif 'bid' in label or 'denaro' in label:
                            cert["bid_price"] = parse_number(value)
                        elif 'ask' in label or 'lettera' in label:
                            cert["ask_price"] = parse_number(value)
                        elif 'emittente' in label or 'issuer' in label:
                            if len(value) > 2:
                                cert["issuer"] = value
                        elif 'sottostante' in label or 'underlying' in label:
                            if 'valore' not in label and len(value) > 2:
                                cert["underlying"] = value
                        elif 'scadenza' in label or 'expiry' in label:
                            cert["expiry_date"] = parse_date(value)
                        elif 'barriera' in label or 'barrier' in label:
                            cert["barrier_pct"] = parse_number(value)
                        elif 'strike' in label:
                            cert["strike"] = parse_number(value)
                        elif 'tipologia' in label or 'marketing' in label or 'type' in label:
                            if len(value) > 2:
                                cert["type"] = value
                        elif 'mercato' in label or 'market' in label:
                            if len(value) > 1:
                                cert["market"] = value
            
            # Verifica che abbiamo dati validi (nome o prezzo)
            if cert["name"] or cert["reference_price"]:
                # Se nome ancora vuoto, usa ISIN
                if not cert["name"]:
                    cert["name"] = f"Certificate {isin}"
                return cert
                
        except Exception as e:
            continue
    
    return None


def main():
    print("=" * 60)
    print("Certificates Scraper v14 - BORSA ITALIANA")
    print("ISIN verificati su Indici/Commodities/Valute/Tassi")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    certificates = []
    errors = 0
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            locale="it-IT"
        )
        page = context.new_page()
        
        try:
            print(f"\nProcessing {len(VERIFIED_ISINS)} verified ISINs...\n")
            
            for i, isin in enumerate(VERIFIED_ISINS, 1):
                print(f"[{i}/{len(VERIFIED_ISINS)}] {isin}...", end=" ", flush=True)
                
                cert = get_certificate(page, isin)
                
                if cert:
                    certificates.append(cert)
                    price = cert.get("reference_price")
                    price_str = f"{price} EUR" if price else "N/A"
                    name = cert.get("name", "")[:40]
                    print(f"OK - {price_str} - {name}")
                else:
                    errors += 1
                    print("NOT FOUND")
                
                time.sleep(0.5)
            
        finally:
            browser.close()
    
    # Output
    output = {
        "metadata": {
            "version": "14.0",
            "timestamp": datetime.now().isoformat(),
            "source": "borsaitaliana.it",
            "total": len(certificates),
            "not_found": errors,
            "filter": "Indices, Commodities, Currencies, Rates"
        },
        "certificates": certificates
    }
    
    with open(CONFIG["output_file"], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 60)
    print("COMPLETED")
    print(f"  Saved: {len(certificates)} certificates")
    print(f"  Not found: {errors}")
    print(f"  File: {CONFIG['output_file']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
