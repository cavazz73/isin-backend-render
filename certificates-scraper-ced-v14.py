#!/usr/bin/env python3
"""
Certificates Scraper v15 - HYBRID (Static + Live Data)
Estrae Dati Statici + PREZZI LIVE da certificatiederivati.it
Include gestione Sessione e Fallback Regex.
"""

import json
import re
import time
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# ===================================
# CONFIGURAZIONE
# ===================================

CONFIG = {
    "home_url": "https://www.certificatiederivati.it",
    "list_url": "https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp",
    "detail_url": "https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin=",
    "max_certificates": 100,
    "page_timeout": 30000,
    "output_file": "certificates-data.json"
}

# Target underlyings
TARGET_KEYWORDS = [
    "indici", "index", "stoxx", "mib", "dax", "cac", "nasdaq", "s&p",
    "nikkei", "gold", "oro", "oil", "petrolio", "silver", "argento",
    "commodity", "worst of", "basket di indici"
]

def clean_text(text):
    if not text: return None
    return re.sub(r'\s+', ' ', text).strip()

def is_target_underlying(text):
    if not text: return False
    return any(kw in text.lower() for kw in TARGET_KEYWORDS)

def parse_number(text):
    """Converte stringa in numero (gestisce formato italiano)"""
    if not text: return None
    try:
        cleaned = text.strip().upper().replace('EUR', '').replace('€', '').replace('%', '').replace(' ', '').replace('\xa0', '')
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        return float(cleaned)
    except:
        return None

def parse_date(text):
    """Converte data italiana in ISO"""
    if not text: return None
    try:
        parts = text.strip().split('/')
        if len(parts) == 3:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return text
    except:
        return text

def extract_certificate_data(page, isin, list_data=None):
    """Estrae TUTTI i dati (Statici + Prezzi)"""
    url = f"{CONFIG['detail_url']}{isin}"
    
    try:
        page.goto(url, timeout=CONFIG["page_timeout"])
        # Attesa intelligente: o tabella o timeout breve (non bloccare tutto se manca)
        try:
            page.wait_for_selector("table.table", timeout=5000)
        except:
            pass 
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        full_text = clean_text(soup.get_text(separator=' | ')) # Testo "piatto" per regex prezzi

        cert = {
            "isin": isin,
            "name": list_data.get("name", "") if list_data else "",
            "type": "",
            "issuer": list_data.get("issuer", "") if list_data else "",
            "market": list_data.get("market", "") if list_data else "",
            "currency": "EUR",
            "price": None,        # NUOVO CAMPO
            "nominal": 1000,
            "issue_date": None,
            "maturity_date": None,
            "strike_date": None,
            "barrier_down": None,
            "underlyings": [],
            "source": "certificatiederivati.it",
            "scraped_at": datetime.now().isoformat()
        }

        # ---------------------------------------------------------
        # 1. ESTRAZIONE PREZZO (La parte mancante!)
        # ---------------------------------------------------------
        # Cerca pattern: "Prezzo ... 100,50"
        regex_prezzo = r'(?:Prezzo|Ultimo|Valore|Quotazione|Ask|Lettera)[^0-9]{0,30}(\d+[.,]\d+)'
        match_price = re.search(regex_prezzo, full_text, re.IGNORECASE)
        if match_price:
            cert["price"] = parse_number(match_price.group(1))
        
        # Fallback: Cerca numero isolato formattato come prezzo (es. | 98,45 |)
        if not cert["price"]:
             fallback_price = re.search(r'\|\s*(\d{2,5}[.,]\d{2})\s*\|', full_text)
             if fallback_price:
                 cert["price"] = parse_number(fallback_price.group(1))

        # ---------------------------------------------------------
        # 2. DATI TABELLARI (Metodo Classico v14)
        # ---------------------------------------------------------
        type_header = soup.find('h3', class_='panel-title')
        if type_header: cert["type"] = type_header.get_text(strip=True)

        for table in soup.find_all('table', class_='table'):
            for row in table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    label = th.get_text(strip=True).upper()
                    value = td.get_text(strip=True)
                    
                    if "MERCATO" in label and not cert["market"]: cert["market"] = value
                    elif "DATA EMISSIONE" in label: cert["issue_date"] = parse_date(value)
                    elif "DATA SCADENZA" in label: cert["maturity_date"] = parse_date(value)
                    elif "DATA STRIKE" in label: cert["strike_date"] = parse_date(value)
                    elif "NOMINALE" in label: cert["nominal"] = parse_number(value) or 1000
                    elif "VALUTA" in label and "DIVISA" not in label: cert["currency"] = value

        # ---------------------------------------------------------
        # 3. BARRIERA (Metodo Ibrido JS + Testo)
        # ---------------------------------------------------------
        # Tentativo 1: JS (Metodo v14)
        barrier_match = re.search(r'barriera:\s*["\'](\d+(?:[.,]\d+)?)\s*(?:&nbsp;)?%["\']', html)
        if barrier_match:
            cert["barrier_down"] = parse_number(barrier_match.group(1))
        
        # Tentativo 2: Testo (Fallback)
        if not cert["barrier_down"]:
            barrier_text = re.search(r'(?:Barriera|Barrier)[^0-9]{0,30}(\d+[.,]\d+)', full_text, re.IGNORECASE)
            if barrier_text:
                cert["barrier_down"] = parse_number(barrier_text.group(1))

        # ---------------------------------------------------------
        # 4. SOTTOSTANTI
        # ---------------------------------------------------------
        sottostante_header = soup.find('h3', string=re.compile(r'Scheda Sottostante', re.IGNORECASE))
        if sottostante_header:
            panel = sottostante_header.find_parent('div', class_='panel')
            if panel:
                table = panel.find('table')
                if table:
                    rows = table.find_all('tr')
                    for row in rows[1:]:
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            name = cells[0].get_text(strip=True)
                            if name and name.upper() != "DESCRIZIONE":
                                strike = parse_number(cells[1].get_text(strip=True)) if len(cells) > 1 else None
                                # Calcolo Barriera Assoluta
                                barrier_abs = None
                                if strike and cert["barrier_down"]:
                                    barrier_abs = round(strike * (cert["barrier_down"] / 100), 2)
                                
                                cert["underlyings"].append({
                                    "name": name,
                                    "strike": strike,
                                    "barrier": barrier_abs
                                })

        return cert
        
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return None

def get_certificate_list(page):
    print("📋 Fetching certificate list...")
    certificates = []
    try:
        page.goto(CONFIG["list_url"], timeout=CONFIG["page_timeout"])
        page.wait_for_load_state("networkidle")
        
        # Estrazione più robusta (cerca link con ISIN)
        hrefs = page.evaluate("""() => {
            return Array.from(document.querySelectorAll("table a[href*='isin=']")).map(a => ({
                href: a.href,
                text: a.innerText
            }))
        }""")
        
        # Deduplica ISIN
        seen = set()
        for item in hrefs:
            m = re.search(r'isin=([A-Z0-9]{12})', item['href'], re.IGNORECASE)
            if m:
                isin = m.group(1).upper()
                if isin not in seen:
                    seen.add(isin)
                    certificates.append({
                        "isin": isin, 
                        "name": item['text'].strip() or isin,
                        "issuer": "N/D", # Verrà popolato dopo
                        "market": "SeDeX"
                    })
        
        print(f"  ✅ Found {len(certificates)} certificates")
        
    except Exception as e:
        print(f"  ❌ Error list: {e}")
    
    return certificates

def main():
    print("=" * 60)
    print("🚀 Certificates Scraper v15 - LIVE PRICES")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    all_certificates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # User Agent Reale
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        try:
            # 1. RISCALDAMENTO (Visita Home per Cookie)
            print("🌍 Visiting Home Page (Session Init)...")
            try:
                page.goto(CONFIG["home_url"], timeout=15000)
                time.sleep(2)
            except:
                print("⚠️ Home page timeout, proceeding anyway...")

            # 2. LISTA
            cert_list = get_certificate_list(page)
            if not cert_list:
                print("❌ No certificates found!")
                return
            
            cert_list = cert_list[:CONFIG["max_certificates"]]
            print(f"\n📊 Processing {len(cert_list)} certificates...\n")
            
            for i, item in enumerate(cert_list, 1):
                print(f"[{i}/{len(cert_list)}] {item['isin']}...", end="\r")
                
                cert = extract_certificate_data(page, item["isin"], item)
                
                if cert:
                    # Filtro Asset Class (opzionale, qui permissive)
                    # if is_target_underlying(cert['name']): ...
                    
                    all_certificates.append(cert)
                    price_str = f"{cert['price']}€" if cert['price'] else "NO PRICE"
                    print(f"✅ {item['isin']} | {price_str} | Barrier: {cert['barrier_down']}%     ")
                else:
                    print(f"❌ {item['isin']} Failed                                      ")
                
                time.sleep(0.5) # Gentilezza
            
        finally:
            browser.close()
    
    # Salva output
    output = {
        "metadata": {
            "version": "15.0",
            "timestamp": datetime.now().isoformat(),
            "note": "Includes Live Prices"
        },
        "certificates": all_certificates
    }
    
    with open(CONFIG["output_file"], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 60)
    print(f"💾 Saved {len(all_certificates)} certificates to {CONFIG['output_file']}")
    print("=" * 60)

if __name__ == "__main__":
    main()
