#!/usr/bin/env python3
"""
Certificates Scraper - COMPLETE STATIC DATA
Estrae TUTTI i dati disponibili da certificatiederivati.it
Calcola campi derivati (barriera assoluta, trigger assoluto)
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
    "base_url": "https://www.certificatiederivati.it",
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


def is_target_underlying(text):
    if not text:
        return False
    return any(kw in text.lower() for kw in TARGET_KEYWORDS)


def parse_number(text):
    """Converte stringa in numero (gestisce formato italiano)"""
    if not text:
        return None
    try:
        # Rimuovi spazi e caratteri non numerici (eccetto . , -)
        cleaned = text.strip().replace(' ', '').replace('\xa0', '')
        # Formato italiano: 1.234,56 -> 1234.56
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        return float(cleaned)
    except:
        return None


def parse_date(text):
    """Converte data italiana in ISO"""
    if not text:
        return None
    try:
        # Formato: 30/04/2020
        parts = text.strip().split('/')
        if len(parts) == 3:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return text
    except:
        return text


def extract_certificate_data(page, isin, list_data=None):
    """Estrae TUTTI i dati dalla pagina certificato"""
    url = f"{CONFIG['detail_url']}{isin}"
    
    try:
        page.goto(url, timeout=CONFIG["page_timeout"])
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2000)
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        cert = {
            "isin": isin,
            "name": "",
            "type": "",
            "issuer": "",
            "market": "",
            "currency": "EUR",
            "nominal": 1000,
            "issue_price": None,
            "issue_date": None,
            "maturity_date": None,
            "strike_date": None,
            "final_valuation_date": None,
            "trading_date": None,
            "barrier_down": None,
            "barrier_type": None,
            "trigger": None,
            "fx_risk": None,
            "quantity": None,
            "underlyings": [],
            "source": "certificatiederivati.it",
            "scraped_at": datetime.now().isoformat()
        }
        
        # Copia dati dalla lista se disponibili
        if list_data:
            cert["name"] = list_data.get("name", "")
            cert["issuer"] = list_data.get("issuer", "")
            cert["market"] = list_data.get("market", "")
        
        # =====================
        # 1. TIPO CERTIFICATO
        # =====================
        type_header = soup.find('h3', class_='panel-title')
        if type_header:
            cert["type"] = type_header.get_text(strip=True)
        
        # =====================
        # 2. TABELLE DATI
        # =====================
        for table in soup.find_all('table', class_='table'):
            for row in table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    label = th.get_text(strip=True).upper()
                    value = td.get_text(strip=True)
                    
                    # Info principali
                    if "MERCATO" in label and not cert["market"]:
                        cert["market"] = value
                    elif "DATA EMISSIONE" in label:
                        cert["issue_date"] = parse_date(value)
                    elif "DATA SCADENZA" in label:
                        cert["maturity_date"] = parse_date(value)
                    elif "DATA STRIKE" in label:
                        cert["strike_date"] = parse_date(value)
                    elif "VALUTAZIONE FINALE" in label:
                        cert["final_valuation_date"] = parse_date(value)
                    elif "DATA NEGOZIAZIONE" in label:
                        cert["trading_date"] = parse_date(value)
                    
                    # Caratteristiche
                    elif "VALUTA" in label and "DIVISA" not in label:
                        cert["currency"] = value
                    elif "DIVISA CERTIFICATO" in label:
                        cert["currency"] = value
                    elif "NOMINALE" in label:
                        cert["nominal"] = parse_number(value) or 1000
                    elif "PREZZO EMISSIONE" in label:
                        cert["issue_price"] = parse_number(value)
                    elif "TRIGGER" in label:
                        cert["trigger"] = parse_number(value)
                    elif "QUANTIT" in label:
                        cert["quantity"] = parse_number(value)
                    elif "RISCHIO CAMBIO" in label:
                        cert["fx_risk"] = value
        
        # =====================
        # 3. EMITTENTE
        # =====================
        emittente_header = soup.find('h3', string=re.compile(r'Scheda Emittente', re.IGNORECASE))
        if emittente_header:
            panel = emittente_header.find_parent('div', class_='panel')
            if panel:
                table = panel.find('table')
                if table:
                    first_td = table.find('td')
                    if first_td:
                        issuer_text = first_td.get_text(strip=True)
                        # Evita rating e link
                        if issuer_text and "Rating" not in issuer_text and "@" not in issuer_text and "http" not in issuer_text.lower():
                            cert["issuer"] = issuer_text
        
        # =====================
        # 4. SOTTOSTANTI
        # =====================
        sottostante_header = soup.find('h3', string=re.compile(r'Scheda Sottostante', re.IGNORECASE))
        if sottostante_header:
            panel = sottostante_header.find_parent('div', class_='panel')
            if panel:
                table = panel.find('table')
                if table:
                    rows = table.find_all('tr')
                    for row in rows[1:]:  # Skip header
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            name = cells[0].get_text(strip=True)
                            if name and name.upper() != "DESCRIZIONE":
                                underlying = {
                                    "name": name,
                                    "strike": parse_number(cells[1].get_text(strip=True)) if len(cells) > 1 else None,
                                    "weight": parse_number(cells[2].get_text(strip=True)) if len(cells) > 2 else None,
                                    "barrier": None,  # Calcolato dopo
                                    "trigger_level": None  # Calcolato dopo
                                }
                                cert["underlyings"].append(underlying)
        
        # =====================
        # 5. BARRIERA DAL JAVASCRIPT
        # =====================
        # Cerca: barriera: "50&nbsp;%"
        barrier_match = re.search(r'barriera:\s*["\'](\d+(?:[.,]\d+)?)\s*(?:&nbsp;)?%["\']', html)
        if barrier_match:
            cert["barrier_down"] = parse_number(barrier_match.group(1))
        
        # Tipo barriera: tipo: "DISCRETA"
        tipo_match = re.search(r'tipo:\s*["\'](\w+)["\']', html)
        if tipo_match:
            cert["barrier_type"] = tipo_match.group(1)
        
        # =====================
        # 6. CALCOLA CAMPI DERIVATI
        # =====================
        for underlying in cert["underlyings"]:
            strike = underlying.get("strike")
            if strike and strike > 0:
                # Barriera assoluta
                if cert["barrier_down"]:
                    underlying["barrier"] = round(strike * (cert["barrier_down"] / 100), 2)
                
                # Trigger assoluto (se trigger è in percentuale)
                if cert["trigger"]:
                    # Se trigger < 2, è probabilmente in formato 0.75 = 75%
                    trigger_pct = cert["trigger"] if cert["trigger"] > 1 else cert["trigger"] * 100
                    underlying["trigger_level"] = round(strike * (trigger_pct / 100), 2)
        
        # =====================
        # 7. COSTRUISCI NOME
        # =====================
        if not cert["name"] and cert["type"] and cert["underlyings"]:
            names = ", ".join([u["name"] for u in cert["underlyings"][:3]])
            cert["name"] = f"{cert['type']} su {names}"
        elif not cert["name"]:
            cert["name"] = f"{cert['type']} - {isin}" if cert["type"] else isin
        
        return cert
        
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return None


def get_certificate_list(page):
    """Ottiene lista certificati"""
    print("📋 Fetching certificate list...")
    certificates = []
    
    try:
        page.goto(CONFIG["list_url"], timeout=CONFIG["page_timeout"])
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2000)
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        for table in soup.find_all('table'):
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 5:
                    isin = cells[0].get_text(strip=True)
                    
                    if len(isin) == 12 and re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                        underlying_type = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                        
                        if is_target_underlying(underlying_type):
                            certificates.append({
                                "isin": isin,
                                "name": cells[1].get_text(strip=True),
                                "issuer": cells[2].get_text(strip=True),
                                "underlying_type": underlying_type,
                                "market": cells[4].get_text(strip=True) if len(cells) > 4 else ""
                            })
        
        print(f"  ✅ Found {len(certificates)} target certificates")
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
    
    return certificates


def main():
    print("=" * 60)
    print("🚀 Certificates Scraper - COMPLETE STATIC DATA")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    all_certificates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()
        
        try:
            cert_list = get_certificate_list(page)
            
            if not cert_list:
                print("❌ No certificates found!")
                return
            
            cert_list = cert_list[:CONFIG["max_certificates"]]
            print(f"\n📊 Processing {len(cert_list)} certificates...\n")
            
            for i, item in enumerate(cert_list, 1):
                print(f"[{i}/{len(cert_list)}] {item['isin']} - {item['name'][:40]}")
                
                cert = extract_certificate_data(page, item["isin"], item)
                
                if cert:
                    all_certificates.append(cert)
                    # Debug: mostra dati estratti
                    und_count = len(cert.get("underlyings", []))
                    barrier = cert.get("barrier_down", "N/A")
                    print(f"    ✅ {und_count} underlyings, barrier={barrier}%")
                    
                    if cert["underlyings"]:
                        for u in cert["underlyings"]:
                            print(f"       - {u['name']}: strike={u.get('strike')}, barrier_abs={u.get('barrier')}")
                else:
                    print(f"    ❌ Failed")
                
                print()
                time.sleep(0.5)
            
        finally:
            browser.close()
    
    # Salva output
    output = {
        "metadata": {
            "scraper_version": "15.0",
            "timestamp": datetime.now().isoformat(),
            "source": "certificatiederivati.it",
            "total_certificates": len(all_certificates),
            "note": "Static data - spot prices not available from source"
        },
        "certificates": all_certificates
    }
    
    with open(CONFIG["output_file"], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("=" * 60)
    print("📊 COMPLETED")
    print(f"  📦 Total: {len(all_certificates)} certificates")
    print(f"  💾 Saved to: {CONFIG['output_file']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
