#!/usr/bin/env python3
"""
Certificates Scraper - certificatiederivati.it
Version 13 - URL corretto per nuove emissioni

Estrae dati certificati dalla pagina nuove emissioni.
"""

import json
import re
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Configurazione
CONFIG = {
    "base_url": "https://www.certificatiederivati.it",
    "list_url": "https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp",
    "detail_url": "https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin=",
    "max_certificates": 150,
    "page_timeout": 30000,
    "output_file": "certificates-data.json"
}

# Sottostanti target (indici, commodities, valute - NO singole azioni)
TARGET_UNDERLYINGS = [
    # Tipi generici
    "BASKET DI INDICI", "INDICI WORST OF", "BASKET INDICI",
    # Indici europei
    "FTSE MIB", "FTSEMIB", "MIB",
    "EURO STOXX", "EUROSTOXX", "SX5E", "STOXX50", "STOXX 50",
    "SELECT DIVIDEND",
    "DAX", "CAC 40", "CAC40", "IBEX",
    # Indici USA
    "S&P 500", "S&P500", "SPX",
    "NASDAQ", "NDX", "NASDAQ 100",
    "DOW JONES", "DJIA",
    # Commodities
    "ORO", "GOLD", "XAU",
    "ARGENTO", "SILVER", "XAG",
    "PETROLIO", "OIL", "WTI", "BRENT",
    "GAS", "NATURAL GAS",
    # Tassi / Valute
    "EURIBOR", "EUR/USD", "EURUSD",
    # Altri indici
    "NIKKEI", "HANG SENG", "RUSSELL"
]

def is_target_underlying(text):
    """Verifica se il testo contiene un sottostante target"""
    if not text:
        return False
    text_upper = text.upper()
    for target in TARGET_UNDERLYINGS:
        if target.upper() in text_upper:
            return True
    return False


def extract_barrier_from_js(html_content):
    """Estrae i dati barriera dal JavaScript inline"""
    barrier_data = {
        "percentage": None,
        "level": None,
        "type": None,
        "reached": False
    }
    
    # Pattern per barriera: "50&nbsp;%" o "50 %"
    barrier_match = re.search(r'barriera:\s*["\'](\d+(?:[.,]\d+)?)\s*(?:&nbsp;)?%["\']', html_content)
    if barrier_match:
        barrier_data["percentage"] = float(barrier_match.group(1).replace(',', '.'))
    
    # Pattern per livello: "665,855" o "665.855"
    level_match = re.search(r'livello:\s*["\'](\d+(?:[.,]\d+)?)["\']', html_content)
    if level_match:
        barrier_data["level"] = float(level_match.group(1).replace(',', '.'))
    
    # Pattern per tipo: "DISCRETA" o "CONTINUA"
    type_match = re.search(r'tipo:\s*["\'](\w+)["\']', html_content)
    if type_match:
        barrier_data["type"] = type_match.group(1)
    
    # Pattern per raggiunta: "true" o "false"
    reached_match = re.search(r'raggiunta:\s*["\']?(true|false)["\']?', html_content, re.IGNORECASE)
    if reached_match:
        barrier_data["reached"] = reached_match.group(1).lower() == "true"
    
    return barrier_data


def extract_certificate_data(page, isin, list_data=None):
    """Estrae tutti i dati da una pagina certificato"""
    url = f"{CONFIG['detail_url']}{isin}"
    
    try:
        page.goto(url, timeout=CONFIG["page_timeout"])
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1500)
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        cert = {
            "isin": isin,
            "name": list_data.get("name", "") if list_data else "",
            "type": "",
            "issuer": list_data.get("issuer", "") if list_data else "",
            "underlying_type": list_data.get("underlying_type", "") if list_data else "",
            "market": list_data.get("market", "") if list_data else "",
            "currency": "EUR",
            "issue_date": list_data.get("date", "") if list_data else "",
            "maturity_date": "",
            "barrier_down": None,
            "barrier_type": "",
            "trigger": None,
            "nominal": 1000,
            "underlyings": [],
            "source": "certificatiederivati.it",
            "scraped_at": datetime.now().isoformat()
        }
        
        # 1. TIPO CERTIFICATO - dall'header del panel principale
        type_header = soup.find('h3', class_='panel-title')
        if type_header:
            cert["type"] = type_header.get_text(strip=True)
        
        # 2. TABELLA PRINCIPALE (ISIN, Mercato, Date)
        for table in soup.find_all('table', class_='table'):
            rows = table.find_all('tr')
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    label = th.get_text(strip=True).upper()
                    value = td.get_text(strip=True)
                    
                    if "MERCATO" in label:
                        cert["market"] = value
                    elif "DATA EMISSIONE" in label:
                        cert["issue_date"] = value
                    elif "DATA SCADENZA" in label or "SCADENZA" in label:
                        cert["maturity_date"] = value
                    elif "VALUTA" in label and "DIVISA" not in label:
                        cert["currency"] = value
                    elif "NOMINALE" in label:
                        try:
                            cert["nominal"] = float(value.replace('.', '').replace(',', '.'))
                        except:
                            pass
                    elif "TRIGGER" in label:
                        try:
                            cert["trigger"] = float(value.replace(',', '.'))
                        except:
                            pass
        
        # 3. EMITTENTE - dalla sezione "Scheda Emittente"
        emittente_panel = soup.find('h3', string=re.compile(r'Scheda Emittente', re.IGNORECASE))
        if emittente_panel:
            parent_panel = emittente_panel.find_parent('div', class_='panel')
            if parent_panel:
                table = parent_panel.find('table')
                if table:
                    for row in table.find_all('tr'):
                        td = row.find('td')
                        if td:
                            text = td.get_text(strip=True)
                            if text and "Rating" not in text and "@" not in text and "http" not in text.lower() and len(text) < 50:
                                cert["issuer"] = text
                                break
        
        # 4. SOTTOSTANTI - dalla sezione "Scheda Sottostante"
        sottostante_panel = soup.find('h3', string=re.compile(r'Scheda Sottostante', re.IGNORECASE))
        if sottostante_panel:
            header_text = sottostante_panel.get_text(strip=True)
            if "Basket" in header_text:
                cert["underlying_type"] = "Basket"
            
            parent_panel = sottostante_panel.find_parent('div', class_='panel')
            if parent_panel:
                table = parent_panel.find('table')
                if table:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            underlying = {
                                "name": cells[0].get_text(strip=True),
                                "strike": None,
                                "weight": None
                            }
                            if len(cells) >= 2:
                                try:
                                    strike_text = cells[1].get_text(strip=True).replace('.', '').replace(',', '.')
                                    underlying["strike"] = float(strike_text) if strike_text else None
                                except:
                                    pass
                            if len(cells) >= 3:
                                weight_text = cells[2].get_text(strip=True)
                                if weight_text and weight_text != '\xa0':
                                    try:
                                        underlying["weight"] = float(weight_text.replace('%', '').replace(',', '.'))
                                    except:
                                        pass
                            
                            if underlying["name"]:
                                cert["underlyings"].append(underlying)
        
        # 5. BARRIERA - dal JavaScript inline
        barrier_data = extract_barrier_from_js(html)
        if barrier_data["percentage"]:
            cert["barrier_down"] = barrier_data["percentage"]
        if barrier_data["type"]:
            cert["barrier_type"] = barrier_data["type"]
        cert["barrier_reached"] = barrier_data["reached"]
        
        # 6. NOME - costruiscilo se non presente
        if not cert["name"] and cert["type"] and cert["underlyings"]:
            underlying_names = ", ".join([u["name"] for u in cert["underlyings"][:3]])
            cert["name"] = f"{cert['type']} su {underlying_names}"
        elif not cert["name"] and cert["type"]:
            cert["name"] = f"{cert['type']} - {isin}"
        elif not cert["name"]:
            cert["name"] = isin
        
        return cert
        
    except Exception as e:
        print(f"    ❌ Error extracting {isin}: {e}")
        return None


def get_certificate_list(page):
    """Ottiene la lista dei certificati dalla pagina nuove emissioni"""
    print("📋 Fetching certificate list from nuove emissioni...")
    
    certificates = []
    
    try:
        page.goto(CONFIG["list_url"], timeout=CONFIG["page_timeout"])
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2000)
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Trova tutte le tabelle
        tables = soup.find_all('table')
        print(f"  📊 Found {len(tables)} tables")
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 5:
                    # Prima cella dovrebbe essere ISIN
                    isin_text = cells[0].get_text(strip=True)
                    
                    # Verifica che sia un ISIN valido
                    if len(isin_text) == 12 and re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin_text):
                        cert_data = {
                            "isin": isin_text,
                            "name": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                            "issuer": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                            "underlying_type": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                            "market": cells[4].get_text(strip=True) if len(cells) > 4 else "",
                            "date": cells[5].get_text(strip=True) if len(cells) > 5 else ""
                        }
                        certificates.append(cert_data)
        
        print(f"  ✅ Found {len(certificates)} certificates in tables")
        
    except Exception as e:
        print(f"  ❌ Error fetching list: {e}")
    
    return certificates


def main():
    """Main function"""
    print("=" * 60)
    print("🚀 Certificates Scraper - certificatiederivati.it v13")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    all_certificates = []
    filtered_certificates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()
        
        try:
            # 1. Ottieni lista certificati dalla pagina nuove emissioni
            cert_list = get_certificate_list(page)
            
            if not cert_list:
                print("❌ No certificates found in list!")
                return
            
            # Pre-filtra per sottostanti target dalla lista
            target_certs = []
            for cert in cert_list:
                underlying_type = cert.get("underlying_type", "")
                # Includi "Basket di indici" ma escludi "Basket di azioni" e "Singolo Sottostante"
                if is_target_underlying(underlying_type) or "indici" in underlying_type.lower():
                    target_certs.append(cert)
                    print(f"  🎯 Target: {cert['isin']} - {cert['name']} ({underlying_type})")
            
            print(f"\n📊 Pre-filtered: {len(target_certs)} certificates with target underlyings")
            
            # Limita al massimo configurato
            target_certs = target_certs[:CONFIG["max_certificates"]]
            
            if not target_certs:
                print("⚠️ No certificates with target underlyings found!")
                # Fallback: prendi i primi N certificati comunque
                target_certs = cert_list[:min(50, len(cert_list))]
                print(f"  📊 Fallback: using first {len(target_certs)} certificates")
            
            print(f"\n📊 Processing {len(target_certs)} certificates...")
            
            # 2. Estrai dati dettagliati per ogni certificato
            for i, cert in enumerate(target_certs, 1):
                print(f"\n[{i}/{len(target_certs)}] {cert['isin']} - {cert['name'][:40]}...")
                
                cert_data = extract_certificate_data(page, cert["isin"], cert)
                
                if cert_data:
                    all_certificates.append(cert_data)
                    
                    # Verifica finale se ha sottostanti target
                    has_target = False
                    
                    # Check underlying_type dalla lista
                    if is_target_underlying(cert.get("underlying_type", "")):
                        has_target = True
                    
                    # Check i singoli sottostanti estratti
                    for underlying in cert_data.get("underlyings", []):
                        if is_target_underlying(underlying.get("name", "")):
                            has_target = True
                            break
                    
                    if has_target:
                        filtered_certificates.append(cert_data)
                        print(f"    ✅ Included")
                    else:
                        # Includi comunque se era pre-filtrato
                        filtered_certificates.append(cert_data)
                        print(f"    ✅ Included (pre-filtered)")
                
                # Pausa per non sovraccaricare il server
                page.wait_for_timeout(500)
            
        finally:
            browser.close()
    
    # 3. Genera output
    output = {
        "metadata": {
            "scraper_version": "13.0",
            "timestamp": datetime.now().isoformat(),
            "source": "certificatiederivati.it",
            "source_page": "db_bs_nuove_emissioni.asp",
            "total_scraped": len(all_certificates),
            "total_filtered": len(filtered_certificates),
            "filter_criteria": "Indici, Commodities, Valute (no single stocks)"
        },
        "certificates": filtered_certificates
    }
    
    # Salva output
    with open(CONFIG["output_file"], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 60)
    print("📊 SCRAPING COMPLETED")
    print(f"  📦 Total scraped: {len(all_certificates)}")
    print(f"  🎯 Filtered: {len(filtered_certificates)}")
    print(f"  💾 Saved to: {CONFIG['output_file']}")
    print("=" * 60)
    
    # Statistiche per tipo
    type_stats = {}
    for cert in filtered_certificates:
        t = cert.get("type", "Unknown")
        type_stats[t] = type_stats.get(t, 0) + 1
    
    if type_stats:
        print("\n📈 Distribution by type:")
        for t, count in sorted(type_stats.items(), key=lambda x: -x[1])[:10]:
            print(f"  - {t}: {count}")
    
    # Statistiche per emittente
    issuer_stats = {}
    for cert in filtered_certificates:
        i = cert.get("issuer", "Unknown")
        if i:
            issuer_stats[i] = issuer_stats.get(i, 0) + 1
    
    if issuer_stats:
        print("\n🏛️ Distribution by issuer:")
        for i, count in sorted(issuer_stats.items(), key=lambda x: -x[1])[:10]:
            print(f"  - {i}: {count}")


if __name__ == "__main__":
    main()
