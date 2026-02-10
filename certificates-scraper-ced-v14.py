#!/usr/bin/env python3
"""
Certificates Scraper v14 - CED + Borsa Italiana
Lista da CED, Prezzi da Borsa Italiana

FILTRI: Solo Indici, Valute, Commodities, Tassi, Credit Linked
NO AZIONI SINGOLE
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
    "ced_list_url": "https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp",
    "ced_detail_url": "https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin=",
    "borsa_detail_url": "https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/",
    "max_certificates": 150,
    "page_timeout": 30000,
    "output_file": "certificates-data.json"
}

# ===================================
# FILTRI SOTTOSTANTI
# ===================================

# INCLUDI: Indici, Valute, Commodities, Tassi, Credit Linked
TARGET_KEYWORDS = [
    # Indici
    "indic", "index", "ftse", "mib", "stoxx", "euro stoxx", "eurostoxx",
    "dax", "cac", "ibex", "s&p", "nasdaq", "dow jones", "nikkei", "hang seng",
    "russell", "msci", "topix", "kospi", "sensex", "bovespa",
    # Commodities  
    "gold", "oro", "silver", "argento", "oil", "petrolio", "wti", "brent",
    "natural gas", "gas naturale", "copper", "rame", "platinum", "platino",
    "palladium", "palladio", "commodity", "commodities", "materie prime",
    "wheat", "grano", "corn", "mais", "coffee", "caffe", "sugar", "zucchero",
    # Valute
    "eur/", "/eur", "usd/", "/usd", "forex", "currency", "valut", "cambio",
    "dollar", "yen", "pound", "sterlina", "franco svizzero", "chf",
    # Tassi
    "euribor", "libor", "bund", "btp", "treasury", "tasso", "rate", "interest",
    "oat", "gilt", "swap", "yield",
    # Credit Linked
    "credit", "cds", "default", "credit linked",
    # Basket generici su indici
    "basket di indici", "worst of", "best of"
]

# ESCLUDI: Azioni singole (italiane e estere)
EXCLUDE_KEYWORDS = [
    # Azioni Italiane
    "unicredit", "intesa", "sanpaolo", "enel", "eni", "generali", "ferrari",
    "stellantis", "stmicroelectronics", "stm", "telecom", "tim", "leonardo",
    "pirelli", "moncler", "campari", "mediobanca", "fineco", "finecobank",
    "poste", "snam", "terna", "recordati", "amplifon", "diasorin", "nexi",
    "prysmian", "tenaris", "saipem", "inwit", "hera", "a2a", "italgas",
    "banco bpm", "bper", "unipol", "azimut", "banca mediolanum",
    # Azioni USA
    "tesla", "apple", "amazon", "nvidia", "microsoft", "alphabet", "google",
    "meta", "facebook", "netflix", "amd", "intel", "qualcomm", "broadcom",
    "adobe", "salesforce", "oracle", "ibm", "cisco", "paypal", "visa",
    "mastercard", "jpmorgan", "goldman", "morgan stanley", "bank of america",
    "wells fargo", "citigroup", "boeing", "lockheed", "raytheon",
    "exxon", "chevron", "conocophillips", "pfizer", "johnson", "merck",
    "abbvie", "eli lilly", "moderna", "coca cola", "pepsi", "mcdonalds",
    "walmart", "home depot", "nike", "disney", "uber", "airbnb",
    # Azioni Europee
    "lvmh", "asml", "sap", "siemens", "allianz", "basf", "bayer", "volkswagen",
    "daimler", "bmw", "deutsche bank", "commerzbank", "total", "sanofi",
    "bnp paribas", "axa", "loreal", "hermes", "kering", "airbus",
    "nestle", "novartis", "roche", "ubs", "credit suisse", "zurich",
    "shell", "bp", "hsbc", "barclays", "vodafone", "glaxo", "astrazeneca",
    "unilever", "rio tinto", "bhp"
]


def is_target_certificate(text):
    """Verifica se il certificato ha sottostanti target (no azioni)"""
    if not text:
        return False
    text_lower = text.lower()
    
    # Prima verifica esclusioni (azioni singole)
    for exc in EXCLUDE_KEYWORDS:
        if exc in text_lower:
            # Eccezione: "basket" o "worst of" con azioni puo essere ok
            if "basket" not in text_lower and "worst of" not in text_lower:
                return False
    
    # Poi verifica inclusioni
    for target in TARGET_KEYWORDS:
        if target in text_lower:
            return True
    
    return False


def parse_number(text):
    """Converte stringa in numero (formato italiano)"""
    if not text or text.strip() in ['N.A.', 'N.D.', '-', '', 'N/A']:
        return None
    try:
        cleaned = text.strip().upper()
        cleaned = re.sub(r'[EUR\u20ac%\s\xa0]', '', cleaned)
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        return float(cleaned)
    except:
        return None


def parse_date(text):
    """Converte data in ISO format"""
    if not text or text.strip() in ['N.A.', 'N.D.', '-', '', 'Open End', '01/01/1900']:
        return None
    try:
        if '/' in text:
            parts = text.strip().split('/')
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return text.strip()
    except:
        return text


def get_price_from_borsa_italiana(page, isin):
    """Ottiene prezzo da Borsa Italiana"""
    url = f"{CONFIG['borsa_detail_url']}{isin}.html"
    
    try:
        page.goto(url, timeout=15000)
        page.wait_for_load_state("domcontentloaded")
        time.sleep(1)
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        prices = {
            "reference_price": None,
            "day_low": None,
            "day_high": None
        }
        
        for table in soup.find_all('table'):
            for row in table.find_all('tr'):
                cells = row.find_all(['th', 'td'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    
                    if 'reference' in label or 'riferimento' in label:
                        prices["reference_price"] = parse_number(value)
                    elif 'day low' in label or 'minimo' in label:
                        prices["day_low"] = parse_number(value)
                    elif 'day high' in label or 'massimo' in label:
                        prices["day_high"] = parse_number(value)
        
        return prices
        
    except Exception as e:
        return None


def extract_certificate_from_ced(page, isin, list_data=None):
    """Estrae dati certificato da CED"""
    url = f"{CONFIG['ced_detail_url']}{isin}"
    
    try:
        page.goto(url, timeout=CONFIG["page_timeout"])
        try:
            page.wait_for_selector("table.table", timeout=5000)
        except:
            pass
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        cert = {
            "isin": isin,
            "name": list_data.get("name", "") if list_data else "",
            "type": "",
            "issuer": list_data.get("issuer", "") if list_data else "",
            "market": list_data.get("market", "SeDeX") if list_data else "SeDeX",
            "currency": "EUR",
            "underlying": list_data.get("underlying", "") if list_data else "",
            "nominal": 1000,
            "issue_date": None,
            "maturity_date": None,
            "strike_date": None,
            "barrier_down": None,
            "barrier_type": None,
            "reference_price": None,
            "day_low": None,
            "day_high": None,
            "underlyings": [],
            "source": "certificatiederivati.it + borsaitaliana.it",
            "scraped_at": datetime.now().isoformat()
        }
        
        # Tipo certificato
        type_header = soup.find('h3', class_='panel-title')
        if type_header:
            cert["type"] = type_header.get_text(strip=True)
        
        # Tabelle dati
        for table in soup.find_all('table', class_='table'):
            for row in table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    label = th.get_text(strip=True).upper()
                    value = td.get_text(strip=True)
                    
                    if "MERCATO" in label and not cert["market"]:
                        cert["market"] = value
                    elif "DATA EMISSIONE" in label:
                        cert["issue_date"] = parse_date(value)
                    elif "DATA SCADENZA" in label:
                        cert["maturity_date"] = parse_date(value)
                    elif "DATA STRIKE" in label:
                        cert["strike_date"] = parse_date(value)
                    elif "NOMINALE" in label:
                        cert["nominal"] = parse_number(value) or 1000
                    elif "VALUTA" in label and "DIVISA" not in label:
                        cert["currency"] = value
        
        # Emittente
        emittente_header = soup.find('h3', string=re.compile(r'Scheda Emittente', re.IGNORECASE))
        if emittente_header:
            panel = emittente_header.find_parent('div', class_='panel')
            if panel:
                first_td = panel.find('td')
                if first_td:
                    issuer_text = first_td.get_text(strip=True)
                    if issuer_text and "Rating" not in issuer_text and "@" not in issuer_text:
                        cert["issuer"] = issuer_text
        
        # Sottostanti
        sottostante_header = soup.find('h3', string=re.compile(r'Scheda Sottostante', re.IGNORECASE))
        if sottostante_header:
            header_text = sottostante_header.get_text(strip=True)
            if "(" in header_text and ")" in header_text:
                cert["underlying"] = header_text.split("(")[1].split(")")[0].strip()
            
            panel = sottostante_header.find_parent('div', class_='panel')
            if panel:
                table = panel.find('table')
                if table:
                    rows = table.find_all('tr')
                    for row in rows[1:]:
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            name = cells[0].get_text(strip=True)
                            if name and name.upper() not in ["DESCRIZIONE", ""]:
                                strike = parse_number(cells[1].get_text(strip=True))
                                cert["underlyings"].append({
                                    "name": name,
                                    "strike": strike,
                                    "barrier": None
                                })
        
        # Barriera dal JavaScript
        barrier_match = re.search(r'barriera:\s*["\'](\d+(?:[.,]\d+)?)\s*(?:&nbsp;)?%["\']', html)
        if barrier_match:
            cert["barrier_down"] = parse_number(barrier_match.group(1))
        
        tipo_match = re.search(r'tipo:\s*["\'](\w+)["\']', html)
        if tipo_match:
            cert["barrier_type"] = tipo_match.group(1)
        
        # Calcola barriere assolute
        if cert["barrier_down"]:
            for und in cert["underlyings"]:
                if und.get("strike"):
                    und["barrier"] = round(und["strike"] * (cert["barrier_down"] / 100), 2)
        
        # Costruisci nome se mancante
        if not cert["name"] and cert["type"] and cert["underlyings"]:
            names = ", ".join([u["name"] for u in cert["underlyings"][:3]])
            cert["name"] = f"{cert['type']} su {names}"
        
        return cert
        
    except Exception as e:
        print(f"    CED Error: {e}")
        return None


def get_certificate_list_from_ced(page):
    """Ottiene lista certificati da CED con filtro sottostanti"""
    print("Fetching list from certificatiederivati.it...")
    print("   Filtro: Indici, Commodities, Valute, Tassi, Credit Linked")
    
    certificates = []
    
    try:
        page.goto(CONFIG["ced_list_url"], timeout=CONFIG["page_timeout"])
        page.wait_for_load_state("networkidle")
        time.sleep(2)
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        for table in soup.find_all('table'):
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 5:
                    isin = cells[0].get_text(strip=True)
                    
                    if len(isin) == 12 and re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                        name = cells[1].get_text(strip=True)
                        issuer = cells[2].get_text(strip=True)
                        underlying = cells[3].get_text(strip=True)
                        market = cells[4].get_text(strip=True) if len(cells) > 4 else "SeDeX"
                        
                        # FILTRO: Solo target sottostanti
                        combined_text = f"{name} {underlying}"
                        if is_target_certificate(combined_text):
                            certificates.append({
                                "isin": isin,
                                "name": name,
                                "issuer": issuer,
                                "underlying": underlying,
                                "market": market
                            })
        
        print(f"   Found {len(certificates)} certificates matching filters")
        
    except Exception as e:
        print(f"   Error: {e}")
    
    return certificates


def main():
    print("=" * 65)
    print("Certificates Scraper v14")
    print("   Fonte: CED + Borsa Italiana")
    print("   Filtri: Indici, Commodities, Valute, Tassi, Credit Linked")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)
    
    all_certificates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        try:
            # 1. Lista da CED (gia filtrata)
            cert_list = get_certificate_list_from_ced(page)
            
            if not cert_list:
                print("No certificates found!")
                return
            
            cert_list = cert_list[:CONFIG["max_certificates"]]
            print(f"\nProcessing {len(cert_list)} certificates...\n")
            
            # 2. Per ogni certificato: dati da CED + prezzi da Borsa Italiana
            for i, item in enumerate(cert_list, 1):
                print(f"[{i}/{len(cert_list)}] {item['isin']}...", end=" ")
                
                # Dati base da CED
                cert = extract_certificate_from_ced(page, item["isin"], item)
                
                if cert:
                    # Prezzi da Borsa Italiana
                    prices = get_price_from_borsa_italiana(page, item["isin"])
                    if prices:
                        cert["reference_price"] = prices.get("reference_price")
                        cert["day_low"] = prices.get("day_low")
                        cert["day_high"] = prices.get("day_high")
                    
                    all_certificates.append(cert)
                    
                    price_str = f"{cert['reference_price']}EUR" if cert['reference_price'] else "N/A"
                    barrier_str = f"{cert['barrier_down']}%" if cert['barrier_down'] else "N/A"
                    print(f"OK Price: {price_str} | Barrier: {barrier_str}")
                else:
                    print("Failed")
                
                time.sleep(0.5)
            
        finally:
            browser.close()
    
    # Salva output
    output = {
        "metadata": {
            "version": "14.0",
            "timestamp": datetime.now().isoformat(),
            "source": "certificatiederivati.it + borsaitaliana.it",
            "total_certificates": len(all_certificates),
            "filter": "Indices, Commodities, Currencies, Rates, Credit Linked (NO single stocks)"
        },
        "certificates": all_certificates
    }
    
    with open(CONFIG["output_file"], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 65)
    print("COMPLETED")
    print(f"   Certificates saved: {len(all_certificates)}")
    print(f"   Output file: {CONFIG['output_file']}")
    print("=" * 65)


if __name__ == "__main__":
    main()
