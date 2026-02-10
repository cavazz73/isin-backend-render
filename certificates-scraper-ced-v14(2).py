#!/usr/bin/env python3
"""
Certificates Scraper v14 - ROBUSTO
Multi-source con fallback + retry

FILTRI: Solo Indici, Valute, Commodities, Tassi
NO AZIONI SINGOLE
"""

import json
import re
import time
import urllib.request
import urllib.error
import ssl
from datetime import datetime

# Disabilita verifica SSL per compatibilità
ssl._create_default_https_context = ssl._create_unverified_context

# ===================================
# LISTA ISIN REALI - Certificati su Indici/Commodities/Valute
# Presi dal tuo file esistente + altri noti
# ===================================

CERTIFICATES_SEED = [
    # Dal tuo file esistente (filtrati per indici/commodities)
    {"isin": "CH1327224759", "name": "Phoenix Memory su WTI Crude, Euro Stoxx 50, S&P 500"},
    {"isin": "XS2745896321", "name": "Twin Win Certificate su S&P 500"},
    
    # Certificati su indici - Vontobel
    {"isin": "DE000VV1GPU6", "name": "Tracker su FTSE MIB"},
    {"isin": "DE000VV5ZU05", "name": "Tracker su Euro Stoxx 50"},
    {"isin": "DE000VV6B9X4", "name": "Tracker su DAX"},
    {"isin": "DE000VV6B8Y5", "name": "Tracker su S&P 500"},
    {"isin": "DE000VV6MWW7", "name": "Tracker su Nasdaq 100"},
    
    # Certificati su commodities - Vontobel
    {"isin": "DE000VX5NXG7", "name": "Tracker su Gold"},
    {"isin": "DE000VV0LBC1", "name": "Tracker su Silver"},
    {"isin": "DE000VX90WB2", "name": "Tracker su Brent Oil"},
    {"isin": "DE000VV1XK47", "name": "Tracker su Natural Gas"},
    
    # Certificati su indici - BNP
    {"isin": "NLBNPIT1XAF4", "name": "Tracker su FTSE MIB"},
    {"isin": "NLBNPIT1XAG2", "name": "Tracker su Euro Stoxx 50"},
    
    # Certificati su indici - Societe Generale
    {"isin": "XS2314660502", "name": "Autocall su Euro Stoxx Banks"},
    {"isin": "XS2394956712", "name": "Phoenix su FTSE MIB"},
    
    # Certificati su indici - UniCredit
    {"isin": "DE000HV8F1D1", "name": "Bonus Cap su Euro Stoxx 50"},
    {"isin": "DE000HV8F1E9", "name": "Bonus Cap su DAX"},
    {"isin": "DE000HV8GYJ5", "name": "Cash Collect su FTSE MIB"},
    
    # Certificati su commodities - altri
    {"isin": "XS1073722347", "name": "Tracker su Commodity Index"},
    {"isin": "GB00B15KXV33", "name": "ETCs Gold"},
    {"isin": "JE00B1VS3770", "name": "ETC WTI Oil"},
]

CONFIG = {
    "output_file": "certificates-data.json",
    "timeout": 20,
    "retries": 2
}


def fetch_url(url, retries=CONFIG["retries"]):
    """Fetch URL con retry"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'it-IT,it;q=0.9,en;q=0.8'
    }
    
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=CONFIG["timeout"]) as response:
                return response.read().decode('utf-8', errors='ignore')
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            return None
    return None


def parse_number(text):
    """Converte stringa in numero"""
    if not text or text.strip().upper() in ['N.A.', 'N.D.', '-', '', 'N/A', '--', 'ND']:
        return None
    try:
        cleaned = re.sub(r'[EUR\u20ac%\s\xa0]', '', text.strip())
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        return round(float(cleaned), 4)
    except:
        return None


def extract_field(html, patterns):
    """Estrae campo usando lista di pattern regex"""
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
        if match:
            value = match.group(1).strip()
            value = re.sub(r'<[^>]+>', '', value).strip()
            return value
    return None


def get_from_borsa_italiana(isin):
    """Prova Borsa Italiana"""
    url = f"https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{isin}.html"
    html = fetch_url(url)
    if not html or "Page Not Found" in html or "404" in html:
        return None
    
    data = {"source": "borsaitaliana.it"}
    
    # Nome
    name = extract_field(html, [r'<h1[^>]*>([^<]+)</h1>', r'<title>([^<]+)</title>'])
    if name:
        data["name"] = name.split(' - ')[0].strip()
    
    # Prezzo
    price = extract_field(html, [
        r'Riferimento[^<]*</t[hd]>\s*<td[^>]*>([^<]+)',
        r'Reference[^<]*</t[hd]>\s*<td[^>]*>([^<]+)',
        r'Ultimo[^<]*</t[hd]>\s*<td[^>]*>([^<]+)'
    ])
    if price:
        data["reference_price"] = parse_number(price)
    
    # Emittente
    issuer = extract_field(html, [
        r'Emittente[^<]*</t[hd]>\s*<td[^>]*>([^<]+)',
        r'Issuer[^<]*</t[hd]>\s*<td[^>]*>([^<]+)'
    ])
    if issuer:
        data["issuer"] = issuer
    
    # Sottostante
    underlying = extract_field(html, [
        r'Sottostante[^<]*</t[hd]>\s*<td[^>]*>([^<]+)',
        r'Underlying[^<]*</t[hd]>\s*<td[^>]*>([^<]+)'
    ])
    if underlying:
        data["underlying"] = underlying
    
    return data if len(data) > 1 else None


def get_from_justetf(isin):
    """Prova JustETF per ETC/ETN"""
    url = f"https://www.justetf.com/it/etf-profile.html?isin={isin}"
    html = fetch_url(url)
    if not html or "non trovato" in html.lower():
        return None
    
    data = {"source": "justetf.com"}
    
    name = extract_field(html, [r'<h1[^>]*>([^<]+)</h1>'])
    if name:
        data["name"] = name
    
    price = extract_field(html, [r'<span[^>]*class="[^"]*val[^"]*"[^>]*>([0-9,.]+)'])
    if price:
        data["reference_price"] = parse_number(price)
    
    return data if len(data) > 1 else None


def enrich_certificate(seed_data):
    """Arricchisce dati certificato da fonti multiple"""
    isin = seed_data["isin"]
    
    cert = {
        "isin": isin,
        "name": seed_data.get("name", ""),
        "type": "",
        "issuer": "",
        "market": "SeDeX",
        "currency": "EUR",
        "underlying": "",
        "reference_price": None,
        "barrier_pct": None,
        "expiry_date": None,
        "source": "seed",
        "scraped_at": datetime.now().isoformat()
    }
    
    # Prova Borsa Italiana
    bi_data = get_from_borsa_italiana(isin)
    if bi_data:
        for k, v in bi_data.items():
            if v and (not cert.get(k) or k == "source"):
                cert[k] = v
        return cert
    
    # Prova JustETF (per ETC)
    etf_data = get_from_justetf(isin)
    if etf_data:
        for k, v in etf_data.items():
            if v and (not cert.get(k) or k == "source"):
                cert[k] = v
        return cert
    
    # Usa dati seed
    cert["source"] = "seed_only"
    return cert


def is_target_underlying(text):
    """Verifica se sottostante è target (no azioni singole)"""
    if not text:
        return True  # Se non sappiamo, includiamo
    
    t = text.lower()
    
    # Esclusioni esplicite (azioni singole)
    excludes = ["tesla", "nvidia", "apple", "amazon", "microsoft", "meta",
                "intesa", "unicredit", "enel", "eni", "generali", "ferrari"]
    for exc in excludes:
        if exc in t:
            return False
    
    return True


def main():
    print("=" * 60)
    print("Certificates Scraper v14 - Multi-Source")
    print("Filtri: Indici, Commodities, Valute, Tassi (NO azioni)")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    certificates = []
    success = 0
    partial = 0
    
    print(f"\nProcessing {len(CERTIFICATES_SEED)} certificates...\n")
    
    for i, seed in enumerate(CERTIFICATES_SEED, 1):
        isin = seed["isin"]
        print(f"[{i}/{len(CERTIFICATES_SEED)}] {isin}...", end=" ", flush=True)
        
        cert = enrich_certificate(seed)
        
        # Filtra per sottostante
        underlying = cert.get("underlying", "") or cert.get("name", "")
        if not is_target_underlying(underlying):
            print("SKIP (single stock)")
            continue
        
        # Verifica qualità dati
        has_price = cert.get("reference_price") is not None
        has_name = bool(cert.get("name"))
        
        if has_price:
            success += 1
            status = f"OK - {cert['reference_price']} EUR"
        elif has_name:
            partial += 1
            status = "PARTIAL (no price)"
        else:
            status = "MINIMAL"
        
        certificates.append(cert)
        name_short = (cert.get("name", "") or underlying)[:30]
        print(f"{status} - {name_short}")
        
        time.sleep(0.3)
    
    # Output
    output = {
        "metadata": {
            "version": "14.0",
            "timestamp": datetime.now().isoformat(),
            "sources": "borsaitaliana.it, justetf.com, seed",
            "total": len(certificates),
            "with_price": success,
            "partial": partial,
            "filter": "Indices, Commodities, Currencies, Rates (NO single stocks)"
        },
        "certificates": certificates
    }
    
    with open(CONFIG["output_file"], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 60)
    print(f"COMPLETED")
    print(f"  Total saved: {len(certificates)}")
    print(f"  With price: {success}")
    print(f"  Partial: {partial}")
    print(f"  File: {CONFIG['output_file']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
