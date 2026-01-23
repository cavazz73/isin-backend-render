import asyncio
import json
import re
import os
import argparse
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURAZIONE ---
TARGET_COUNT = 80
OUTPUT_FILE = "data/certificates-data.json"

# --- FILTRO ASSET CLASS ---
VALID_KEYWORDS = [
    "EURO STOXX", "EUROSTOXX", "S&P", "SP500", "NASDAQ", "DOW JONES", "FTSE", "DAX", 
    "CAC", "IBEX", "NIKKEI", "HANG SENG", "MSCI", "STOXX", "BANKS", "AUTOMOBILES", 
    "INSURANCE", "UTILITIES", "ENERGY", "TECH", "MIB", "LEONARDO", "ENI", "ENEL", "INTESA",
    "GOLD", "ORO", "SILVER", "ARGENTO", "OIL", "PETROLIO", "BRENT", "WTI", "GAS", 
    "COPPER", "RAME", "PALLADIUM", "PLATINUM", "WHEAT", "CORN", "NATURAL GAS",
    "EUR/", "USD/", "/EUR", "/USD", "EURIBOR", "CMS", "IRS", "SOFR", "ESTR", 
    "LIBOR", "T-NOTE", "BUND", "BTP", "FOREX"
]

def clean_text(text):
    if not text: return "N/D"
    return re.sub(r'\s+', ' ', text).strip()

def parse_float(text):
    if not text: return 0.0
    text = text.upper().replace('EUR', '').replace('€', '').replace('%', '').strip()
    if ',' in text and '.' in text: text = text.replace('.', '').replace(',', '.')
    elif ',' in text: text = text.replace(',', '.')
    clean = re.sub(r'[^\d.]', '', text)
    try: return float(clean)
    except: return 0.0

def is_valid_asset(underlying_name):
    u = underlying_name.upper()
    for kw in VALID_KEYWORDS:
        if kw in u: return True
    return False

def calculate_scenarios(price, strike, barrier):
    if not price or price <= 0: return []
    if not strike or strike <= 0: strike = price
    if not barrier or barrier <= 0: barrier = strike * 0.60 
    scenarios = []
    variations = [-50, -40, -30, -20, -10, 0, 10, 20]
    for var in variations:
        underlying_sim = strike * (1 + (var / 100))
        if underlying_sim >= barrier: redemption = 100.0
        else: redemption = (underlying_sim / strike) * 100
        pl_pct = ((redemption - price) / price) * 100
        scenarios.append({
            "variation_pct": var,
            "underlying_price": round(underlying_sim, 2),
            "redemption": round(redemption, 2),
            "pl_pct": round(pl_pct, 2)
        })
    return scenarios
    # --- CORE SCRAPER ---
async def scrape_certificate(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    try:
        await page.goto(url, timeout=20000, wait_until="domcontentloaded")
        if "home.asp" in page.url: return None

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        full_text = clean_text(soup.get_text(separator=' | '))

        if "EMITTENTE" not in full_text.upper(): return None

        # 1. DATI BASE
        nome_tag = soup.find('td', class_='titolo_scheda')
        nome = clean_text(nome_tag.get_text()) if nome_tag else f"Cert {isin}"
        
        m_und = re.search(r'Sottostante[^a-zA-Z0-9]{0,15}([a-zA-Z0-9 &/\.\-\+]{3,50})', full_text, re.IGNORECASE)
        sottostante = clean_text(m_und.group(1)) if m_und else nome

        # 2. FILTRO
        if not is_valid_asset(sottostante) and not is_valid_asset(nome):
            return "SKIPPED_TYPE"

        # 3. PREZZO (Regex sicura)
        prezzo = 0.0
        regex_prezzo = r'(?:Prezzo|Ultimo|Valore|Quotazione|Ask|Lettera)[^0-9]{0,30}(\d+[.,]\d+)'
        match = re.search(regex_prezzo, full_text, re.IGNORECASE)
        if match: prezzo = parse_float(match.group(1))
        
        if prezzo <= 0: return None

        # 4. DATI TECNICI
        def get_val(kw):
            pat = kw + r'[^0-9]{0,30}(\d+[.,]\d+)'
            m = re.search(pat, full_text, re.IGNORECASE)
            return parse_float(m.group(1)) if m else 0.0

        strike = get_val("Strike|Iniziale")
        barriera = get_val("Barriera|Barrier")
        cedola = get_val("Cedola|Premio")
        
        emittente = "N/D"
        m_em = re.search(r'Emittente[^a-zA-Z0-9]{0,20}([a-zA-Z ]{3,30})', full_text, re.IGNORECASE)
        if m_em: emittente = clean_text(m_em.group(1))

        if strike == 0: strike = prezzo
        if barriera == 0: barriera = prezzo * 0.60

        return {
            "isin": isin, "name": nome, "type": "Certificate", "asset_class": "Indices/Commodities",
            "issuer": emittente, "market": "SeDeX", "currency": "EUR",
            "price": prezzo, "bid_price": prezzo, "ask_price": prezzo,
            "annual_coupon_yield": cedola, "barrier_down": barriera, "strike": strike,
            "maturity_date": (datetime.now() + timedelta(days=730)).isoformat(),
            "underlyings": [{"name": sottostante, "strike": strike, "barrier": barriera}],
            "scenario_analysis": calculate_scenarios(prezzo, strike, barriera),
            "source": "Certificati&Derivati"
        }
    except: return None

# --- RUNNER ---
async def run_batch():
    print("--- 🚀 AVVIO SCRAPER ---")
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36']
        )
        page = await browser.new_page()
        isins = []
        try:
            await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", timeout=30000)
            hrefs = await page.evaluate("() => Array.from(document.querySelectorAll(\"table a[href*='isin=']\")).map(a => a.href)")
            for h in hrefs:
                m = re.search(r'isin=([A-Z0-9]{12})', h or "", re.IGNORECASE)
                if m: isins.append(m.group(1))
            isins = list(set(isins))[:200]
        except: isins = ['IT0006771510', 'DE000HD8SXZ1']

        print(f"📋 Coda: {len(isins)} ISIN")
        for isin in isins:
            if len(results) >= TARGET_COUNT: break
            print(f"Analisi {isin}...", end="\r")
            await asyncio.sleep(0.5)
            data = await scrape_certificate(page, isin)
            if isinstance(data, dict):
                results.append(data)
                print(f"✅ PRESO: {isin} | {data['price']}€")
            elif data == "SKIPPED_TYPE":
                print(f"⏩ SALTATO: {isin} (Asset)")
            else:
                print(f"❌ SKIP: {isin}")
        
        await browser.close()

    output = {
        "last_update": datetime.now().isoformat(),
        "total": len(results),
        "certificates": results
    }
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    print(f"\n💾 Salvati {len(results)} certificati.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", help="Live search")
    args = parser.parse_args()
    if args.isin:
        asyncio.run(scrape_certificate(args.isin))
    else:
        asyncio.run(run_batch())
