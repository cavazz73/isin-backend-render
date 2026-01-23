import asyncio
import json
import re
import os
import argparse
import random
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# --- CONFIGURAZIONE ---
TARGET_COUNT = 80
OUTPUT_FILE = "data/certificates-data.json"

# --- HELPER ---
def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip() if text else "N/D"

def parse_float(text):
    if not text: return 0.0
    # Rimuovi tutto tranne numeri, punti e virgole
    text = text.upper().replace('EUR', '').replace('€', '').replace('%', '').strip()
    # Caso 1.234,56 -> 1234.56
    if ',' in text and '.' in text:
        text = text.replace('.', '').replace(',', '.')
    # Caso 1234,56 -> 1234.56
    elif ',' in text:
        text = text.replace(',', '.')
    
    # Pulizia finale caratteri non numerici rimasti
    clean = re.sub(r'[^\d.]', '', text)
    try:
        return float(clean)
    except:
        return 0.0

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

# ==========================================
# MOTORE DI SCRAPING "RAGGI X" (REGEX)
# ==========================================
async def scrape_ced_regex(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    try:
        await page.goto(url, timeout=20000, wait_until="domcontentloaded")
        
        if "home.asp" in page.url: return None

        # Parsing HTML
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        full_text = clean_text(soup.get_text())

        # Check validità
        if "EMITTENTE" not in full_text.upper() and "SOTTOSTANTE" not in full_text.upper():
            return None

        # --- ESTRAZIONE DATI ---
        
        # 1. NOME
        nome_tag = soup.find('td', class_='titolo_scheda')
        nome = clean_text(nome_tag.get_text()) if nome_tag else f"Certificate {isin}"

        # 2. PREZZO (Strategia Regex Potente)
        # Cerca pattern tipo: "Prezzo 100,50" oppure "Ultimo: 98.4"
        # Il pattern cerca: Parola chiave -> spaziatura opzionale -> Numero con virgola o punto
        prezzo = 0.0
        
        # Regex per trovare prezzi (es. 100,00 | 95.40)
        price_patterns = [
            r'Prezzo\s*[:]?\s*(\d+[.,]\d+)',
            r'Ultimo\s*[:]?\s*(\d+[.,]\d+)',
            r'Valore\s*[:]?\s*(\d+[.,]\d+)',
            r'Quotazione\s*[:]?\s*(\d+[.,]\d+)',
            r'Ask\s*[:]?\s*(\d+[.,]\d+)',
            r'Lettera\s*[:]?\s*(\d+[.,]\d+)'
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                val_str = match.group(1)
                p = parse_float(val_str)
                if p > 0:
                    prezzo = p
                    break
        
        # Fallback: Se Regex fallisce, cerca il primo numero con € nel testo
        if prezzo == 0:
            euro_match = re.search(r'(\d+[.,]\d{2})\s*€', full_text)
            if euro_match:
                prezzo = parse_float(euro_match.group(1))

        if prezzo <= 0: return None

        # 3. DATI TECNICI (Regex)
        def extract_by_regex(keywords):
            for kw in keywords:
                # Cerca "Keyword: 123,45"
                pat = kw + r'\s*[:]?\s*(\d+[.,]?\d*)'
                m = re.search(pat, full_text, re.IGNORECASE)
                if m: return parse_float(m.group(1))
            return 0.0

        strike = extract_by_regex(["Strike", "Iniziale", "Strike Level"])
        barriera = extract_by_regex(["Barriera", "Barrier", "Livello Barriera"])
        cedola = extract_by_regex(["Cedola", "Premio", "Bonus"])
        
        # Emittente (Cerca nella tabella standard perché è testo)
        emittente = "N/D"
        em_tag = soup.find(string=re.compile("Emittente", re.IGNORECASE))
        if em_tag and em_tag.find_parent('td'):
             nxt = em_tag.find_parent('td').find_next_sibling('td')
             if nxt: emittente = clean_text(nxt.get_text())

        # Defaults logici
        if strike == 0: strike = prezzo
        if barriera == 0: barriera = prezzo * 0.60

        return {
            "isin": isin,
            "name": nome,
            "type": "Certificate",
            "issuer": emittente,
            "market": "SeDeX",
            "currency": "EUR",
            "price": prezzo,
            "bid_price": prezzo,
            "ask_price": prezzo,
            "annual_coupon_yield": cedola,
            "barrier_down": barriera,
            "strike": strike,
            "maturity_date": (datetime.now() + timedelta(days=730)).isoformat(),
            "underlyings": [{"name": nome, "strike": strike, "barrier": barriera}],
            "scenario_analysis": calculate_scenarios(prezzo, strike, barriera),
            "source": "Certificati&Derivati"
        }

    except Exception:
        return None

# ==========================================
# RUNNER
# ==========================================
async def run_batch():
    print("--- 🚀 AVVIO SCRAPER REGEX (FINAL FIX) ---")
    results = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36']
        )
        page = await browser.new_page()

        # 1. Recupero ISIN da lista "Nuove Emissioni"
        try:
            await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", timeout=20000)
            links = await page.locator("a[href*='isin=']").all()
            isins = []
            for l in links:
                href = await l.get_attribute("href")
                m = re.search(r'isin=([A-Z0-9]{12})', href or "", re.IGNORECASE)
                if m: isins.append(m.group(1))
            
            isins = list(set(isins))
            if not isins: raise Exception("Lista vuota")
        except:
            # Fallback manuale se la lista fallisce
            isins = ['IT0006771510', 'DE000HD8SXZ1', 'XS2470031936', 'CH1390857220', 'IT0006755018', 'XS2544207512']

        print(f"📋 Coda Analisi: {len(isins)} ISIN")

        for isin in isins:
            if len(results) >= TARGET_COUNT: break
            
            print(f"Analisi {isin}...", end="\r")
            await asyncio.sleep(random.uniform(0.5, 1.5)) # Human delay
            
            data = await scrape_ced_regex(page, isin)
            
            if data:
                results.append(data)
                print(f"✅ OK: {isin} | {data['price']}€            ")
            else:
                print(f"❌ SKIP: {isin} (Regex fallita)        ")
        
        await browser.close()

    # Salvataggio
    output = {
        "last_update": datetime.now().isoformat(),
        "total": len(results),
        "certificates": results
    }
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n💾 Salvati {len(results)} certificati.")

async def run_live(isin):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        data = await scrape_ced_regex(page, isin)
        await browser.close()
        print(json.dumps(data if data else {"error": "not_found"}))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", help="Live search")
    args = parser.parse_args()

    if args.isin:
        asyncio.run(run_live(args.isin))
    else:
        asyncio.run(run_batch())
