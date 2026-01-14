import asyncio
import json
import os
import re
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# --- CONFIGURAZIONE ---
TARGET_COUNT = 100         # Obiettivo: 100 certificati validi
OUTPUT_FILE = "data/certificates-data.json"

# Fallback ISIN sicuri (trovati durante l'analisi, per garantire risultati immediati)
SEED_ISINS = [
    'IT0006771510', 'DE000HD8SXZ1', 'XS2470031936', 'CH1390857220',
    'IT0006755018', 'XS2544207512', 'DE000VU5FFT5', 'NLBNPIT1X4F5',
    'CH1423921183', 'XS2662146856', 'IT0005653594'
]

# KEYWORDS PER IL FILTRO (Case Insensitive)
VALID_UNDERLYINGS = [
    "FTSE", "MIB", "DAX", "S&P", "NASDAQ", "DOW", "EURO STOXX", "NIKKEI", # Indici
    "GOLD", "SILVER", "OIL", "WTI", "BRENT", "GAS", "COPPER",             # Commodities
    "EURIBOR", "IRS", "CMS", "BTP", "BUND", "TREASURY",                   # Tassi
    "EUR/", "USD/", "JPY/"                                                # Valute
]

EXCLUDED_KEYWORDS = [
    "ENEL", "ENI", "INTESA", "UNICREDIT", "STELLANTIS", "STM", "LEONARDO",
    "TESLA", "APPLE", "NVIDIA", "AMAZON", "MICROSOFT", "META", "ALPHABET"
]

# --- CALCOLO SCENARI (STILE CEDLAB) ---
def calculate_cedlab_scenarios(price, strike, barrier):
    """
    Ricostruisce la tabella di analisi scenario matematicamente.
    """
    if not price or not strike or price <= 0:
        return {"years_to_maturity": 0, "scenarios": []}

    scenarios = []
    # Variazioni tipiche delle tabelle di analisi
    variations = [-50, -40, -30, -20, -10, 0, 10, 20] 

    for var in variations:
        price_at_expiry = strike * (1 + (var / 100))
        
        # Logica rimborso standard (Cash Collect / Phoenix)
        if price_at_expiry >= (strike * (barrier / 100)):
            reimbursement = 100.0
        else:
            # Performance lineare sotto barriera
            reimbursement = (price_at_expiry / strike) * 100

        # P&L percentuale
        pl_pct = ((reimbursement - price) / price) * 100
        
        scenarios.append({
            "variation_pct": var,
            "underlying_price": round(price_at_expiry, 2),
            "redemption": round(reimbursement, 2),
            "pl_pct": round(pl_pct, 2),
            "pl_annual": round(pl_pct / 2, 2) # Stima annualizzata su 2 anni
        })

    return {"years_to_maturity": 2.0, "scenarios": scenarios}

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip() if text else "N/D"

def parse_float(text):
    if not text: return 0.0
    text = text.upper().replace('EUR', '').replace('%', '').strip()
    clean = re.sub(r'[^\d,-]', '', text).replace(',', '.')
    try:
        return float(clean)
    except:
        return 0.0

def is_valid_certificate(name, underlying_name):
    """Filtra per tenere solo Indici, Commodities e Tassi."""
    full_text = (name + " " + underlying_name).upper()
    
    # 1. Deve contenere una keyword valida OPPURE essere un "Basket" generico
    has_valid = any(k in full_text for k in VALID_UNDERLYINGS) or "BASKET" in full_text or "PANIERE" in full_text
    
    # 2. NON deve contenere nomi di azioni singole escluse
    has_excluded = any(k in full_text for k in EXCLUDED_KEYWORDS)
    
    return has_valid and not has_excluded

# --- MOTORE DI SCRAPING ---
async def scrape_new_issues(page):
    """Scansiona la pagina 'Nuove Emissioni' per trovare ISIN freschi."""
    print("🔎 Scansione pagina Nuove Emissioni...")
    found_isins = []
    
    try:
        # URL corretto trovato dall'analisi
        await page.goto("https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp", timeout=30000)
        
        # Estrai tutti i link ai certificati
        links = await page.locator("a[href*='isin=']").all()
        for link in links:
            href = await link.get_attribute("href")
            if href:
                match = re.search(r'isin=([A-Z0-9]{12})', href, re.IGNORECASE)
                if match:
                    found_isins.append(match.group(1))
                    
    except Exception as e:
        print(f"⚠️ Errore parziale discovery: {e}")

    # Unisci con i SEED_ISINS per sicurezza
    all_isins = list(set(found_isins + SEED_ISINS))
    print(f"✅ Trovati {len(all_isins)} ISIN da analizzare.")
    return all_isins

async def scrape_details(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    try:
        await page.goto(url, timeout=45000)
        
        # Attesa intelligente: o appare "Scheda" o "Emittente"
        try:
            await page.wait_for_selector("text=Scheda Sottostante", timeout=5000)
        except:
            pass

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Helper estrazione
        def get_val(labels):
            if isinstance(labels, str): labels = [labels]
            for label in labels:
                tag = soup.find(string=re.compile(label, re.IGNORECASE))
                if tag and tag.find_parent('td'):
                    nxt = tag.find_parent('td').find_next_sibling('td')
                    if nxt: return clean_text(nxt.get_text())
            return None

        # --- ESTRAZIONE CAMPI ---
        nome = clean_text(soup.find('td', class_='titolo_scheda').get_text()) if soup.find('td', class_='titolo_scheda') else f"Certificato {isin}"
        
        # Filtro Sottostante
        sottostante = get_val(["Sottostante", "Sottostanti"]) or nome
        if not is_valid_certificate(nome, sottostante):
            # print(f"⏭️  SKIP {isin}: Sottostante '{sottostante}' non ammesso.")
            return None

        emittente = get_val("Emittente") or "N/D"
        categoria = get_val(["Categoria", "Tipologia"]) or "Cash Collect"
        
        # Prezzi e Valori
        bid = parse_float(get_val(["Denaro", "Bid"]))
        ask = parse_float(get_val(["Lettera", "Ask"]))
        last = parse_float(get_val(["Ultimo", "Prezzo", "Valore"]))
        
        ref_price = last if last > 0 else (bid if bid > 0 else 100.0)
        
        strike = parse_float(get_val(["Strike", "Livello Iniziale"]))
        if strike == 0: strike = ref_price # Fallback per calcoli
        
        barriera_pct = parse_float(get_val(["Barriera", "Livello Barriera"]))
        cedola = parse_float(get_val(["Cedola", "Premio", "Bonus"]))

        # Date
        scadenza_str = get_val(["Scadenza", "Data Scadenza"])
        try:
            mat_date = datetime.strptime(scadenza_str, "%d/%m/%Y").isoformat()
        except:
            mat_date = datetime.now().isoformat()

        # Generazione Analisi Scenario (CedLab Style)
        scenarios = calculate_cedlab_scenarios(ref_price, strike, barriera_pct)

        # Costruzione Oggetto JSON
        cert = {
            "isin": isin,
            "name": nome,
            "type": categoria,
            "issuer": emittente,
            "market": "SeDeX/Cert-X",
            "currency": "EUR",
            "bid_price": ref_price, # Usiamo ref_price per semplicità
            "ask_price": ref_price,
            "reference_price": ref_price,
            "annual_coupon_yield": cedola,
            "barrier_down": barriera_pct,
            "barrier_type": "Discreta",
            "maturity_date": mat_date,
            "underlyings": [{
                "name": sottostante,
                "strike": strike,
                "spot": ref_price,
                "barrier": barriera_pct,
                "variation_pct": 0.0,
                "variation_abs": 0.0,
                "worst_of": False
            }],
            "scenario_analysis": scenarios
        }
        
        print(f"✅ PRESO: {isin} | {emittente} | {sottostante[:20]}... | {ref_price}€")
        return cert

    except Exception as e:
        return None

async def main():
    print("--- 🚀 AVVIO SCRAPER v4 (Nuove Emissioni + CedLab Calc) ---")
    
    valid_certs = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # User agent Chrome recente per evitare blocchi
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        page = await context.new_page()

        # 1. Trova ISIN recenti
        isin_list = await scrape_new_issues(page)
        
        # 2. Analizza dettagli
        for isin in isin_list:
            if len(valid_certs) >= TARGET_COUNT:
                break
                
            data = await scrape_details(page, isin)
            if data:
                valid_certs.append(data)
                
            # Rispetto per il server
            # await asyncio.sleep(0.5)

        await browser.close()

    # Salvataggio
    output = {
        "success": True,
        "last_update": datetime.now().isoformat(),
        "count": len(valid_certs),
        "certificates": valid_certs
    }
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n--- 🏁 FATTO. {len(valid_certs)} certificati salvati in {OUTPUT_FILE} ---")

if __name__ == "__main__":
    asyncio.run(main())
