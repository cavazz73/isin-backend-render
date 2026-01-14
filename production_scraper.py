import asyncio
import json
import os
import re
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# --- CONFIGURAZIONE ---
TARGET_COUNT = 100         # Obiettivo: 100 certificati validi
MAX_PAGES_TO_SCAN = 10     # Quante pagine dell'elenco scorrere
OUTPUT_FILE = "data/certificates-data.json"

# KEYWORDS PER IL FILTRO (Case Insensitive)
# Inclusione: Indici, Commodities, Tassi, Valute
ALLOWED_KEYWORDS = [
    "FTSE", "MIB", "DAX", "S&P", "NASDAQ", "DOW", "EURO STOXX", "NIKKEI", "HANG SENG", # Indici
    "GOLD", "SILVER", "OIL", "WTI", "BRENT", "GAS", "COPPER", "PALLADIUM",             # Commodities
    "EURIBOR", "IRS", "CMS", "BTP", "BUND", "TREASURY", "T-NOTE",                      # Tassi
    "EUR/USD", "USD/JPY", "CHANGE"                                                     # Valute
]

# Esclusione: Azioni Singole (Per evitare falsi positivi)
EXCLUDED_KEYWORDS = [
    "ENEL", "ENI", "INTESA", "UNICREDIT", "STELLANTIS", "STM", "LEONARDO",
    "TESLA", "APPLE", "NVIDIA", "AMAZON", "MICROSOFT", "META", "ALPHABET",
    "NETFLIX", "FERRARI", "MONCLER", "PRYSMIAN", "GENERALI"
]

# --- FUNZIONI DI CALCOLO (SCENARI) ---
def calculate_scenarios(price, strike, barrier):
    """Calcola scenari semplificati stile CedLab"""
    if not price or not strike or price == 0:
        return {"years_to_maturity": 0, "scenarios": []}

    scenarios = []
    # Variazioni standard mostrate nei tool professionali
    variations = [-50, -40, -30, -20, -10, 0, 10, 20] 

    for var in variations:
        price_at_expiry = strike * (1 + (var / 100))
        
        # Logica rimborso base (Cash Collect):
        # Sopra barriera = 100 (Nominale). Sotto barriera = Performance lineare
        if price_at_expiry >= (strike * (barrier / 100)):
            reimbursement = 100.0
        else:
            reimbursement = (price_at_expiry / strike) * 100

        # Profit & Loss
        pl_pct = ((reimbursement - price) / price) * 100
        
        scenarios.append({
            "variation_pct": var,
            "underlying_price": round(price_at_expiry, 2),
            "redemption": round(reimbursement, 2),
            "pl_pct": round(pl_pct, 2),
            "pl_annual": round(pl_pct / 2, 2) # Stima su 2 anni
        })

    return {"years_to_maturity": 2.0, "scenarios": scenarios}

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def parse_float(text):
    if not text: return 0.0
    text = text.upper().replace('EUR', '').replace('%', '').strip()
    clean = re.sub(r'[^\d,-]', '', text).replace(',', '.')
    try:
        return float(clean)
    except:
        return 0.0

# --- LOGICA DI FILTRO ---
def analyze_underlying(name, isin):
    """
    Ritorna TRUE se il sottostante è valido (Indici/Commodities), FALSE se è azione singola.
    """
    u_name = name.upper()
    
    # 1. Check Esclusioni (Azioni singole famose)
    for ex in EXCLUDED_KEYWORDS:
        if ex in u_name:
            print(f"   [SKIP] {isin}: Scartato '{name}' (Contiene {ex})")
            return False
            
    # 2. Check Inclusioni (Indici/Mat.Prime)
    for ok in ALLOWED_KEYWORDS:
        if ok in u_name:
            return True
            
    # 3. Se contiene "BASKET" o "PANIER", lo accettiamo (spesso sono indici settoriali)
    if "BASKET" in u_name or "PANIERE" in u_name:
        return True

    print(f"   [SKIP] {isin}: Scartato '{name}' (Non riconosciuto come Indice/Commodity)")
    return False

# --- FASE 1: RACCOLTA LINK (DISCOVERY) ---
async def collect_isins(page):
    print("🔎 AVVIO SCANSIONE ELENCHI...")
    found_isins = []
    
    # URL Diretto all'elenco certificati (spesso ordinato per data decrescente di default)
    base_url = "https://www.certificatiederivati.it/db_bs_elenco_certificati.asp"
    
    try:
        print(f"   Navigazione: {base_url}")
        await page.goto(base_url, timeout=30000)
        
        # Scorriamo le pagine (simulazione)
        # Nota: I siti ASP usano PostBack, è difficile fare "Next" pulito senza cliccare.
        # Raccogliamo tutto quello che c'è nella prima pagina e proviamo a vedere se ci sono link alle pagine successive.
        
        # Strategia: Prendiamo tutti i link che sembrano certificati
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        # Cerchiamo link tipo "db_bs_scheda_certificato.asp?isin=..."
        links = soup.find_all('a', href=re.compile(r'isin=[A-Z0-9]{12}'))
        
        for link in links:
            href = link.get('href')
            match = re.search(r'isin=([A-Z0-9]{12})', href, re.IGNORECASE)
            if match:
                found_isins.append(match.group(1))

        # --- TENTATIVO DI SCROLLING/PAGINAZIONE ---
        # Se ne abbiamo trovati pochi, proviamo a cercare altri elenchi o "Ultimi inseriti"
        if len(found_isins) < 50:
            print("   ⚠️ Pochi risultati in pagina 1. Provo sezione 'Ultimi Inseriti'...")
            # Spesso c'è un box "Ultimi inseriti" in home
            await page.goto("https://www.certificatiederivati.it/home.asp", timeout=30000)
            content_home = await page.content()
            soup_home = BeautifulSoup(content_home, 'html.parser')
            links_home = soup_home.find_all('a', href=re.compile(r'isin=[A-Z0-9]{12}'))
            for link in links_home:
                href = link.get('href')
                match = re.search(r'isin=([A-Z0-9]{12})', href, re.IGNORECASE)
                if match:
                    found_isins.append(match.group(1))

    except Exception as e:
        print(f"❌ Errore durante la discovery: {e}")

    # Rimuovi duplicati mantenendo l'ordine
    unique = list(dict.fromkeys(found_isins))
    print(f"✅ TROVATI {len(unique)} ISIN UNIVOCI DA ANALIZZARE.\n")
    return unique

# --- FASE 2: ESTRAZIONE DETTAGLI ---
async def scrape_details(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    try:
        await page.goto(url, timeout=20000)
        
        # Attendiamo un elemento chiave, ma con fallback veloce
        try:
            await page.wait_for_selector("text=Emittente", timeout=5000)
        except:
            pass # Proviamo a parsare comunque

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Funzione helper per estrarre celle tabella
        def get_val(labels):
            if isinstance(labels, str): labels = [labels]
            for label in labels:
                # Cerca stringa case-insensitive
                tag = soup.find(string=re.compile(label, re.IGNORECASE))
                if tag and tag.find_parent('td'):
                    # Cerca nella cella successiva o adiacente
                    nxt = tag.find_parent('td').find_next_sibling('td')
                    if nxt: return clean_text(nxt.get_text())
            return None

        # --- DATI ---
        nome = clean_text(soup.find('td', class_='titolo_scheda').get_text()) if soup.find('td', class_='titolo_scheda') else f"Certificato {isin}"
        
        # Sottostante (Cruciale per il filtro)
        # A volte è "Sottostante", a volte è nel nome
        sottostante_raw = get_val(["Sottostante", "Sottostanti"]) or nome
        
        # APPLICAZIONE FILTRO
        if not analyze_underlying(sottostante_raw, isin):
            return None

        emittente = get_val("Emittente") or "N/D"
        categoria = get_val(["Categoria", "Tipologia"]) or "Investment Certificate"
        
        # Prezzi
        bid = parse_float(get_val(["Denaro", "Bid"]))
        ask = parse_float(get_val(["Lettera", "Ask"]))
        last = parse_float(get_val(["Ultimo", "Prezzo", "Valore"]))
        
        # Se mancano bid/ask, usiamo l'ultimo prezzo per tutto
        ref_price = last if last > 0 else (bid if bid > 0 else 100.0)
        if bid == 0: bid = ref_price
        if ask == 0: ask = ref_price

        # Dati tecnici
        barriera = parse_float(get_val(["Barriera", "Livello Barriera"]))
        strike = parse_float(get_val(["Strike", "Livello Iniziale"]))
        if strike == 0: strike = ref_price # Fallback
        
        cedola = parse_float(get_val(["Cedola", "Premio", "Bonus"]))
        
        scadenza_str = get_val(["Scadenza", "Data Scadenza"])
        try:
            mat_date = datetime.strptime(scadenza_str, "%d/%m/%Y").isoformat()
        except:
            mat_date = datetime.now().isoformat()

        # Scenari
        scenarios = calculate_scenarios(ref_price, strike, barriera)

        # Output JSON Object
        return {
            "isin": isin,
            "name": nome,
            "type": categoria,
            "issuer": emittente,
            "market": "SeDeX",
            "currency": "EUR",
            "bid_price": bid,
            "ask_price": ask,
            "reference_price": ref_price,
            "annual_coupon_yield": cedola,
            "barrier_down": barriera,
            "barrier_type": "Discreta",
            "maturity_date": mat_date,
            "underlyings": [{
                "name": sottostante_raw,
                "strike": strike,
                "spot": ref_price, # Approssimazione
                "barrier": barriera,
                "variation_pct": 0.0,
                "variation_abs": 0.0,
                "worst_of": False
            }],
            "scenario_analysis": scenarios
        }

    except Exception as e:
        # print(f"   Errore parsing {isin}: {e}")
        return None

# --- MAIN ---
async def main():
    print("--- 🚀 AVVIO SCRAPER v3 (Discovery + Filter) ---")
    
    valid_certificates = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # User agent realistico per evitare blocchi
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36")
        page = await context.new_page()

        # 1. Trova ISIN
        isin_list = await collect_isins(page)
        
        # 2. Itera e Filtra
        print(f"--- INIZIO ANALISI DETTAGLI SU {len(isin_list)} ISIN ---")
        for i, isin in enumerate(isin_list):
            if len(valid_certificates) >= TARGET_COUNT:
                print("🎯 Target raggiunto!")
                break
                
            print(f"Processing {i+1}/{len(isin_list)}: {isin}...", end="\r")
            
            data = await scrape_details(page, isin)
            if data:
                print(f"✅ PRESO: {isin} | {data['underlyings'][0]['name'][:25]}... | {data['annual_coupon_yield']}%")
                valid_certificates.append(data)
            
            # Pausa etica minima
            # await asyncio.sleep(0.5)

        await browser.close()

    # Salvataggio
    output = {
        "last_update": datetime.now().isoformat(),
        "count": len(valid_certificates),
        "certificates": valid_certificates
    }
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n\n--- 🏁 COMPLETATO. Salvati {len(valid_certificates)} certificati in {OUTPUT_FILE} ---")

if __name__ == "__main__":
    asyncio.run(main())
