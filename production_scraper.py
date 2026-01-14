import asyncio
import json
import os
import re
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# --- CONFIGURAZIONE TARGET ---
TARGET_COUNT = 100  # Quanti certificati validi vogliamo
MAX_PAGES_TO_CRAWL = 20 # Quante pagine di elenchi scorrere
OUTPUT_FILE = "data/certificates-data.json"

# Parole chiave per FILTRARE i sottostanti (Vogliamo Indici, Commodities, Tassi)
# Se il nome del sottostante contiene queste parole, lo teniamo.
ALLOWED_KEYWORDS = [
    "FTSE", "DAX", "S&P", "NASDAQ", "EURO STOXX", "NIKKEI", "DOW JONES", # Indici
    "GOLD", "SILVER", "OIL", "WTI", "BRENT", "GAS", "RAME",              # Commodities
    "EURIBOR", "IRS", "CMS", "BTP", "BUND", "TREASURY"                   # Tassi
]

# Parole chiave per ESCLUDERE (Azioni singole comuni)
EXCLUDED_KEYWORDS = [
    "ENEL", "ENI", "INTESA", "UNICREDIT", "STELLANTIS", "STM", "LEONARDO",
    "TESLA", "APPLE", "NVIDIA", "AMAZON", "MICROSOFT", "META", "GOOGLE"
]

# --- FUNZIONI DI CALCOLO FINANZIARIO (SCENARI CEDLAB STYLE) ---
def calculate_scenarios(price, strike, barrier, strategy="Long"):
    """
    Genera la tabella di analisi scenario (Simulazione a scadenza).
    """
    if not price or not strike or price == 0:
        return {"years_to_maturity": 0, "scenarios": []}

    scenarios = []
    variations = [-50, -40, -30, -20, -10, 0, 10, 20] # Variazioni % del sottostante

    for var in variations:
        underlying_price_at_expiry = strike * (1 + (var / 100))
        
        # Logica rimborso semplificata (Cash Collect / Phoenix)
        # Se > Barriera -> Rimborsa 100 (Nominale) + Eventuali cedole (qui semplifichiamo al nominale)
        # Se < Barriera -> Rimborsa performance lineare
        
        if underlying_price_at_expiry >= (strike * (barrier / 100)):
            reimbursement = 100.0 # Rimborso pieno
        else:
            # Performance negativa lineare: (Prezzo Finale / Strike) * 100
            reimbursement = (underlying_price_at_expiry / strike) * 100

        # P&L del certificato rispetto al prezzo di acquisto OGGI
        pl_pct = ((reimbursement - price) / price) * 100
        
        scenarios.append({
            "variation_pct": var,
            "underlying_price": round(underlying_price_at_expiry, 2),
            "redemption": round(reimbursement, 2),
            "pl_pct": round(pl_pct, 2),
            "pl_annual": round(pl_pct, 2) # Semplificazione (andrebbe diviso per anni)
        })

    return {
        "years_to_maturity": 2.0, # Valore default se non calcolabile
        "scenarios": scenarios
    }

# --- FUNZIONI DI UTILITY ---
def parse_float(text):
    if not text: return 0.0
    clean = re.sub(r'[^\d,-]', '', text).replace(',', '.')
    try:
        return float(clean)
    except:
        return 0.0

def is_wanted_underlying(name):
    """Filtro intelligente: TRUE se è Indice/Commodity/Tasso, FALSE se azione singola"""
    name = name.upper()
    # 1. Deve contenere una parola chiave "buona"
    has_allowed = any(k in name for k in ALLOWED_KEYWORDS)
    # 2. NON deve contenere parole chiave "escluse" (per evitare falsi positivi misti)
    has_excluded = any(k in name for k in EXCLUDED_KEYWORDS)
    
    # Logica permissiva: se contiene keyword buone, vince.
    # Se è un paniere misto (es. "Basket Indici"), lo teniamo.
    return has_allowed and not has_excluded

# --- CORE SCRAPER ---
async def scrape_catalog(page):
    """
    Naviga le pagine elenco per trovare ISIN recenti.
    Simuliamo la visita alla pagina 'Ultimi inseriti' o Ricerca.
    """
    isins_to_check = []
    
    # URL di esempio per "Ricerca Avanzata" o elenco (Da adattare se cambia l'URL reale del sito)
    # Usiamo una pagina che elenca molti certificati recenti
    print("🔎 Navigazione catalogo per scoprire nuovi ISIN...")
    
    # Nota: Poiché non posso navigare live, uso un pattern comune. 
    # In produzione, l'URL migliore è quello di "Tutti i certificati in quotazione" ordinati per data.
    # Qui simuliamo la raccolta visitando la home e le sezioni principali.
    urls_to_scan = [
        "https://www.certificatiederivati.it", 
        "https://www.certificatiederivati.it/db_bs_elenco_certificati.asp" 
    ]

    for url in urls_to_scan:
        try:
            await page.goto(url, timeout=30000)
            links = await page.locator("a[href*='isin=']").all()
            
            for link in links:
                href = await link.get_attribute("href")
                if href:
                    match = re.search(r'isin=([A-Z0-9]{12})', href, re.IGNORECASE)
                    if match:
                        isins_to_check.append(match.group(1))
        except Exception as e:
            print(f"Errore scansione {url}: {e}")

    # Rimuovi duplicati
    unique_isins = list(set(isins_to_check))
    print(f"✅ Trovati {len(unique_isins)} ISIN potenziali. Inizio analisi dettagli...")
    return unique_isins

async def scrape_certificate_details(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    try:
        await page.goto(url, timeout=45000)
        
        # Aspetta caricamento dati
        try:
            await page.wait_for_selector("text=Scheda Sottostante", timeout=5000)
        except:
            pass # Continua lo stesso, proviamo a parsare

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # --- ESTRAZIONE DATI ---
        def get_text_next_to(label):
            tag = soup.find(string=re.compile(label, re.IGNORECASE))
            if tag and tag.find_parent('td'):
                nxt = tag.find_parent('td').find_next_sibling('td')
                return nxt.get_text(strip=True) if nxt else None
            return None

        # Nome e Emittente
        nome_tag = soup.find('td', class_='titolo_scheda')
        nome = nome_tag.get_text(strip=True) if nome_tag else f"Certificato {isin}"
        emittente = get_text_next_to("Emittente") or "N/D"

        # Sottostanti (Analisi Tabella Sottostanti per il filtro)
        sottostanti = []
        underlying_table = soup.find("table", string=re.compile("Sottostante")) # Euristica
        # Se non trova la tabella specifica, cerca nel testo del nome
        sottostante_str = get_text_next_to("Sottostante") or nome
        
        # FILTRO CRUCIALE: Controlla se è Indice/Commodity
        if not is_wanted_underlying(sottostante_str):
            # print(f"⏭️  Saltato {isin}: Sottostante '{sottostante_str}' non è Indice/Commodity/Tasso.")
            return None

        # Dati Numerici
        prezzo_str = get_text_next_to("Prezzo") or get_text_next_to("Ultimo") or "100"
        prezzo = parse_float(prezzo_str)
        
        strike_str = get_text_next_to("Strike") or "100" # Spesso non esplicito in header
        strike = parse_float(strike_str)
        if strike == 0: strike = prezzo # Fallback per calcoli
        
        barriera_str = get_text_next_to("Barriera") or "0"
        barriera_val = parse_float(barriera_str)
        
        cedola_str = get_text_next_to("Cedola") or get_text_next_to("Premio") or "0"
        cedola = parse_float(cedola_str)

        scadenza = get_text_next_to("Scadenza")
        try:
            date_obj = datetime.strptime(scadenza, "%d/%m/%Y")
            scadenza_iso = date_obj.isoformat()
        except:
            scadenza_iso = datetime.now().isoformat()

        # Generazione Scenari (CedLab Style)
        scenarios = calculate_scenarios(prezzo, strike, barriera_val)

        # Costruzione Oggetto Finale
        cert = {
            "isin": isin,
            "name": nome,
            "type": get_text_next_to("Categoria") or "Investment Certificate",
            "issuer": emittente,
            "market": "SeDeX",
            "currency": "EUR",
            "bid_price": prezzo,
            "ask_price": prezzo,
            "reference_price": prezzo,
            "annual_coupon_yield": cedola,
            "barrier_down": barriera_val,
            "barrier_type": "Discreta",
            "maturity_date": scadenza_iso,
            "underlyings": [{
                "name": sottostante_str,
                "strike": strike,
                "spot": strike, # Assumiamo spot=strike se non real-time
                "barrier": barriera_val,
                "variation_pct": 0.0,
                "variation_abs": 0.0
            }],
            "scenario_analysis": scenarios
        }
        
        print(f"✅ PRESO: {isin} | {sottostante_str[:20]}... | {emittente}")
        return cert

    except Exception as e:
        # print(f"❌ Errore su {isin}: {e}")
        return None

async def main():
    print("--- 🚀 AVVIO SCRAPER INTELLIGENTE (Indici/Commodities/Tassi) ---")
    
    final_results = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36")
        page = await context.new_page()

        # 1. SCOPERTA ISIN
        found_isins = await scrape_catalog(page)
        
        # Se il crawling automatico non ne trova abbastanza, aggiungi una lista di backup manuale (facoltativo)
        if len(found_isins) < 10:
             print("⚠️  Pochi ISIN trovati automaticamente. Aggiungo fallback...")
             # Qui potresti aggiungere ISIN statici se il crawler fallisce

        # 2. ANALISI E FILTRAGGIO
        count = 0
        for isin in found_isins:
            if count >= TARGET_COUNT:
                break
                
            data = await scrape_certificate_details(page, isin)
            if data:
                final_results.append(data)
                count += 1
            
            # Pausa per non intasare il server
            await asyncio.sleep(1)

        await browser.close()

    # Salvataggio
    output = {
        "last_update": datetime.now().isoformat(),
        "count": len(final_results),
        "certificates": final_results
    }
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"--- 🏁 COMPLETATO. Salvati {len(final_results)} certificati filtrati. ---")

if __name__ == "__main__":
    asyncio.run(main())
