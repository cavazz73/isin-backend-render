import asyncio
import json
import os
import re
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# --- CONFIGURAZIONE ---
# Lista di ISIN da monitorare. 
# NOTA: Per un uso reale, potresti voler caricare questa lista da un file esterno o DB.
ISIN_LIST = [
    "IT0006755661", "IT0006756297", "XS2463717681", "IT0006754433",
    "CH1261320142", "IT0006753765", "XS2544211182", "IT0006751231",
    "IT0006758889", "XS2623358325" 
]

OUTPUT_FILE = "data/certificates-data.json"

# --- FUNZIONI DI PARSING ---
def parse_float(text):
    """Converte stringhe come '1.000,50 %' in float 1000.50"""
    if not text: return 0.0
    clean = re.sub(r'[^\d,-]', '', text).replace(',', '.')
    try:
        return float(clean)
    except ValueError:
        return 0.0

def clean_text(text):
    if not text: return "N/D"
    return " ".join(text.split())

# --- LOGICA DI SCRAPING (CORE) ---
async def scrape_certificate(page, isin):
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    print(f"[{isin}] ⏳ Navigazione: {url}")
    
    try:
        # 1. Navigazione Stealth
        await page.goto(url, timeout=60000)
        
        # 2. PUNTO CRITICO: Attendiamo che la tabella dati sia renderizzata
        # Se fallisce qui, significa che il sito ha bloccato o la pagina non esiste
        try:
            await page.wait_for_selector("text=Scheda Sottostante", timeout=15000)
        except Exception:
            print(f"[{isin}] ⚠️ Timeout attesa dati (o ISIN non valido).")
            return None

        # 3. Estrazione HTML
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # 4. Parsing della Tabella ASP (Layout vecchio stile)
        def get_val_by_label(label_pattern):
            """Cerca una cella che contiene il testo e restituisce la cella successiva"""
            tag = soup.find(string=re.compile(label_pattern, re.IGNORECASE))
            if tag:
                td = tag.find_parent('td')
                if td:
                    next_td = td.find_next_sibling('td')
                    if next_td:
                        return next_td.get_text(strip=True)
            return None

        # Estrazione Campi
        nome = soup.find('td', class_='titolo_scheda')
        nome_text = clean_text(nome.get_text()) if nome else f"Certificato {isin}"
        
        prezzo_str = get_val_by_label("Prezzo") or get_val_by_label("Ultimo") or "0"
        prezzo = parse_float(prezzo_str)
        
        emittente = clean_text(get_val_by_label("Emittente"))
        categoria = clean_text(get_val_by_label("Categoria"))
        
        barriera_str = get_val_by_label("Barriera") or "0"
        barriera = parse_float(barriera_str)
        
        cedola_str = get_val_by_label("Cedola") or get_val_by_label("Premio") or "0"
        cedola = parse_float(cedola_str)
        
        scadenza_str = get_val_by_label("Scadenza")
        try:
            # Tenta conversione data dd/mm/yyyy -> ISO
            dt = datetime.strptime(scadenza_str, "%d/%m/%Y")
            scadenza_iso = dt.isoformat()
        except:
            scadenza_iso = datetime.now().isoformat() # Fallback

        # --- CREAZIONE OGGETTO JSON COMPATIBILE COL FRONTEND ---
        # Le chiavi devono corrispondere esattamente a quelle usate in certificates.js
        cert_data = {
            "isin": isin,
            "name": nome_text,
            "type": categoria if categoria != "N/D" else "Cash Collect",
            "issuer": emittente,
            "market": "SeDeX", 
            "currency": "EUR",
            "bid_price": prezzo,       
            "ask_price": prezzo,       # In mancanza di book completo, usiamo l'ultimo prezzo
            "reference_price": prezzo,
            "annual_coupon_yield": cedola,
            "barrier_down": barriera,
            "barrier_type": "Discreta", # Default ragionevole
            "issue_date": datetime.now().isoformat(), # Dato spesso non presente in scheda rapida
            "maturity_date": scadenza_iso,
            
            # Struttura Sottostanti (Mock intelligente se il parsing dettagliato è complesso)
            "underlyings": [
                {
                    "name": "Panier Sottostanti", # Spesso i nomi sono immagini nel sito target
                    "strike": 100.0,
                    "spot": 100.0, 
                    "barrier": barriera,
                    "variation_pct": 0.0,
                    "variation_abs": 0.0,
                    "worst_of": True
                }
            ],
            
            # Struttura Scenari (Vuota ma presente per evitare crash frontend)
            "scenario_analysis": {
                "years_to_maturity": 0.0,
                "scenarios": []
            }
        }
        
        print(f"[{isin}] ✅ Dati estratti: {prezzo}€ | Barriera: {barriera}%")
        return cert_data

    except Exception as e:
        print(f"[{isin}] ❌ Errore imprevisto: {e}")
        return None

# --- LOOP PRINCIPALE ---
async def main():
    print("--- 🚀 AVVIO SCRAPER CERTIFICATI (Playwright Engine) ---")
    
    results = []
    
    async with async_playwright() as p:
        # Launch browser con opzioni anti-detect base
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        for isin in ISIN_LIST:
            data = await scrape_certificate(page, isin)
            if data:
                results.append(data)
            # Sleep per evitare ban IP
            await asyncio.sleep(2)

        await browser.close()

    # Salvataggio su file JSON
    output_data = {
        "success": True,
        "last_update": datetime.now().isoformat(),
        "count": len(results),
        "certificates": results
    }

    # Assicura che la cartella esista
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"--- 🏁 COMPLETATO. {len(results)} certificati salvati in {OUTPUT_FILE} ---")

if __name__ == "__main__":
    asyncio.run(main())
