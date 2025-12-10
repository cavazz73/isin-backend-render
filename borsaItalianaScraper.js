/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Borsa Italiana Bond Scraper
 * Fonte ufficiale per dati obbligazionari italiani (BTP, BOT, CCT, CTZ)
 */

const axios = require('axios');
const cheerio = require('cheerio');

class BorsaItalianaScraper {
  constructor() {
    this.baseUrl = 'https://www.borsaitaliana.it';
    this.categories = {
      btp: 'governativi/btp',
      bot: 'governativi/bot', 
      cct: 'governativi/cct',
      ctz: 'governativi/ctz'
    };
    
    // Rate limiting: 1 richiesta ogni 2 secondi
    this.lastRequest = 0;
    this.minDelay = 2000;
    
    // Cache 1 ora
    this.cache = new Map();
    this.cacheDuration = 60 * 60 * 1000;
  }

  /**
   * Rate limiting per rispettare i limiti del server
   */
  async rateLimit() {
    const now = Date.now();
    const timeSinceLastRequest = now - this.lastRequest;
    
    if (timeSinceLastRequest < this.minDelay) {
      await new Promise(resolve => 
        setTimeout(resolve, this.minDelay - timeSinceLastRequest)
      );
    }
    
    this.lastRequest = Date.now();
  }

  /**
   * Verifica e recupera dalla cache
   */
  getFromCache(key) {
    const cached = this.cache.get(key);
    if (cached && Date.now() - cached.timestamp < this.cacheDuration) {
      return cached.data;
    }
    return null;
  }

  /**
   * Salva in cache
   */
  setCache(key, data) {
    this.cache.set(key, {
      data,
      timestamp: Date.now()
    });
  }

  /**
   * Cerca bond per categoria
   * @param {string} category - 'btp', 'bot', 'cct', 'ctz'
   * @param {number} limit - Numero massimo risultati (default: 20)
   */
  async searchBonds(category = 'btp', limit = 20) {
    try {
      const cacheKey = `search_${category}_${limit}`;
      const cached = this.getFromCache(cacheKey);
      if (cached) return cached;

      await this.rateLimit();

      const categoryPath = this.categories[category.toLowerCase()];
      if (!categoryPath) {
        throw new Error(`Categoria non valida: ${category}`);
      }

      const url = `${this.baseUrl}/borsa/obbligazioni/${categoryPath}`;
      
      const response = await axios.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept': 'text/html,application/xhtml+xml',
          'Accept-Language': 'it-IT,it;q=0.9,en;q=0.8',
          'Cache-Control': 'no-cache'
        },
        timeout: 10000
      });

      const $ = cheerio.load(response.data);
      const bonds = [];

      // Parsing tabella bond
      $('.m-table tbody tr').each((i, row) => {
        if (i >= limit) return false;

        const $row = $(row);
        const cells = $row.find('td');
        
        if (cells.length >= 5) {
          const bond = {
            name: $(cells[0]).text().trim(),
            isin: $(cells[1]).text().trim(),
            price: this.parsePrice($(cells[2]).text()),
            change: this.parseChange($(cells[3]).text()),
            yield: this.parseYield($(cells[4]).text()),
            volume: this.parseVolume($(cells[5]).text()),
            category: category.toUpperCase(),
            currency: 'EUR',
            source: 'Borsa Italiana',
            dataDelay: '15min',
            timestamp: new Date().toISOString()
          };

          bonds.push(bond);
        }
      });

      const result = {
        category: category.toUpperCase(),
        count: bonds.length,
        bonds: bonds,
        disclaimer: 'Dati forniti da Borsa Italiana con delay di 15 minuti. Prezzi puramente indicativi.'
      };

      this.setCache(cacheKey, result);
      return result;

    } catch (error) {
      console.error(`Errore scraping Borsa Italiana (${category}):`, error.message);
      throw error;
    }
  }

  /**
   * Ottieni dettagli specifico bond per ISIN
   */
  async getBondDetails(isin, category = 'btp') {
    try {
      const cacheKey = `details_${isin}`;
      const cached = this.getFromCache(cacheKey);
      if (cached) return cached;

      await this.rateLimit();

      const categoryPath = this.categories[category.toLowerCase()];
      const url = `${this.baseUrl}/borsa/obbligazioni/${categoryPath}/scheda/${isin}.html`;

      const response = await axios.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept': 'text/html,application/xhtml+xml',
          'Accept-Language': 'it-IT,it;q=0.9,en;q=0.8'
        },
        timeout: 10000
      });

      const $ = cheerio.load(response.data);

      const details = {
        isin: isin,
        name: $('.t-text-large').first().text().trim(),
        price: this.parsePrice($('.m-valueblock--price .m-valueblock__value').text()),
        change: this.parseChange($('.m-valueblock--change .m-valueblock__value').text()),
        changePercent: this.parseChangePercent($('.m-valueblock--change-perc .m-valueblock__value').text()),
        
        // Caratteristiche bond
        coupon: this.parseCoupon($('.m-data-list').text()),
        maturity: this.parseMaturity($('.m-data-list').text()),
        issueDate: this.parseIssueDate($('.m-data-list').text()),
        
        currency: 'EUR',
        category: category.toUpperCase(),
        source: 'Borsa Italiana',
        dataDelay: '15min',
        timestamp: new Date().toISOString()
      };

      this.setCache(cacheKey, details);
      return details;

    } catch (error) {
      console.error(`Errore dettagli bond ${isin}:`, error.message);
      throw error;
    }
  }

  /**
   * Parsing helpers
   */
  parsePrice(text) {
    const match = text.match(/[\d.,]+/);
    return match ? parseFloat(match[0].replace(',', '.')) : null;
  }

  parseChange(text) {
    const match = text.match(/[-+]?[\d.,]+/);
    return match ? parseFloat(match[0].replace(',', '.')) : null;
  }

  parseChangePercent(text) {
    const match = text.match(/[-+]?[\d.,]+/);
    return match ? parseFloat(match[0].replace(',', '.')) : null;
  }

  parseYield(text) {
    const match = text.match(/[\d.,]+/);
    return match ? parseFloat(match[0].replace(',', '.')) : null;
  }

  parseVolume(text) {
    const cleaned = text.replace(/[^\d]/g, '');
    return cleaned ? parseInt(cleaned) : null;
  }

  parseCoupon(text) {
    const match = text.match(/Cedola[:\s]*([\d.,]+)%/i);
    return match ? parseFloat(match[1].replace(',', '.')) : null;
  }

  parseMaturity(text) {
    const match = text.match(/Scadenza[:\s]*(\d{2}\/\d{2}\/\d{4})/i);
    return match ? match[1] : null;
  }

  parseIssueDate(text) {
    const match = text.match(/Emissione[:\s]*(\d{2}\/\d{2}\/\d{4})/i);
    return match ? match[1] : null;
  }

  /**
   * Pulisci cache (opzionale)
   */
  clearCache() {
    this.cache.clear();
  }

  /**
   * Statistiche cache
   */
  getCacheStats() {
    return {
      size: this.cache.size,
      entries: Array.from(this.cache.keys())
    };
  }
}

module.exports = BorsaItalianaScraper;

// Test
if (require.main === module) {
  (async () => {
    const scraper = new BorsaItalianaScraper();
    
    console.log('🔍 Test Borsa Italiana Scraper\n');
    
    try {
      // Test 1: Cerca BTP
      console.log('1️⃣ Ricerca BTP...');
      const btps = await scraper.searchBonds('btp', 5);
      console.log(`   ✅ Trovati ${btps.count} BTP`);
      console.log(`   Primo: ${btps.bonds[0].name} - ${btps.bonds[0].price}€\n`);
      
      // Test 2: Dettagli specifico
      if (btps.bonds.length > 0) {
        console.log('2️⃣ Dettagli primo BTP...');
        const details = await scraper.getBondDetails(btps.bonds[0].isin, 'btp');
        console.log(`   ✅ ${details.name}`);
        console.log(`   Prezzo: ${details.price}€`);
        console.log(`   Cedola: ${details.coupon}%`);
        console.log(`   Scadenza: ${details.maturity}\n`);
      }
      
      // Test 3: Cache stats
      console.log('3️⃣ Cache Statistics:');
      console.log(scraper.getCacheStats());
      
    } catch (error) {
      console.error('❌ Errore test:', error.message);
    }
  })();
}
