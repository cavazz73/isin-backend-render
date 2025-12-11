/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Redis Cache Manager
 * Intelligent caching with TTL management
 */

const Redis = require('ioredis');

class RedisCache {
    constructor(redisUrl) {
        this.client = null;
        this.redisUrl = redisUrl;
        this.isConnected = false;
        this.initializeClient();
        
        // TTL Configuration (in seconds)
        this.TTL = {
            // Fast-changing data
            PRICE: 300,           // 5 minutes
            QUOTE: 300,           // 5 minutes
            SEARCH: 300,          // 5 minutes
            
            // Medium-changing data
            MARKET_CAP: 3600,     // 1 hour
            PE_RATIO: 3600,       // 1 hour
            
            // Slow-changing data
            COMPANY_INFO: 86400,  // 24 hours
            LOGO: 604800,         // 7 days
            ISIN: 2592000,        // 30 days (ISIN never changes)
        };
        
        console.log('[RedisCache] Initialized with TTL config');
    }

    /**
     * Initialize Redis client with TLS support (ioredis)
     */
    initializeClient() {
        try {
            // ioredis automatically connects and handles TLS for Upstash URLs
            this.client = new Redis(this.redisUrl, {
                tls: {
                    rejectUnauthorized: false
                },
                retryStrategy: (times) => {
                    if (times > 3) {
                        return null; // Stop retrying
                    }
                    return Math.min(times * 100, 3000);
                }
            });

            this.client.on('connect', () => {
                console.log('[RedisCache] Connected to Upstash Redis');
                this.isConnected = true;
            });

            this.client.on('error', (err) => {
                console.error('[RedisCache] Error:', err.message);
                this.isConnected = false;
            });

            this.client.on('end', () => {
                console.log('[RedisCache] Disconnected from Redis');
                this.isConnected = false;
            });

        } catch (error) {
            console.error('[RedisCache] Initialization error:', error.message);
            this.isConnected = false;
        }
    }

    /**
     * Generate cache key
     */
    generateKey(type, identifier) {
        return `isin:${type}:${identifier.toLowerCase()}`;
    }

    /**
     * Get data from cache
     */
    async get(type, identifier) {
        if (!this.isConnected) {
            console.log('[RedisCache] Not connected, skipping cache');
            return null;
        }

        try {
            const key = this.generateKey(type, identifier);
            const cached = await this.client.get(key);
            
            if (cached) {
                console.log(`[RedisCache] ✅ HIT: ${key}`);
                return JSON.parse(cached);
            } else {
                console.log(`[RedisCache] ❌ MISS: ${key}`);
                return null;
            }
        } catch (error) {
            console.error('[RedisCache] Get error:', error.message);
            return null;
        }
    }

    /**
     * Set data in cache with TTL
     */
    async set(type, identifier, data, customTTL = null) {
        if (!this.isConnected) {
            console.log('[RedisCache] Not connected, skipping cache');
            return false;
        }

        try {
            const key = this.generateKey(type, identifier);
            const ttl = customTTL || this.TTL[type.toUpperCase()] || this.TTL.SEARCH;
            
            await this.client.setEx(key, ttl, JSON.stringify(data));
            console.log(`[RedisCache] ✅ SET: ${key} (TTL: ${ttl}s)`);
            
            return true;
        } catch (error) {
            console.error('[RedisCache] Set error:', error.message);
            return false;
        }
    }

    /**
     * Delete from cache
     */
    async delete(type, identifier) {
        if (!this.isConnected) {
            return false;
        }

        try {
            const key = this.generateKey(type, identifier);
            await this.client.del(key);
            console.log(`[RedisCache] 🗑️ DELETE: ${key}`);
            return true;
        } catch (error) {
            console.error('[RedisCache] Delete error:', error.message);
            return false;
        }
    }

    /**
     * Clear all cache (use with caution!)
     */
    async clearAll() {
        if (!this.isConnected) {
            return false;
        }

        try {
            await this.client.flushAll();
            console.log('[RedisCache] 🗑️ CLEARED ALL CACHE');
            return true;
        } catch (error) {
            console.error('[RedisCache] Clear all error:', error.message);
            return false;
        }
    }

    /**
     * Get cache statistics
     */
    async getStats() {
        if (!this.isConnected) {
            return {
                connected: false,
                message: 'Redis not connected'
            };
        }

        try {
            const info = await this.client.info('stats');
            const dbSize = await this.client.dbSize();
            
            return {
                connected: true,
                keysCount: dbSize,
                info: info,
                ttlConfig: this.TTL
            };
        } catch (error) {
            console.error('[RedisCache] Stats error:', error.message);
            return {
                connected: this.isConnected,
                error: error.message
            };
        }
    }

    /**
     * Check if Redis is healthy
     */
    async healthCheck() {
        if (!this.isConnected) {
            return {
                status: 'disconnected',
                message: 'Redis client not connected'
            };
        }

        try {
            const pong = await this.client.ping();
            return {
                status: 'healthy',
                response: pong,
                connected: true
            };
        } catch (error) {
            return {
                status: 'error',
                message: error.message,
                connected: false
            };
        }
    }

    /**
     * Close connection
     */
    async close() {
        if (this.client) {
            await this.client.quit();
            console.log('[RedisCache] Connection closed');
        }
    }
}

module.exports = RedisCache;
