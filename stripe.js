/**
 * Copyright (c) 2024-2026 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Stripe Integration Module
 * Handles: checkout sessions, webhooks, plan management
 * 
 * Plans:
 *   - free: default, no payment
 *   - pro: €9.90/month
 *   - advanced: €19.90/month
 */

const express = require('express');
const router = express.Router();

// Stripe init - uses STRIPE_SECRET_KEY from env
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);

const WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET;

// Plan config - Stripe Price IDs (set these after creating products in Stripe Dashboard)
const PLANS = {
    pro: {
        priceId: process.env.STRIPE_PRO_PRICE_ID,
        name: 'Pro',
        price: 990, // cents
        features: ['unlimited_favorites', 'pdf_export', 'ai_50', 'cloud_sync', 'advanced_filters', 'comparison_export']
    },
    advanced: {
        priceId: process.env.STRIPE_ADVANCED_PRICE_ID,
        name: 'Advanced',
        price: 1990, // cents
        features: ['unlimited_favorites', 'pdf_branded', 'ai_unlimited', 'cloud_sync', 'advanced_filters', 'comparison_export', 'client_workspace', 'priority_support']
    }
};

// ===================================
// FIREBASE ADMIN (for updating user plan in Firestore)
// ===================================

let db = null;
try {
    const admin = require('firebase-admin');
    if (!admin.apps.length) {
        const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT 
            ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT) 
            : null;
        
        if (serviceAccount) {
            admin.initializeApp({
                credential: admin.credential.cert(serviceAccount)
            });
        } else {
            // Fallback: use application default credentials
            admin.initializeApp();
        }
    }
    db = admin.firestore();
    console.log('  ✅ Firebase Admin initialized for Stripe module');
} catch (err) {
    console.warn('  ⚠️  Firebase Admin not available:', err.message);
    console.warn('     User plan updates will need manual sync');
}

// ===================================
// Helper: Update user plan in Firestore
// ===================================

async function updateUserPlan(firebaseUid, plan, stripeCustomerId, subscriptionId) {
    if (!db) {
        console.warn('[STRIPE] No Firestore connection - cannot update user plan');
        return false;
    }
    
    try {
        await db.collection('users').doc(firebaseUid).set({
            plan: plan, // 'free', 'pro', 'advanced'
            stripeCustomerId: stripeCustomerId,
            stripeSubscriptionId: subscriptionId,
            planUpdatedAt: new Date().toISOString(),
        }, { merge: true });
        
        console.log(`[STRIPE] Updated user ${firebaseUid} to plan: ${plan}`);
        return true;
    } catch (err) {
        console.error('[STRIPE] Failed to update user plan:', err.message);
        return false;
    }
}

// ===================================
// POST /checkout - Create Stripe Checkout Session
// ===================================

router.post('/checkout', async (req, res) => {
    try {
        const { planId, firebaseUid, email, successUrl, cancelUrl } = req.body;
        
        if (!planId || !firebaseUid || !email) {
            return res.status(400).json({ error: 'Missing planId, firebaseUid, or email' });
        }
        
        const plan = PLANS[planId];
        if (!plan || !plan.priceId) {
            return res.status(400).json({ error: `Invalid plan: ${planId}. Set STRIPE_${planId.toUpperCase()}_PRICE_ID in env.` });
        }
        
        // Create or retrieve Stripe customer
        let customer;
        const existingCustomers = await stripe.customers.list({ email: email, limit: 1 });
        
        if (existingCustomers.data.length > 0) {
            customer = existingCustomers.data[0];
            // Update metadata with Firebase UID
            if (customer.metadata.firebaseUid !== firebaseUid) {
                await stripe.customers.update(customer.id, {
                    metadata: { firebaseUid }
                });
            }
        } else {
            customer = await stripe.customers.create({
                email: email,
                metadata: { firebaseUid }
            });
        }
        
        // Create checkout session
        const session = await stripe.checkout.sessions.create({
            customer: customer.id,
            payment_method_types: ['card'],
            line_items: [{
                price: plan.priceId,
                quantity: 1,
            }],
            mode: 'subscription',
            success_url: successUrl || 'https://isin-research.com?payment=success',
            cancel_url: cancelUrl || 'https://isin-research.com?payment=cancelled',
            metadata: {
                firebaseUid,
                planId,
            },
            subscription_data: {
                metadata: {
                    firebaseUid,
                    planId,
                }
            },
            allow_promotion_codes: true,
        });
        
        console.log(`[STRIPE] Checkout session created for ${email} (${planId})`);
        res.json({ url: session.url, sessionId: session.id });
        
    } catch (error) {
        console.error('[STRIPE] Checkout error:', error.message);
        res.status(500).json({ error: 'Failed to create checkout session' });
    }
});

// ===================================
// POST /portal - Customer portal (manage subscription)
// ===================================

router.post('/portal', async (req, res) => {
    try {
        const { email } = req.body;
        
        if (!email) {
            return res.status(400).json({ error: 'Missing email' });
        }
        
        const customers = await stripe.customers.list({ email, limit: 1 });
        if (customers.data.length === 0) {
            return res.status(404).json({ error: 'No subscription found for this email' });
        }
        
        const portalSession = await stripe.billingPortal.sessions.create({
            customer: customers.data[0].id,
            return_url: 'https://isin-research.com',
        });
        
        res.json({ url: portalSession.url });
        
    } catch (error) {
        console.error('[STRIPE] Portal error:', error.message);
        res.status(500).json({ error: 'Failed to create portal session' });
    }
});

// ===================================
// GET /plan/:uid - Get user plan (for frontend verification)
// ===================================

router.get('/plan/:uid', async (req, res) => {
    try {
        const { uid } = req.params;
        
        if (!db) {
            return res.json({ plan: 'free', source: 'no-db' });
        }
        
        const userDoc = await db.collection('users').doc(uid).get();
        
        if (!userDoc.exists) {
            return res.json({ plan: 'free' });
        }
        
        const data = userDoc.data();
        const plan = data.plan || 'free';
        
        res.json({
            plan,
            planUpdatedAt: data.planUpdatedAt || null,
            features: plan === 'free' ? [] : (PLANS[plan]?.features || []),
        });
        
    } catch (error) {
        console.error('[STRIPE] Plan check error:', error.message);
        res.json({ plan: 'free', error: error.message });
    }
});

// ===================================
// GET /plans - Public plan info
// ===================================

router.get('/plans', (req, res) => {
    res.json({
        plans: [
            {
                id: 'free',
                name: 'Free',
                price: 0,
                period: 'month',
                features: [
                    'Ricerca strumenti illimitata',
                    '3 favoriti',
                    'Consultazione bond e certificati',
                ],
                cta: 'Inizia Gratis'
            },
            {
                id: 'pro',
                name: 'Pro',
                price: 9.90,
                period: 'month',
                features: [
                    'Favoriti illimitati',
                    'PDF export (logo ISIN Research)',
                    '50 query AI/mese',
                    'Cloud sync dispositivi',
                    'Filtri avanzati',
                    'Export comparazione',
                ],
                cta: 'Prova Pro',
                popular: true
            },
            {
                id: 'advanced',
                name: 'Advanced',
                price: 19.90,
                period: 'month',
                features: [
                    'Tutto Pro +',
                    'PDF brandizzato (tuo logo)',
                    'Query AI illimitate',
                    'Workspace clienti',
                    'Gestione portafogli',
                    'Supporto prioritario',
                ],
                cta: 'Scegli Advanced'
            }
        ]
    });
});

// ===================================
// WEBHOOK - Stripe events (subscription changes)
// This must be registered with express.raw() body, NOT json
// ===================================

router.post('/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
    let event;
    
    try {
        if (WEBHOOK_SECRET) {
            const sig = req.headers['stripe-signature'];
            event = stripe.webhooks.constructEvent(req.body, sig, WEBHOOK_SECRET);
        } else {
            // Dev mode - no signature verification
            event = JSON.parse(req.body);
            console.warn('[STRIPE] ⚠️  Webhook signature not verified (no WEBHOOK_SECRET)');
        }
    } catch (err) {
        console.error('[STRIPE] Webhook signature failed:', err.message);
        return res.status(400).send(`Webhook Error: ${err.message}`);
    }
    
    console.log(`[STRIPE] Webhook received: ${event.type}`);
    
    try {
        switch (event.type) {
            case 'checkout.session.completed': {
                const session = event.data.object;
                const firebaseUid = session.metadata?.firebaseUid;
                const planId = session.metadata?.planId;
                
                if (firebaseUid && planId) {
                    await updateUserPlan(firebaseUid, planId, session.customer, session.subscription);
                    console.log(`[STRIPE] ✅ User ${firebaseUid} activated ${planId}`);
                }
                break;
            }
            
            case 'customer.subscription.updated': {
                const subscription = event.data.object;
                const firebaseUid = subscription.metadata?.firebaseUid;
                
                if (firebaseUid) {
                    if (subscription.status === 'active') {
                        const planId = subscription.metadata?.planId || 'pro';
                        await updateUserPlan(firebaseUid, planId, subscription.customer, subscription.id);
                    } else if (['canceled', 'unpaid', 'past_due'].includes(subscription.status)) {
                        await updateUserPlan(firebaseUid, 'free', subscription.customer, subscription.id);
                        console.log(`[STRIPE] ⚠️  User ${firebaseUid} downgraded to free (${subscription.status})`);
                    }
                }
                break;
            }
            
            case 'customer.subscription.deleted': {
                const subscription = event.data.object;
                const firebaseUid = subscription.metadata?.firebaseUid;
                
                if (firebaseUid) {
                    await updateUserPlan(firebaseUid, 'free', subscription.customer, null);
                    console.log(`[STRIPE] ❌ User ${firebaseUid} subscription cancelled`);
                }
                break;
            }
            
            case 'invoice.payment_failed': {
                const invoice = event.data.object;
                console.log(`[STRIPE] ⚠️  Payment failed for customer ${invoice.customer}`);
                break;
            }
        }
    } catch (err) {
        console.error(`[STRIPE] Webhook handler error: ${err.message}`);
    }
    
    res.json({ received: true });
});

module.exports = router;
