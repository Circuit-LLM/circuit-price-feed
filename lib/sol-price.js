// lib/sol-price.js — Cached SOL/USD price for enriching token price responses.
//
// Sources SOL/USD from Jupiter's free price API (lite-api.jup.ag), which prices
// wSOL directly. Self-contained within Circuit — depends on NO other local service.
// (Previously polled an external node; that created a cross-system dependency and
//  silently broke this oracle whenever that node was down.)
//
// USD is only used to enrich responses with priceUsd and to convert Jupiter's
// USD-quoted fallback prices into SOL. The core trading loop is fully SOL-denominated
// and does not depend on this value.
//
// Falls back to the last known good value if a poll fails.
'use strict';

const https = require('https');
const redis = require('./redis');

const SOL_MINT      = 'So11111111111111111111111111111111111111112';
// lite-api.jup.ag is Jupiter's free tier. The api.jup.ag host requires a paid key
// and returns HTTP 429 for unauthenticated requests — do not use it here.
const POLL_URL      = `https://lite-api.jup.ag/price/v3?ids=${SOL_MINT}`;
const POLL_INTERVAL = 30_000; // ms — SOL/USD moves slowly; 30s is ample and gentle on the free tier
const TIMEOUT_MS    = 4_000;

let _price     = null;  // last known good SOL/USD price
let _updatedAt = null;  // ms timestamp of last successful fetch
let _started   = false;

function _fetch() {
  return new Promise((resolve) => {
    const req = https.get(POLL_URL, { timeout: TIMEOUT_MS }, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try {
          const d = JSON.parse(body);
          // v3 shape: { "<SOL_MINT>": { usdPrice, decimals, ... } } — no outer "data" wrapper
          const p = parseFloat(d?.[SOL_MINT]?.usdPrice ?? d?.[SOL_MINT]?.price);
          if (p && isFinite(p) && p > 0) resolve(p);
          else resolve(null);
        } catch { resolve(null); }
      });
    });
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.on('error', () => resolve(null));
  });
}

async function _poll() {
  const p = await _fetch();
  if (p) { _price = p; _updatedAt = Date.now(); return; }
  // Jupiter unavailable (e.g. lite-api rate-limited) — fall back to the indexer's on-chain
  // reserve-based SOL/USD price already in Redis (circuit:price:SOL, raydium-clmm). Keeps the
  // oracle alive with no external dependency, and survives restarts (Jupiter's first poll after
  // a restart often hits the rate limit, which previously left _price null).
  try {
    const raw = await redis.get(`circuit:price:${SOL_MINT}`);
    if (raw) {
      const onchain = parseFloat((typeof raw === 'string' ? JSON.parse(raw) : raw).priceUsd);
      if (onchain && isFinite(onchain) && onchain > 0) { _price = onchain; _updatedAt = Date.now(); }
    }
  } catch { /* keep last known good */ }
}

function start() {
  if (_started) return;
  _started = true;
  _poll(); // immediate first fetch
  setInterval(_poll, POLL_INTERVAL).unref();
}

function getSolPrice() {
  return _price;
}

function getSolPriceInfo() {
  return { price: _price, updatedAt: _updatedAt };
}

module.exports = { start, getSolPrice, getSolPriceInfo };
