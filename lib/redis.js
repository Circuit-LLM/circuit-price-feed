// lib/redis.js — Redis client for circuit-price-feed.
// Reads from the same Redis instance as circuit-indexer.
'use strict';

let _client = null;

async function getClient() {
  if (_client) return _client;
  const ioredis = require('ioredis');
  const url = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
  _client = new ioredis(url, { lazyConnect: true, maxRetriesPerRequest: 2 });
  try {
    await _client.connect();
  } catch (e) {
    console.error('[redis] connect failed:', e.message);
    _client = null;
  }
  return _client;
}

async function get(key) {
  const r = await getClient();
  if (!r) return null;
  try {
    const v = await r.get(key);
    return v ? JSON.parse(v) : null;
  } catch { return null; }
}

async function getString(key) {
  const r = await getClient();
  if (!r) return null;
  try { return await r.get(key); } catch { return null; }
}

async function set(key, value, ttlSeconds) {
  const r = await getClient();
  if (!r) return;
  const str = typeof value === 'string' ? value : JSON.stringify(value);
  try {
    if (ttlSeconds) await r.setex(key, ttlSeconds, str);
    else await r.set(key, str);
  } catch { /* non-fatal */ }
}

async function isConnected() {
  const r = await getClient();
  if (!r) return false;
  try { await r.ping(); return true; } catch { return false; }
}

// Trending — ZSET of mints ranked by accumulated on-chain SOL volume
async function getTrending(limit = 20) {
  const r = await getClient();
  if (!r) return [];
  try {
    const raw = await r.zrevrange('circuit:trending', 0, limit - 1, 'WITHSCORES');
    const out = [];
    for (let i = 0; i < raw.length; i += 2) {
      out.push({ mint: raw[i], volumeSol: parseFloat(raw[i + 1]) });
    }
    return out;
  } catch { return []; }
}

// Price history ring buffer — oldest-first, up to limit entries
async function getPriceHistory(mint, limit = 100) {
  const r = await getClient();
  if (!r) return [];
  try {
    const raw = await r.lrange(`circuit:ph:${mint}`, 0, limit - 1);
    return raw.map(e => { try { return JSON.parse(e); } catch { return null; } })
              .filter(Boolean)
              .reverse();
  } catch { return []; }
}

// Candle ring buffer — oldest-first, up to limit candles for the given window
async function getCandles(mint, window, limit = 100) {
  const r = await getClient();
  if (!r) return [];
  try {
    const raw = await r.lrange(`circuit:candles:${window}:${mint}`, 0, limit - 1);
    return raw.map(e => { try { return JSON.parse(e); } catch { return null; } })
              .filter(Boolean)
              .reverse();
  } catch { return []; }
}

module.exports = { getClient, get, getString, set, isConnected, getTrending, getPriceHistory, getCandles };
