// circuit-price-feed — Real-time Solana pricing engine for the circuit-agent swarm.
//
// Reads live reserve-based prices from Redis (circuit-indexer → Triton gRPC).
// Falls back to Jupiter Price API v3 for any miss (exotic mints, unindexed pools).
// All price responses include priceUsd enriched from cached SOL/USD oracle price.
//
// Endpoints:
//   GET  /health                        — service health + Redis status + SOL price
//   GET  /sol-price                     — current SOL/USD price from oracle
//   GET  /price/:mint                   — single token price (SOL + USD, source, age, reserves)
//   GET  /prices?mints=m1,m2,...        — batch up to 20 mints
//   GET  /token/:mint                   — enriched token card: price + on-chain metadata
//   GET  /trending?limit=20             — top tokens by on-chain volume (SOL, from CPMM swaps)
//   GET  /pool/:poolAccount             — raw pool state from Redis
//   GET  /slippage/sell/:mint           — ?tokenAmount=N&decimals=6 — sell-side impact estimate
//   GET  /slippage/buy/:mint            — ?solAmount=N — buy-side impact estimate
//   GET  /history/:mint                 — ?limit=100 — short-term price tick history
//   GET  /candles/:mint                 — ?window=1m|5m|1h|1d&limit=100 — OHLCV ring buffer
//   POST /warm                          — {mint} — pre-populate Redis for a freshly bought token
//   POST /register                      — {mint, poolAccount} — write pool-by-mint reverse index
'use strict';

process.stdout.on('error', e => { if (e.code !== 'EPIPE') throw e; });

const express  = require('express');
const redis    = require('./lib/redis');
const solPrice = require('./lib/sol-price');
const { resolvePrice, resolvePrices } = require('./lib/prices');
const { estimateAmmSolOut, estimateAmmTokenOut } = require('./lib/slippage');

const PORT      = parseInt(process.env.PORT ?? '18941', 10);
const startedAt = Date.now();

const VALID_WINDOWS = new Set(['1m', '5m', '1h', '1d']);

// Start SOL/USD background poller immediately
solPrice.start();

const app = express();
app.use(express.json());

// ── /health ───────────────────────────────────────────────────────────────────

app.get('/health', async (_req, res) => {
  const redisOk = await redis.isConnected();
  const sol     = solPrice.getSolPriceInfo();
  res.json({
    status:    redisOk ? 'ok' : 'degraded',
    redis:     redisOk,
    solUsd:    sol.price,
    solUpdatedAt: sol.updatedAt ? new Date(sol.updatedAt).toISOString() : null,
    uptimeMs:  Date.now() - startedAt,
  });
});

// ── /sol-price ────────────────────────────────────────────────────────────────

app.get('/sol-price', (_req, res) => {
  const sol = solPrice.getSolPriceInfo();
  if (!sol.price) return res.status(503).json({ error: 'SOL price not yet available' });
  res.json({
    price:     sol.price,
    symbol:    'SOL',
    currency:  'USD',
    updatedAt: sol.updatedAt ? new Date(sol.updatedAt).toISOString() : null,
    source:    'jupiter-lite',
  });
});

// ── /price/:mint ──────────────────────────────────────────────────────────────

app.get('/price/:mint', async (req, res) => {
  const { mint } = req.params;
  if (!mint || mint.length < 32) return res.status(400).json({ error: 'invalid mint' });

  const result = await resolvePrice(mint);
  if (!result) return res.status(404).json({ error: 'price not found', mint });

  res.json({ mint, ...result });
});

// ── /prices?mints=m1,m2,... ───────────────────────────────────────────────────

app.get('/prices', async (req, res) => {
  const raw   = req.query.mints ?? '';
  const mints = raw.split(',').map(s => s.trim()).filter(s => s.length >= 32);
  if (!mints.length) return res.status(400).json({ error: 'mints query param required' });
  if (mints.length > 20) return res.status(400).json({ error: 'max 20 mints per request' });

  const results  = await resolvePrices(mints);
  const solUsd   = solPrice.getSolPrice();
  res.json({ results, solUsd, count: mints.length });
});

// ── /token/:mint ──────────────────────────────────────────────────────────────
// Enriched token card: on-chain metadata merged with live price data.
// One call = everything you need to display a token.

app.get('/token/:mint', async (req, res) => {
  const { mint } = req.params;
  if (!mint || mint.length < 32) return res.status(400).json({ error: 'invalid mint' });

  const [priceResult, mintMeta] = await Promise.all([
    resolvePrice(mint),
    redis.get(`circuit:mint:${mint}`),
  ]);

  if (!priceResult && !mintMeta) {
    return res.status(404).json({ error: 'token not found', mint });
  }

  // Merge metadata + price; both may be null individually
  const meta = mintMeta ?? {};
  const price = priceResult ?? {};

  res.json({
    mint,
    // On-chain metadata
    symbol:          meta.symbol          ?? null,
    name:            meta.name            ?? null,
    decimals:        meta.decimals        ?? null,
    supply:          meta.supply          ?? null,
    tokenProgram:    meta.tokenProgram    ?? null,
    mintAuthority:   meta.mintAuthority   ?? null,
    freezeAuthority: meta.freezeAuthority ?? null,
    indexedAt:       meta.indexedAt       ? new Date(meta.indexedAt).toISOString() : null,
    // Live price
    priceSol:        price.priceSol       ?? null,
    priceUsd:        price.priceUsd       ?? null,
    solUsd:          price.solUsd         ?? null,
    poolAccount:     price.poolAccount    ?? null,
    poolType:        price.source         ?? null,
    priceAgeMs:      price.ageMs          ?? null,
    priceSource:     price.source         ?? null,
    // Pool reserves (if available — for slippage estimation)
    reserves:        price.reserves       ?? null,
    updatedAt: new Date().toISOString(),
  });
});

// ── /trending?limit=20 ────────────────────────────────────────────────────────
// Top tokens by accumulated on-chain SOL volume (Raydium CPMM swaps indexed live).
// Volume is cumulative since indexer start — rank is relative, not a rolling window.

app.get('/trending', async (req, res) => {
  const limit = Math.min(Math.max(parseInt(req.query.limit ?? '20', 10), 1), 50);

  const trending = await redis.getTrending(limit);
  if (!trending.length) {
    return res.json({ tokens: [], count: 0, note: 'volume data accumulates from CPMM swaps — check back soon' });
  }

  // Enrich with live prices in parallel
  const enriched = await Promise.all(
    trending.map(async ({ mint, volumeSol }) => {
      const p = await resolvePrice(mint);
      return {
        mint,
        volumeSol,
        priceSol:  p?.priceSol  ?? null,
        priceUsd:  p?.priceUsd  ?? null,
        source:    p?.source    ?? null,
        priceAgeMs: p?.ageMs    ?? null,
      };
    })
  );

  res.json({
    tokens:    enriched,
    count:     enriched.length,
    solUsd:    solPrice.getSolPrice(),
    updatedAt: new Date().toISOString(),
  });
});

// ── /active?limit=100&minTxns=2 ─────────────────────────────────────────────────
// Tokens ranked by RECENT trading activity (txns + volume in the last ~15 min), NOT
// cumulative volume like /trending. This is the dip-reversal discovery universe: only
// tokens actually trading right now can produce a tradeable bounce, and their candle
// data (5m change, buy ratio) is statistically meaningful. Ranking by cumulative volume
// surfaces tokens that were busy hours ago and are now dead.
//
// Best-practice resource model: ONE pipelined Redis scan computed on a 30s cache and
// served to all callers — never per-request (a SCAN per request would be expensive and
// the result barely changes within 30s). Read-only; adds ZERO load to the indexer.
let _activeCache   = null;
let _activeCacheTs = 0;
const ACTIVE_TTL_MS  = 30_000;       // recent activity shifts faster than the 60s losers list
const ACTIVE_STALE_MS = 20 * 60_000; // ignore tokens with no 5m candle in the last 20 min

app.get('/active', async (req, res) => {
  const limit   = Math.min(Math.max(parseInt(req.query.limit ?? '100', 10), 1), 300);
  const minTxns = Math.max(parseInt(req.query.minTxns ?? '2', 10), 0);

  const now = Date.now();
  if (_activeCache && now - _activeCacheTs < ACTIVE_TTL_MS) {
    const out = _activeCache.filter(t => t.recentTxns >= minTxns).slice(0, limit);
    return res.json({ tokens: out, count: out.length, total: _activeCache.length, solUsd: solPrice.getSolPrice(), cached: true });
  }

  try {
    const r = await redis.getClient();
    if (!r) return res.status(503).json({ error: 'Redis not available' });

    // Scan 5m candle keys (~835 tokens) — widest coverage with dense enough txn counts.
    const keys = [];
    let cursor = '0';
    do {
      const [next, batch] = await r.scan(cursor, 'MATCH', 'circuit:candles:5m:*', 'COUNT', '500');
      cursor = next;
      keys.push(...batch);
    } while (cursor !== '0');

    // Pipeline the newest 3 5m candles per token (~last 15 min of activity).
    const pipe = r.pipeline();
    for (const k of keys) pipe.lrange(k, 0, 2);
    const results = await pipe.exec();

    const active = [];
    for (let i = 0; i < keys.length; i++) {
      const mint = keys[i].replace('circuit:candles:5m:', '');
      if (LOSERS_STABLE.has(mint)) continue;
      const raw = results[i]?.[1] ?? [];
      if (!raw.length) continue;
      let cs;
      try { cs = raw.map(x => JSON.parse(x)); } catch { continue; }
      const newest = cs[0];
      if (!newest || now - newest.t > ACTIVE_STALE_MS) continue;   // gone quiet → skip
      const recentTxns   = cs.reduce((s, c) => s + (c.b || 0) + (c.s || 0), 0);
      if (recentTxns < 1) continue;                                // no trades → skip
      const recentVolSol = cs.reduce((s, c) => s + (c.v || 0), 0);
      active.push({
        mint,
        recentTxns,
        recentVolSol: parseFloat(recentVolSol.toFixed(4)),
        ageMin: parseFloat(((now - newest.t) / 60_000).toFixed(1)),
      });
    }
    // Rank by recent txn count, then recent SOL volume.
    active.sort((a, b) => b.recentTxns - a.recentTxns || b.recentVolSol - a.recentVolSol);
    _activeCache   = active;
    _activeCacheTs = now;

    const out = active.filter(t => t.recentTxns >= minTxns).slice(0, limit);
    res.json({ tokens: out, count: out.length, total: active.length, solUsd: solPrice.getSolPrice(), cached: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── /pool/:poolAccount ────────────────────────────────────────────────────────
// Raw pool state from Redis — useful for LP tools and depth analysis.

app.get('/pool/:poolAccount', async (req, res) => {
  const { poolAccount } = req.params;
  if (!poolAccount || poolAccount.length < 32) return res.status(400).json({ error: 'invalid poolAccount' });

  const pool = await redis.get(`circuit:pool:${poolAccount}`);
  if (!pool) return res.status(404).json({ error: 'pool not found', poolAccount });

  res.json({ poolAccount, ...pool });
});

// ── /slippage/sell/:mint ──────────────────────────────────────────────────────
// Estimated SOL received when selling tokenAmount raw units into the pool.

app.get('/slippage/sell/:mint', async (req, res) => {
  const { mint }    = req.params;
  const tokenAmount = parseFloat(req.query.tokenAmount ?? '0');
  const decimals    = parseInt(req.query.decimals ?? '6', 10);

  if (!mint || mint.length < 32) return res.status(400).json({ error: 'invalid mint' });
  if (!tokenAmount || tokenAmount <= 0) return res.status(400).json({ error: 'tokenAmount required' });

  const priceResult = await resolvePrice(mint);
  if (!priceResult) return res.status(404).json({ error: 'price not found', mint });

  const reserves = priceResult.reserves;
  if (!reserves?.coinReserve) {
    const tokenHuman      = tokenAmount / Math.pow(10, decimals);
    const estimatedSolOut = priceResult.priceSol * tokenHuman;
    return res.json({
      mint, side: 'sell', tokenAmount, estimatedSolOut,
      priceImpactPct: null, effectivePriceSol: priceResult.priceSol,
      source: priceResult.source,
      note: 'no pool reserves — mid-price estimate only',
    });
  }

  const est = estimateAmmSolOut({ ...reserves, coinDecimals: reserves.coinDecimals ?? decimals }, tokenAmount);
  if (!est) return res.status(500).json({ error: 'slippage calculation failed' });

  res.json({
    mint, side: 'sell', tokenAmount,
    estimatedSolOut:   est.estimatedSolOut,
    priceImpactPct:    est.priceImpactPct,
    effectivePriceSol: est.effectivePriceSol,
    source: priceResult.source,
  });
});

// Keep legacy path working for existing agents
app.get('/slippage/:mint', async (req, res) => {
  req.url = `/slippage/sell/${req.params.mint}${req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : ''}`;
  app.handle(req, res);
});

// ── /slippage/buy/:mint ───────────────────────────────────────────────────────
// Estimated tokens received when buying with solAmount SOL.

app.get('/slippage/buy/:mint', async (req, res) => {
  const { mint }  = req.params;
  const solAmount = parseFloat(req.query.solAmount ?? '0');
  const decimals  = parseInt(req.query.decimals ?? '6', 10);

  if (!mint || mint.length < 32) return res.status(400).json({ error: 'invalid mint' });
  if (!solAmount || solAmount <= 0) return res.status(400).json({ error: 'solAmount required' });

  const priceResult = await resolvePrice(mint);
  if (!priceResult) return res.status(404).json({ error: 'price not found', mint });

  const reserves = priceResult.reserves;
  if (!reserves?.coinReserve) {
    const estimatedTokensOut = (priceResult.priceSol > 0)
      ? solAmount / priceResult.priceSol
      : null;
    return res.json({
      mint, side: 'buy', solAmount, estimatedTokensOut,
      priceImpactPct: null, effectivePriceSol: priceResult.priceSol,
      source: priceResult.source,
      note: 'no pool reserves — mid-price estimate only',
    });
  }

  const est = estimateAmmTokenOut({ ...reserves, coinDecimals: reserves.coinDecimals ?? decimals }, solAmount);
  if (!est) return res.status(500).json({ error: 'slippage calculation failed' });

  res.json({
    mint, side: 'buy', solAmount,
    estimatedTokensOut: est.estimatedTokensOut,
    priceImpactPct:     est.priceImpactPct,
    effectivePriceSol:  est.effectivePriceSol,
    source: priceResult.source,
  });
});

// ── /history/:mint ────────────────────────────────────────────────────────────
// Short-term price tick history from Redis ring buffer (populated by circuit-indexer).
// Returns oldest-first array of {p: priceSol, ts: unixMs}.

app.get('/history/:mint', async (req, res) => {
  const { mint } = req.params;
  const limit    = Math.min(Math.max(parseInt(req.query.limit ?? '100', 10), 1), 300);

  if (!mint || mint.length < 32) return res.status(400).json({ error: 'invalid mint' });

  const history  = await redis.getPriceHistory(mint, limit);
  const solUsd   = solPrice.getSolPrice();

  if (!history.length) {
    // Fall back to current price as a single-point history
    const current = await resolvePrice(mint);
    if (current) {
      return res.json({
        mint,
        ticks: [{ p: current.priceSol, ts: Date.now() }],
        count: 1, solUsd,
        note: 'history accumulates from live indexer — only current price available',
      });
    }
    return res.status(404).json({ error: 'no price history available', mint });
  }

  res.json({ mint, ticks: history, count: history.length, solUsd });
});

// ── /candles/:mint ────────────────────────────────────────────────────────────
// OHLCV candlestick data from Redis ring buffer.
// ?window=1m|5m|1h|1d  (default 5m)
// ?limit=100           (max varies by window)
// Returns oldest-first array of {t, o, h, l, c, v, n} (openTime ms, OHLCV, ticks).

app.get('/candles/:mint', async (req, res) => {
  const { mint }  = req.params;
  const window    = VALID_WINDOWS.has(req.query.window) ? req.query.window : '5m';
  const limit     = Math.min(Math.max(parseInt(req.query.limit ?? '100', 10), 1), 300);

  if (!mint || mint.length < 32) return res.status(400).json({ error: 'invalid mint' });

  const candles = await redis.getCandles(mint, window, limit);

  if (!candles.length) {
    return res.status(404).json({
      error: 'no candle data available',
      mint, window,
      note: 'candles accumulate from live indexer — data available within minutes of first trade',
    });
  }

  res.json({ mint, window, candles, count: candles.length });
});

// ── /losers ───────────────────────────────────────────────────────────────────
// Tokens with negative 1h price change, computed from on-chain OHLCV candles
// (primary, ~400 tokens) plus price-history ring buffers (wider, ~925 tokens).
// Used by circuit-data-api scan route as primary discovery source for dip-reversal.
// Results cached 60s — Redis SCANs on every request would be too expensive.

const LOSERS_STABLE = new Set([
  'So11111111111111111111111111111111111111112',  // WSOL
  'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
  'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
]);

let _losersCache    = null;
let _losersCacheTs  = 0;
const LOSERS_TTL_MS = 60_000;

app.get('/losers', async (req, res) => {
  const limit     = Math.min(Math.max(parseInt(req.query.limit ?? '60', 10), 1), 200);
  const minChange = parseFloat(req.query.minChange ?? '-0.1');  // at least this negative (e.g. -0.1)
  // Floor on how far it dropped. Default -100 = no floor (return every dipper). Callers that want
  // to skip deep drops pass e.g. maxChange=-15. NOTE: the filter is `change1h >= maxChange`, so a
  // default of 0 made the bare endpoint impossible (change1h <= -0.1 AND >= 0) → always empty.
  const maxChange = parseFloat(req.query.maxChange ?? '-100');

  const now = Date.now();
  if (_losersCache && now - _losersCacheTs < LOSERS_TTL_MS) {
    const out = _losersCache
      .filter(l => l.change1h <= minChange && l.change1h >= maxChange)
      .slice(0, limit);
    return res.json({ losers: out, count: out.length, total: _losersCache.length, cached: true });
  }

  try {
    const r = await redis.getClient();
    if (!r) return res.status(503).json({ error: 'Redis not available' });

    // ── Source 1: completed 1h OHLCV candles ─────────────────────────────────
    const candleKeys = [];
    let cursor = '0';
    do {
      const [next, batch] = await r.scan(cursor, 'MATCH', 'circuit:candles:1h:*', 'COUNT', '500');
      cursor = next;
      candleKeys.push(...batch);
    } while (cursor !== '0');

    // ── Source 2: price history ring buffers (wider coverage, ~925 tokens) ───
    const phKeys = [];
    cursor = '0';
    do {
      const [next, batch] = await r.scan(cursor, 'MATCH', 'circuit:ph:*', 'COUNT', '500');
      cursor = next;
      phKeys.push(...batch);
    } while (cursor !== '0');

    // Pipeline both sources together
    const pipe = r.pipeline();
    for (const k of candleKeys) pipe.lrange(k, 0, 0);          // newest closed candle
    for (const k of phKeys)     pipe.lrange(k, 0, 0);          // newest ph tick
    for (const k of phKeys)     pipe.lrange(k, -1, -1);        // oldest ph tick (for change)
    const results = await pipe.exec();

    const seenMints  = new Set();
    const losers     = [];
    const STALE_4H   = 4 * 3_600_000;
    const MIN_SPAN   = 30 * 60_000;   // need at least 30 min of ph history
    const MAX_SPAN   = 2  * 3_600_000; // reject spans > 2h — not a proxy for 1h change
    const MIN_CHANGE = -50;            // -50%+ in 1h is noise or a rug, not a dip-reversal candidate

    // Candle-based losers (most accurate — intra-candle open-to-close)
    for (let i = 0; i < candleKeys.length; i++) {
      const mint = candleKeys[i].replace('circuit:candles:1h:', '');
      if (LOSERS_STABLE.has(mint)) continue;
      const raw  = results[i]?.[1] ?? [];
      if (!raw.length) continue;
      try {
        const c = JSON.parse(raw[0]);
        if (!c.o || c.o <= 0 || !c.c) continue;
        if (now - (c.t + 3_600_000) > STALE_4H) continue;
        const change1h = ((c.c - c.o) / c.o) * 100;
        if (change1h < 0 && change1h >= MIN_CHANGE) {
          losers.push({ mint, change1h: parseFloat(change1h.toFixed(4)), src: 'candle' });
          seenMints.add(mint);
        }
      } catch { continue; }
    }

    // Price-history-based losers (broader coverage — newest vs oldest tick)
    const phBase = candleKeys.length;
    for (let i = 0; i < phKeys.length; i++) {
      const mint = phKeys[i].replace('circuit:ph:', '');
      if (seenMints.has(mint)) continue;       // already covered by candle source
      if (LOSERS_STABLE.has(mint)) continue;   // skip infrastructure tokens
      const newestRaw = results[phBase + i]?.[1]?.[0];
      const oldestRaw = results[phBase + phKeys.length + i]?.[1]?.[0];
      if (!newestRaw || !oldestRaw) continue;
      try {
        const newest = JSON.parse(newestRaw);
        const oldest = JSON.parse(oldestRaw);
        if (!oldest.p || oldest.p <= 0 || !newest.p) continue;
        const span = newest.ts - oldest.ts;
        if (span < MIN_SPAN) continue;   // not enough history yet
        if (span > MAX_SPAN) continue;   // span too wide — oldest tick is >2h old, not a 1h proxy
        if (now - newest.ts > STALE_4H) continue; // token went quiet
        const change1h = ((newest.p - oldest.p) / oldest.p) * 100;
        if (change1h < 0 && change1h >= MIN_CHANGE) {
          losers.push({ mint, change1h: parseFloat(change1h.toFixed(4)), src: 'ph' });
          seenMints.add(mint);
        }
      } catch { continue; }
    }

    losers.sort((a, b) => a.change1h - b.change1h);
    _losersCache   = losers;
    _losersCacheTs = now;

    const out = losers.filter(l => l.change1h <= minChange && l.change1h >= maxChange).slice(0, limit);
    res.json({ losers: out, count: out.length, total: losers.length, cached: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── /warm ─────────────────────────────────────────────────────────────────────
// Pre-populate Redis for a mint that an agent just bought.
// Called immediately after a buy so the first monitor tick (10s later) is a
// Redis hit rather than a DexScreener call. Resolves through the full chain:
// Redis → pool-by-mint → DexScreener (with caching). The DexScreener result, if
// needed, is written back to Redis with a 30s TTL by resolvePrice() itself.

app.post('/warm', async (req, res) => {
  const { mint } = req.body ?? {};
  if (!mint || mint.length < 32) return res.status(400).json({ error: 'invalid mint' });

  const result = await resolvePrice(mint);
  if (!result) return res.status(404).json({ error: 'price not found', mint, note: 'token may not be on-chain yet' });

  res.json({ mint, priceSol: result.priceSol, priceUsd: result.priceUsd, source: result.source, ageMs: result.ageMs });
});

// ── /register ─────────────────────────────────────────────────────────────────

app.post('/register', async (req, res) => {
  const { mint, poolAccount } = req.body ?? {};
  if (!mint || !poolAccount) return res.status(400).json({ error: 'mint and poolAccount required' });
  if (mint.length < 32 || poolAccount.length < 32) return res.status(400).json({ error: 'invalid mint or poolAccount' });

  await redis.set(`circuit:pool-by-mint:${mint}`, poolAccount, 300);
  res.json({ ok: true, mint, poolAccount });
});

// ── Start ─────────────────────────────────────────────────────────────────────

app.listen(PORT, '127.0.0.1', () => {
  console.log(`[circuit-price-feed] listening on 127.0.0.1:${PORT}`);
});
