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
//   GET  /active?limit=100&minTxns=2    — recently-active token universe (30s cached)
//   GET  /scan?limit=30&minLiquidity=&seed=m1,m2 — server-side dip-reversal candidate build (one request)
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

// Compute (and 30s-cache) the recent-activity token list. Extracted so /active and /scan
// share one SCAN — the expensive 5m-candle key sweep runs at most once per 30s regardless
// of which endpoint is hit. Throws 'Redis not available' when the client is down.
async function getActiveList() {
  const now = Date.now();
  if (_activeCache && now - _activeCacheTs < ACTIVE_TTL_MS) return _activeCache;

  const r = await redis.getClient();
  if (!r) throw new Error('Redis not available');

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
  return active;
}

app.get('/active', async (req, res) => {
  const limit   = Math.min(Math.max(parseInt(req.query.limit ?? '100', 10), 1), 300);
  const minTxns = Math.max(parseInt(req.query.minTxns ?? '2', 10), 0);
  const wasCached = !!(_activeCache && Date.now() - _activeCacheTs < ACTIVE_TTL_MS);
  try {
    const active = await getActiveList();
    const out = active.filter(t => t.recentTxns >= minTxns).slice(0, limit);
    res.json({ tokens: out, count: out.length, total: active.length, solUsd: solPrice.getSolPrice(), cached: wasCached });
  } catch (err) {
    res.status(err.message === 'Redis not available' ? 503 : 500).json({ error: err.message });
  }
});

// ── /scan?limit=30&minLiquidity=80000&seed=mint1,mint2 ──────────────────────────
// Server-side dip-reversal candidate builder. Does the whole universe → price → OHLCV
// enrichment in ONE request, co-located with Redis. This replaces the ~200-request-per-scan
// per-mint fan-out that circuit-agent used to run client-side (lib/circuit.js scanFree),
// which only worked for VPS-co-located agents hitting 127.0.0.1:18941 directly — remote
// agents (running the bot on their own machine) got nothing on localhost:18941, fell back
// to the paid DexScreener scan every cycle, and never saw the live Geyser feed. Remote
// agents now reach this via the circuit-data-api /api/price-feed/scan proxy in a single
// rate-limit-friendly call. Returns candidates in the EXACT shape circuit-agent/lib/scoring.js
// consumes, so scoring/gates are unchanged vs the old client-side build.
const SCAN_WSOL = 'So11111111111111111111111111111111111111112';

async function buildScanCandidates({ limit, minLiquidity, seedMints }) {
  const solUsd = solPrice.getSolPrice() || 150;

  // Universe = RECENTLY-ACTIVE tokens (real trades in the last ~15 min). /trending (cumulative
  // volume) is the fallback only when /active is empty, so discovery never goes blind.
  let universeMints = [];
  try {
    const active = await getActiveList();
    universeMints = active.filter(t => t.recentTxns >= 2).map(t => t.mint);
  } catch { /* fall through to trending */ }
  if (!universeMints.length) {
    const trending = await redis.getTrending(50);
    universeMints = trending.map(t => t.mint).filter(Boolean);
  }

  // seedMints (currently-dipping tokens from the Geyser losers feed) placed FIRST so they are
  // always evaluated even if they fall outside the active top-N. De-dupe preserving seed-first
  // order; skip wrapped SOL (quote asset — degenerate self-priced candles).
  const topMints = [...new Set([
    ...seedMints.filter(m => m && m !== SCAN_WSOL),
    ...universeMints.filter(m => m && m !== SCAN_WSOL),
  ])];
  if (!topMints.length) return [];

  // Seeds get their own headroom PLUS a full active slice (mirrors the tuned client budget).
  const scanBudget = Math.min(topMints.length, seedMints.length + limit * 2);

  // Batch price lookup (solReserve → liquidityUsd; priceSol → scan-time anchor).
  const priceMap = {};
  for (let i = 0; i < scanBudget; i += 20) {
    Object.assign(priceMap, await resolvePrices(topMints.slice(i, i + 20)));
  }

  const candidates = (await Promise.all(topMints.slice(0, scanBudget).map(async (mint) => {
    try {
      const [c5, c1, cd, cm] = await Promise.all([
        redis.getCandles(mint, '5m', 13),
        redis.getCandles(mint, '1h', 7),
        redis.getCandles(mint, '1d', 2),
        redis.getCandles(mint, '1m', 4),
      ]);
      if (!c5.length) return null; // token not yet indexed

      // % change across the last `n` candles of a series (oldest-of-n open → latest close).
      const pctChange = (arr, n) => {
        if (!arr.length) return 0;
        const open  = arr[Math.max(0, arr.length - n)]?.o;
        const close = arr[arr.length - 1]?.c;
        return open > 0 ? ((close - open) / open) * 100 : 0;
      };
      const sumV = (arr) => arr.reduce((s, c) => s + (c.v ?? 0), 0);
      const sumB = (arr) => arr.reduce((s, c) => s + (c.b ?? 0), 0);
      const sumS = (arr) => arr.reduce((s, c) => s + (c.s ?? 0), 0);

      // Recent metrics over the last ~2 5m candles (~10 min) — a true recent bounce/buy-pressure read.
      const recent5       = c5.slice(-2);
      const priceChange5m = pctChange(recent5, recent5.length);
      const buys5m        = sumB(recent5);
      const sells5m       = sumS(recent5);
      const vol5m         = sumV(recent5) * solUsd;

      // ── Data-quality metadata (feed honesty) — freshness, true single-candle 5m, confidence.
      const lastCandle   = c5[c5.length - 1];
      const lastTsRaw    = lastCandle?.t ?? null;
      const lastTsMs     = lastTsRaw == null ? null : (lastTsRaw > 2e10 ? lastTsRaw : lastTsRaw * 1000);
      const CANDLE_MS    = 300_000; // 5m window
      // Age from the candle's CLOSE (t + window), not its start.
      const dataAgeSec   = lastTsMs == null ? null : Math.max(0, Math.round((Date.now() - (lastTsMs + CANDLE_MS)) / 1000));
      const pc5mTrue     = lastCandle && lastCandle.o > 0 ? ((lastCandle.c - lastCandle.o) / lastCandle.o) * 100 : 0;
      const txns5mTrue   = (lastCandle?.b ?? 0) + (lastCandle?.s ?? 0);
      const totalTxns5m  = buys5m + sells5m;
      const STALE_AGE_SEC = 600; // >10 min since a candle closed ⇒ stale, not live
      const MIN_LIVE_TXNS = 4;   // matches scoring.minActivityTxns5m default
      let confidence = 'high';
      if (dataAgeSec != null && dataAgeSec > STALE_AGE_SEC) confidence = 'stale';
      else if (totalTxns5m < MIN_LIVE_TXNS)                 confidence = 'thin';

      // Sustained-reversal confirmation: a real turn HOLDS across candles (higher low + advancing close).
      let sustainedBounce = false;
      if (c5.length >= 2) {
        const prevC = c5[c5.length - 2];
        const currC = c5[c5.length - 1];
        sustainedBounce = currC.l >= prevC.l * 0.99 && currC.c >= prevC.c * 0.995;
      }

      // S2 — fresh 1m bounce read (the turn is happening NOW, not ~10 min ago).
      let priceChange1m = null, bounceFresh = null;
      if (cm.length >= 2) {
        priceChange1m = +pctChange(cm.slice(-2), 2).toFixed(4);
        const pm = cm[cm.length - 2], cc = cm[cm.length - 1];
        bounceFresh = cc.c >= pm.c * 0.999 && cc.c >= cc.o * 0.995;
      }

      const priceChange1h  = pctChange(c1, 2);                       // most recent ~1h
      const priceChange6h  = pctChange(c1, 6);                       // aggregate of 1h candles
      const priceChange24h = cd.length ? pctChange(cd, 2) : pctChange(c1, 24);

      // Corrupt-candle skip — a bad OHLCV point can produce thousands-of-percent moves.
      if (Math.abs(priceChange5m) > 200 || Math.abs(priceChange1h) > 200) return null;

      const volume1h       = sumV(c1) * solUsd;
      const volume24h      = (cd.length ? sumV(cd) : sumV(c1)) * solUsd;
      const buys1h         = sumB(c1);
      const sells1h        = sumS(c1);

      const solReserve    = priceMap[mint]?.solReserve ?? 0;
      const liquidityUsd  = solReserve > 0 ? solReserve * 2 * solUsd : 0;
      if (liquidityUsd > 0 && liquidityUsd < minLiquidity) return null;

      return {
        mint,
        symbol:         '?',
        name:           '?',
        price:          priceMap[mint]?.priceSol ?? 0,
        priceChange5m:  parseFloat(priceChange5m.toFixed(4)),
        priceChange1h:  parseFloat(priceChange1h.toFixed(4)),
        priceChange6h:  parseFloat(priceChange6h.toFixed(4)),
        priceChange24h: parseFloat(priceChange24h.toFixed(4)),
        liquidity:      liquidityUsd,
        volume5m:       vol5m,
        volume1h:       volume1h,
        volume24h:      volume24h,
        txns5m:  { buys: buys5m, sells: sells5m },
        txns1h:  { buys: buys1h, sells: sells1h },
        dataAgeSec,
        stale:          confidence === 'stale',
        pc5mTrue:       parseFloat(pc5mTrue.toFixed(4)),
        txns5mTrue,
        confidence,
        sustainedBounce,
        priceChange1m,   // S2 — fresh 1m bounce magnitude (null if 1m data sparse)
        bounceFresh,     // S2 — is the turn still advancing in the last 1m (null if sparse)
        fdv:     0,
        pairAddress:    null,
        verdict:        null,
        rugRisk:        null,
      };
    } catch { return null; }
  }))).filter(Boolean);

  return candidates;
}

app.get('/scan', async (req, res) => {
  const limit        = Math.min(Math.max(parseInt(req.query.limit ?? '30', 10), 1), 100);
  const minLiquidity = Math.max(Number(req.query.minLiquidity ?? '5000') || 0, 0);
  const seedMints    = (req.query.seed ?? '').split(',').map(s => s.trim()).filter(s => s.length >= 32).slice(0, 80);
  try {
    const candidates = await buildScanCandidates({ limit, minLiquidity, seedMints });
    res.json({ candidates, count: candidates.length, solUsd: solPrice.getSolPrice(), source: 'price-feed-scan' });
  } catch (err) {
    res.status(500).json({ error: err.message, candidates: [], source: 'price-feed-scan' });
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
let _losersInflight = null;        // single-flight: one SCAN at a time, shared by concurrent refreshers
const LOSERS_TTL_MS = 60_000;

// The expensive bit — SCAN ~2k Redis keys + a big pipeline + classify. Kept OFF the request path:
// a background timer refreshes it every 45s and requests serve the (at most ~seconds-stale) cache.
// Previously this ran inline on a cache miss and could exceed the data-api's 5s proxy timeout → 502.
async function computeLosers() {
  const r = await redis.getClient();
  if (!r) throw new Error('Redis not available');
  const now = Date.now();

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
  _losersCacheTs = Date.now();
  return losers;
}

// Single-flight refresh — concurrent callers share one in-flight SCAN instead of stacking them.
function refreshLosers() {
  if (!_losersInflight) {
    _losersInflight = computeLosers()
      .catch((e) => { console.warn('[losers] refresh failed:', e.message); return null; })
      .finally(() => { _losersInflight = null; });
  }
  return _losersInflight;
}

app.get('/losers', async (req, res) => {
  const limit     = Math.min(Math.max(parseInt(req.query.limit ?? '60', 10), 1), 200);
  const minChange = parseFloat(req.query.minChange ?? '-0.1');  // at least this negative (e.g. -0.1)
  // Floor on how far it dropped. Default -100 = no floor (return every dipper). Callers that want
  // to skip deep drops pass e.g. maxChange=-15. NOTE: the filter is `change1h >= maxChange`, so a
  // default of 0 made the bare endpoint impossible (change1h <= -0.1 AND >= 0) → always empty.
  const maxChange = parseFloat(req.query.maxChange ?? '-100');

  // Serve the cache; only the very first request (cold start) waits on a compute. A stale cache is
  // returned immediately and refreshed in the background → no request ever blocks on the SCAN.
  const stale = !_losersCache || Date.now() - _losersCacheTs >= LOSERS_TTL_MS;
  if (!_losersCache) {
    await refreshLosers();
    if (!_losersCache) return res.status(503).json({ error: 'losers warming up — retry shortly' });
  } else if (stale) {
    refreshLosers(); // fire-and-forget; serve what we have now
  }

  const base = _losersCache || [];
  const out = base.filter(l => l.change1h <= minChange && l.change1h >= maxChange).slice(0, limit);
  res.json({ losers: out, count: out.length, total: base.length, cached: !stale });
});

// Keep the cache warm so requests never wait on the SCAN (refresh at 45s, well inside the 60s TTL).
setInterval(refreshLosers, 45_000).unref?.();
setTimeout(refreshLosers, 2_000).unref?.(); // prime shortly after boot

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
