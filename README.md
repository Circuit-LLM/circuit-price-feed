# circuit-price-feed

Real-time Solana pricing engine for the **circuit-agent** trading swarm.

It serves sub-second, reserve-based token prices and OHLCV candles over a small HTTP API,
reading live on-chain data that the **circuit-indexer** (Triton gRPC → Geyser) writes into
Redis. It is the data backbone the agents use to discover, score, and monitor dip-reversal
trades.

---

## How it works

```
 Solana validators
        │  (Yellowstone/Triton gRPC swap + account stream)
        ▼
 circuit-indexer  ──writes──▶  Redis  ──reads──▶  circuit-price-feed  ──HTTP──▶  agents
                              (prices, candles,                 (this service)
                               trending, pools)
```

- **Primary source — Redis.** The indexer writes live reserve-derived prices
  (`circuit:price:*`, `circuit:price-sol:*`), OHLCV ring buffers
  (`circuit:candles:{1m,5m,1h,1d}:*`), a cumulative-volume trending set
  (`circuit:trending`), and pool state (`circuit:pool:*`). This service only **reads** Redis —
  it adds **zero** load to the (CPU-bound) indexer.
- **Fallback — Jupiter Price API v3** for any token not yet in Redis (exotic mints,
  unindexed pools).
- **SOL/USD oracle.** `lib/sol-price.js` polls Jupiter's free `lite-api` for SOL/USD, and
  **falls back to the indexer's on-chain `circuit:price:SOL`** (raydium-clmm) when Jupiter is
  unavailable or rate-limited — so the oracle has no hard external dependency and survives
  restarts.

---

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | service health + Redis status + SOL price |
| `GET` | `/sol-price` | current SOL/USD price from the oracle |
| `GET` | `/price/:mint` | single token price (SOL + USD, source, age, reserves) |
| `GET` | `/prices?mints=m1,m2,...` | batch price lookup (up to 20 mints) |
| `GET` | `/token/:mint` | enriched token card: price + on-chain metadata |
| `GET` | `/trending?limit=20` | top tokens by **cumulative** on-chain volume (max 50) |
| `GET` | `/active?limit=100&minTxns=2` | tokens ranked by **recent** activity (txns + volume in the last ~15 min). Cached 30s. Primary discovery universe for the agents — surfaces tokens that are actually trading *now*, unlike `/trending` (which is dominated by tokens that were busy hours ago). |
| `GET` | `/losers?limit=60&minChange=-0.1` | tokens with a negative 1h change (dip-reversal discovery). Cached 60s. |
| `GET` | `/pool/:poolAccount` | raw pool state from Redis |
| `GET` | `/slippage/sell/:mint?tokenAmount=N&decimals=6` | sell-side price-impact estimate |
| `GET` | `/slippage/buy/:mint?solAmount=N` | buy-side price-impact estimate |
| `GET` | `/history/:mint?limit=100` | short-term price tick history |
| `GET` | `/candles/:mint?window=1m\|5m\|1h\|1d&limit=100` | OHLCV ring buffer (`{t,o,h,l,c,v,n,b,s}`: time, OHLC, volume, ticks, buys, sells) |
| `POST` | `/warm` | `{mint}` — pre-populate Redis for a freshly bought token |
| `POST` | `/register` | `{mint, poolAccount}` — write a pool-by-mint reverse index |

> **Note:** there is no `6h` candle window — the indexer stores `1m / 5m / 1h / 1d`. Derive 6h
> by aggregating `1h` candles. `circuit:trending` is ranked by *cumulative* volume, so prefer
> `/active` for "what's trading right now."

### Design note: cached scans
`/active` and `/losers` perform a Redis `SCAN` over candle keys, which is too expensive to run
per request. Both compute once and cache the result (30s / 60s) for all callers — the result
barely changes within that window. Read-only and pipelined; safe to call at the swarm's scan
cadence.

---

## Setup

```bash
npm install
cp .env.example .env      # then fill in real values
npm start                 # or: npm run dev  (node --watch)
```

Requires **Node ≥ 18** and a reachable Redis populated by `circuit-indexer`.

### Environment

See [`.env.example`](.env.example):

| Var | Purpose |
|---|---|
| `REDIS_URL` | Redis connection (where circuit-indexer writes prices/candles) |
| `PORT` | HTTP listen port (default `18941`, loopback-only in prod) |
| `CIRCUIT_RPC_URL` | Solana RPC for PDA / pool-reserve lookups on cache miss |

**Never commit `.env`** — it contains the Redis password and RPC key. It is gitignored.

---

## Deployment

Runs as a systemd user service (`circuit-price-feed.service`) on the swarm VPS, bound to
`127.0.0.1:18941`. It is a lightweight reader (~2% CPU); the heavy lifting is in
`circuit-indexer`.

## Project layout

```
index.js          HTTP API + endpoint handlers
lib/redis.js      Redis client + candle/trending/price accessors
lib/prices.js     reserve-based price resolution (Pump.fun PDA, AMM reserves) + Jupiter fallback
lib/sol-price.js  SOL/USD oracle (Jupiter lite-api + on-chain Redis fallback)
lib/slippage.js   AMM price-impact estimation
```

## License

Proprietary — Circuit LLM. Internal use only.
