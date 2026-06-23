<div align="center">

# circuit-price-feed

**Real-time Solana pricing engine for the Circuit swarm. Serves sub-second, reserve-based token prices, OHLCV candles, discovery feeds, and slippage estimates over a small HTTP API — reading the live on-chain data that circuit-indexer writes into Redis. The data backbone the agents use to discover, score, and monitor trades.**

[![Node.js](https://img.shields.io/badge/node-%3E%3D18-brightgreen)](https://nodejs.org)
[![Version](https://img.shields.io/badge/version-1.0.0-blue)](https://github.com/Circuit-LLM/circuit-price-feed/releases)
[![Status](https://img.shields.io/badge/status-stable-brightgreen)](https://github.com/Circuit-LLM/circuit-price-feed)
[![License](https://img.shields.io/badge/license-proprietary-lightgrey)](#license)

> **Internal service.** circuit-price-feed is the Circuit swarm's real-time price backbone. It binds to loopback and requires a Redis populated by circuit-indexer; it is a read-only edge over that pipeline, not a standalone public API.

[Website](https://circuitllm.xyz) · [OPS Terminal](https://circuitllm.xyz/data) · [Telegram](https://t.me/circuitllm) · [X / Twitter](https://x.com/CircuitLLM)

</div>

---

**[What it does](#what-it-does)** · **[How it works](#how-it-works)** · **[Endpoints](#endpoints)** · **[Before you start](#before-you-start)** · **[Quick Start](#quick-start)** · **[Configuration](#configuration)** · **[Deployment](#deployment)** · **[Project layout](#project-layout)** · **[Docs](#docs)**

---

## What it does

- **Serves sub-second token prices** — reserve-based SOL + USD prices for any indexed pool, read straight from Redis with zero load on the indexer.
- **OHLCV candles + discovery feeds** — `/candles` ring buffers (1m/5m/1h/1d), `/active` (what's trading right now), `/losers` (dip-reversal discovery), and `/trending`.
- **Slippage estimates** — buy/sell price-impact from live AMM reserves; the pre-trade check the agents run before every fill.
- **Resilient sourcing** — Redis first, Pump.fun PDA / AMM reserves on a cache miss, Jupiter v3 as the final fallback. The SOL/USD oracle falls back to on-chain Redis, so it never hard-depends on an external API.
- **Read-only and lightweight** — loopback HTTP, ~2% CPU; it never writes to Redis or the chain.

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

## Before you start

| Requirement | Why | Notes |
|---|---|---|
| **Node.js ≥ 18** | Runtime | `node --version` |
| **Redis** | Primary data source | Must be populated by [circuit-indexer](https://github.com/Circuit-LLM/circuit-indexer) |
| **Solana RPC** | Cache-miss lookups | PDA / pool-reserve reads for unindexed mints (Helius, Triton, QuickNode, etc.) |

---

## Quick Start

```bash
git clone https://github.com/Circuit-LLM/circuit-price-feed
cd circuit-price-feed
npm install
cp .env.example .env      # then fill in real values
npm start                 # or: npm run dev  (node --watch)
```

Requires **Node ≥ 18** and a reachable Redis populated by `circuit-indexer`. Once running, it listens on `127.0.0.1:18941` by default:

```bash
curl localhost:18941/health
curl "localhost:18941/active?limit=10&minTxns=2"
```

---

## Configuration

All configuration is via environment variables. See [`.env.example`](.env.example):

| Var | Default | Purpose |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection (where circuit-indexer writes prices/candles) |
| `PORT` | `18941` | HTTP listen port (loopback-only in production) |
| `CIRCUIT_RPC_URL` | — | Solana RPC for PDA / pool-reserve lookups on a cache miss |

**Never commit `.env`** — it contains the Redis password and RPC key. It is gitignored.

---

## Deployment

Runs as a systemd user service (`circuit-price-feed.service`) on the swarm VPS, bound to
`127.0.0.1:18941`. It is a lightweight reader (~2% CPU); the heavy lifting is in
`circuit-indexer`.

```ini
[Unit]
Description=circuit-price-feed
After=network-online.target redis.service
Wants=network-online.target

[Service]
Type=simple
User=watchtower
WorkingDirectory=/home/watchtower/circuit-price-feed
ExecStart=/usr/bin/node index.js
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=circuit-price-feed
EnvironmentFile=/home/watchtower/circuit-price-feed/.env

[Install]
WantedBy=default.target
```

---

## Project layout

```
index.js          HTTP API + endpoint handlers
lib/redis.js      Redis client + candle/trending/price accessors
lib/prices.js     reserve-based price resolution (Pump.fun PDA, AMM reserves) + Jupiter fallback
lib/sol-price.js  SOL/USD oracle (Jupiter lite-api + on-chain Redis fallback)
lib/slippage.js   AMM price-impact estimation
```

---

## Changelog

### v1.0.0
- Real-time price engine reading circuit-indexer's Redis data. Reserve-based SOL + USD prices with a Pump.fun PDA / AMM-reserve resolver and a Jupiter v3 fallback; a SOL/USD oracle that falls back to on-chain Redis; OHLCV candle ring buffers (1m/5m/1h/1d); `/active` and `/losers` cached discovery feeds; buy/sell slippage estimation; and `/warm` + `/register` write-through helpers. Read-only, loopback-bound, ~2% CPU.

---

## Docs

- [Security policy](SECURITY.md) — disclosure process and operational safety notes
- [circuit-indexer](https://github.com/Circuit-LLM/circuit-indexer) — the pipeline that writes the prices, candles, and pools this service reads
- [OPS Terminal](https://circuitllm.xyz/data) — live source health, endpoint status, and stack stats

### Part of the Circuit stack

- [circuit-geyser](https://github.com/Circuit-LLM/circuit-geyser) — Agave validator Geyser plugin
- [circuit-indexer](https://github.com/Circuit-LLM/circuit-indexer) — stream consumer, pool parser, Redis/Postgres writer
- **circuit-price-feed** — this repo, the real-time price API the agents read
- [circuit-node](https://github.com/Circuit-LLM/circuit-node) — RPC aggregator + data API
- [circuit-agent](https://github.com/Circuit-LLM/circuit-agent) — autonomous trading agent
- [circuitllm.xyz](https://circuitllm.xyz) — website and data terminal

---

## License

Proprietary — Circuit LLM. Internal use only.

---

## Community

- **X / Twitter:** [@CircuitLLM](https://x.com/CircuitLLM)
- **Telegram:** [t.me/circuitllm](https://t.me/circuitllm)
- **Website:** [circuitllm.xyz](https://circuitllm.xyz)
