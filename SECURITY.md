# Security

circuit-price-feed is a read-only HTTP service bound to loopback. Security matters.

---

## Supported versions

| Version | Supported |
|---|---|
| 1.0.x (current) | ✅ |

---

## Scope

circuit-price-feed is a read-only edge over the circuit-indexer Redis pipeline. Its security surface is:

- **Redis reads only** — GET/SCAN/pipelined reads; it never writes Redis configuration or exposes Redis to external networks
- **No inbound public listener** — binds to `127.0.0.1` (loopback) in production; not reachable off-host
- **Cache-miss lookups** — read-only Solana RPC and Jupiter price calls for unindexed mints; no transactions, no signing
- **No cryptographic signing or wallet access** — the service holds no private keys and cannot move funds
- **No authentication handling** — credentials (`REDIS_URL`, `CIRCUIT_RPC_URL`) are read from environment variables and never logged

The `/warm` and `/register` POST endpoints write only derived index entries to Redis (a pre-populated price and a pool-by-mint reverse index); they accept no privileged input and perform no on-chain action.

---

## Reporting Vulnerabilities

Please do not open a public GitHub issue for security vulnerabilities.

Email: **security@circuitllm.xyz**

Include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof of concept
- The version of circuit-price-feed and Node.js you are using

We will acknowledge receipt within 48 hours and aim to issue a patch within 7 days for confirmed vulnerabilities.

---

## Responsible Disclosure

We ask that you give us reasonable time to address the issue before public disclosure. We will credit researchers who report valid vulnerabilities.
