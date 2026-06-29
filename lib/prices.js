// lib/prices.js — Price resolution chain for circuit-price-feed.
//
// Resolution order for a given mint:
//   1. circuit:price-sol:{mint}           — set by circuit-indexer for Raydium/Orca/CPMM/PumpSwap pools
//   2. circuit:pool-by-mint:{mint}        — reverse index → fetch pool → compute priceSol from reserves
//      2a. If pumpswap pool record found but vault balances null → RPC fetch vault balances
//   2.5 Pump.fun bonding curve PDA lookup — derive bcAddress from mint → circuit:pool:{bcAddress}
//       (The indexer streams all bonding curve updates but can't write the reverse index because the
//        bonding curve account data does not contain the mint address. PDA derivation closes this gap
//        with zero external calls — all data is already in Redis from gRPC.)
//   2.6 PumpSwap vault RPC fallback (graduated tokens) — when bonding curve returns null (complete=true)
//       and we have the pool address from step 2, fetch vault balances live from Triton RPC.
//       Eliminates the need for external REST APIs for all graduated pump.fun tokens we have ever seen.
//   3. Jupiter Price API v3           — universal last resort for unindexed mints
//
// All resolved prices include priceUsd if the SOL/USD price is available (via sol-price poller).
'use strict';

const redis    = require('./redis');
const https    = require('https');
const solPrice = require('./sol-price');
const { PublicKey } = require('@solana/web3.js');

const SOL_MINT           = 'So11111111111111111111111111111111111111112';
const PUMP_PROGRAM       = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
const PUMPSWAP_PROGRAM   = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
// lite-api.jup.ag is Jupiter's free tier. The api.jup.ag host requires a paid key
// and returns HTTP 429 for unauthenticated requests.
const JUPITER_PRICE_BASE = 'https://lite-api.jup.ag/price/v3';
const JUPITER_TIMEOUT    = 4000; // ms

// ── Pump.fun bonding curve PDA ────────────────────────────────────────────────

// Derives the bonding curve account address for a given mint.
// PDA seeds: ["bonding-curve", mint_bytes], program: Pump.fun
// Deterministic — no RPC call needed.
function _pumpBondingCurveAddress(mint) {
  try {
    const [addr] = PublicKey.findProgramAddressSync(
      [Buffer.from('bonding-curve'), new PublicKey(mint).toBuffer()],
      PUMP_PROGRAM
    );
    return addr.toBase58();
  } catch { return null; }
}

// Parses a pump.fun bonding curve account data buffer.
// Layout (Anchor): [0-7] discriminator, [8-15] virtualTokenReserves u64,
// [16-23] virtualSolReserves u64, [24-31] realTokenReserves u64,
// [32-39] realSolReserves u64, [40-47] tokenTotalSupply u64, [48] complete bool.
// Returns null on any parse failure or if virtualTokenReserves is zero.
function _parseBondingCurveBuffer(buf) {
  if (!Buffer.isBuffer(buf) || buf.length < 49) return null;
  try {
    const vToken   = buf.readBigUInt64LE(8);
    const vSol     = buf.readBigUInt64LE(16);
    const rToken   = buf.readBigUInt64LE(24);
    const rSol     = buf.readBigUInt64LE(32);
    const supply   = buf.readBigUInt64LE(40);
    const complete = buf.readUInt8(48) !== 0;
    if (vToken === 0n) return null;
    return {
      type:                 'pump-bonding-curve',
      virtualTokenReserves: vToken.toString(),
      virtualSolReserves:   vSol.toString(),
      realTokenReserves:    rToken.toString(),
      realSolReserves:      rSol.toString(),
      tokenTotalSupply:     supply.toString(),
      priceRaw:             Number(vSol) / Number(vToken),
      complete,
    };
  } catch { return null; }
}

// Fetches a pump.fun bonding curve account directly from the Triton RPC when
// the gRPC-streamed Redis record has expired (60s TTL, quiet tokens get evicted).
// Returns a pool record compatible with _poolToPriceSol(), or null on any failure.
// Returns null for graduated tokens (complete=true) so the caller falls through
// to the PumpSwap/Raydium path instead of returning a stale bonding curve price.
async function _rpcFetchBondingCurve(bcAddress) {
  const rpcUrl = process.env.CIRCUIT_RPC_URL;
  if (!rpcUrl) return null;

  try {
    const resp = await fetch(rpcUrl, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({
        jsonrpc: '2.0',
        id:      1,
        method:  'getAccountInfo',
        params:  [bcAddress, { encoding: 'base64', commitment: 'confirmed' }],
      }),
      signal: AbortSignal.timeout(3_000),
    });
    if (!resp.ok) return null;

    const json = await resp.json();
    const val  = json?.result?.value;
    if (!val) return null;

    // Verify the account is owned by the pump.fun program.
    if (val.owner !== PUMP_PROGRAM.toBase58()) return null;

    const buf  = Buffer.from(val.data[0], 'base64');
    const pool = _parseBondingCurveBuffer(buf);
    if (!pool) return null;

    // Graduated tokens should resolve via their PumpSwap pool, not the
    // bonding curve — prices diverge post-migration as the curve is no longer traded.
    if (pool.complete) return null;

    const record = { poolAccount: bcAddress, ts: Date.now(), updatedAt: Date.now(), ...pool };

    // Write back to Redis to serve subsequent ticks from cache (same 60s TTL as
    // the gRPC stream path) so we don't make an RPC call on every monitor tick.
    redis.set(`circuit:pool:${bcAddress}`, record, 60).catch(() => {});

    return record;
  } catch { return null; }
}

// ── PumpSwap vault RPC fallback ───────────────────────────────────────────────

// Fetches live PumpSwap pool price from Triton RPC by reading vault token balances.
// Used when the gRPC-streamed Redis records have expired (pool-by-mint = 120s TTL
// from indexer, extended to 86400s on first RPC fetch here).
//
// Pool account layout (parsed by circuit-indexer/parsers/pumpswap.js, 301 bytes):
//   [43-74]   baseMint   — non-SOL token
//   [75-106]  quoteMint  — WSOL
//   [139-170] baseVault  — token SPL account (holds baseMint / non-SOL token)
//   [171-202] quoteVault — WSOL SPL account  (holds quoteMint / WSOL)
//
// SPL token account balance is stored at byte offset 64 as a uint64 LE.
// All pump.fun tokens have exactly 6 decimals; WSOL has 9.
// priceSol = (quoteVaultBalance / 1e9) / (baseVaultBalance / 1e6)
//          = (WSOL_lamports / 1e9) / (token_raw / 1e6)
//
// On success: writes pool record (300s TTL), pool-by-mint (86400s TTL), and
// price-sol (120s TTL) to Redis so subsequent monitor ticks use the cache.
// On failure: returns null, caller falls to Jupiter.
async function _rpcFetchPumpSwapPrice(poolAccount, pool, mint) {
  const rpcUrl = process.env.CIRCUIT_RPC_URL;
  if (!rpcUrl) return null;

  let baseVault  = pool?.baseVault;   // SOL vault pubkey string
  let quoteVault = pool?.quoteVault;  // token vault pubkey string

  // If vault addresses are missing (pool record null/incomplete), fetch the pool account.
  if (!baseVault || !quoteVault) {
    try {
      const resp = await fetch(rpcUrl, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({
          jsonrpc: '2.0',
          id:      1,
          method:  'getAccountInfo',
          params:  [poolAccount, { encoding: 'base64', commitment: 'confirmed' }],
        }),
        signal: AbortSignal.timeout(3_000),
      });
      if (!resp.ok) return null;
      const json = await resp.json();
      const val  = json?.result?.value;
      if (!val) return null;
      // Verify this is actually a PumpSwap pool before parsing
      if (val.owner !== PUMPSWAP_PROGRAM) return null;
      const buf = Buffer.from(val.data[0], 'base64');
      if (buf.length < 203) return null;
      // Parse vault pubkeys directly from the pool account buffer.
      // baseVault at offset 139-170, quoteVault at offset 171-202.
      baseVault  = new PublicKey(buf.slice(139, 171)).toBase58();
      quoteVault = new PublicKey(buf.slice(171, 203)).toBase58();
    } catch { return null; }
  }

  if (!baseVault || !quoteVault) return null;

  // Fetch both vault token account balances in one RPC call.
  try {
    const resp = await fetch(rpcUrl, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({
        jsonrpc: '2.0',
        id:      2,
        method:  'getMultipleAccounts',
        params:  [[baseVault, quoteVault], { encoding: 'base64', commitment: 'confirmed' }],
      }),
      signal: AbortSignal.timeout(3_000),
    });
    if (!resp.ok) return null;
    const json     = await resp.json();
    const accounts = json?.result?.value;
    if (!Array.isArray(accounts) || accounts.length < 2) return null;

    // SPL token account: amount (u64 LE) is at byte offset 64.
    const readBalance = (acct) => {
      if (!acct?.data?.[0]) return null;
      const buf = Buffer.from(acct.data[0], 'base64');
      if (buf.length < 72) return null;
      return buf.readBigUInt64LE(64); // BigInt — full precision (Number truncates vaults > 2^53 raw)
    };

    // PumpSwap layout: baseVault (accounts[0]) = TOKEN vault, quoteVault (accounts[1]) = WSOL vault.
    // baseVault holds the non-SOL token (baseMint); quoteVault holds WSOL (quoteMint = WSOL).
    const tokenBalance = readBalance(accounts[0]); // baseVault  → token raw (6 dec), BigInt
    const solBalance   = readBalance(accounts[1]); // quoteVault → WSOL lamports (9 dec), BigInt

    if (tokenBalance == null || solBalance == null) return null;
    if (tokenBalance === 0n  || solBalance === 0n)  return null;

    // priceSol = (WSOL_lamports/1e9) / (token_raw/1e6) = solRaw / (tokenRaw * 1000). Divide in BigInt at
    // 1e12 precision before the (small, safe) Number downcast, so big vaults don't lose the ratio's bits.
    const priceSol = Number((solBalance * 1_000_000_000_000n) / (tokenBalance * 1000n)) / 1e12;
    if (!isFinite(priceSol) || priceSol <= 0) return null;

    // Refresh Redis so subsequent monitor ticks hit path 1 or 2 instead of RPC.
    // _vault0Balance = token (baseVault), _vault1Balance = WSOL (quoteVault).
    // dec0 = 6 (token), dec1 = 9 (WSOL). Used by _poolToPriceSol.
    // coinReserve = token raw, pcReserve = WSOL lamports — for estimateAmmSolOut.
    const updatedPool = {
      ...(pool ?? {}),
      poolAccount,
      type:           'pumpswap',
      poolType:       'pumpswap',
      baseVault,
      quoteVault,
      _vault0Balance: Number(tokenBalance), // Number for the JSON/Redis cache + _poolToPriceSol
      _vault1Balance: Number(solBalance),
      dec0:           6, // token (baseVault)
      dec1:           9, // WSOL  (quoteVault)
      price:          priceSol,
      updatedAt:      Date.now(),
      ts:             Date.now(),
    };
    redis.set(`circuit:pool:${poolAccount}`,       updatedPool, 300).catch(() => {});  // 5 min pool record
    redis.set(`circuit:pool-by-mint:${mint}`,      poolAccount, 86400).catch(() => {}); // 24h pointer
    redis.set(`circuit:price-sol:${mint}`, {
      priceSol,
      source:      'pumpswap:rpc',
      ts:          Date.now(),
      poolAccount,
      coinReserve:  tokenBalance,
      pcReserve:    solBalance,
      coinDecimals: 6,
      pcDecimals:   9,
    }, 120).catch(() => {});

    return priceSol;
  } catch { return null; }
}

// ── Raydium CPMM vault RPC fallback ───────────────────────────────────────────
// Parity with the PumpSwap fallback above. CPMM prices are derived from vault-balance deltas (the
// Token-Program subscription); when those updates are missed/stale, _poolToPriceSol just returns the
// FROZEN pool.price. This refetches both vault balances live from RPC and recomputes — so a missed
// CPMM vault self-heals instead of dropping straight to rate-limited Jupiter. The CPMM pool record
// already carries vault0/vault1 + dec0/dec1 (parsed from the pool account). SOL-quoted pools only;
// a USD-quoted CPMM still falls through to Jupiter (rare, and Jupiter covers it).
async function _rpcFetchCpmmPrice(poolAccount, pool, mint) {
  const rpcUrl = process.env.CIRCUIT_RPC_URL;
  if (!rpcUrl) return null;
  const vault0 = pool?.vault0, vault1 = pool?.vault1;
  const dec0 = pool?.dec0, dec1 = pool?.dec1;
  if (!vault0 || !vault1 || dec0 == null || dec1 == null) return null;

  try {
    const resp = await fetch(rpcUrl, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({
        jsonrpc: '2.0',
        id:      2,
        method:  'getMultipleAccounts',
        params:  [[vault0, vault1], { encoding: 'base64', commitment: 'confirmed' }],
      }),
      signal: AbortSignal.timeout(3_000),
    });
    if (!resp.ok) return null;
    const accounts = (await resp.json())?.result?.value;
    if (!Array.isArray(accounts) || accounts.length < 2) return null;

    const readBalance = (acct) => {
      if (!acct?.data?.[0]) return null;
      const buf = Buffer.from(acct.data[0], 'base64');
      if (buf.length < 72) return null;
      return buf.readBigUInt64LE(64); // BigInt — full precision (Number truncates raw > 2^53)
    };
    // vault0 ↔ mint0, vault1 ↔ mint1 (CPMM layout). bal0 = mint0 raw, bal1 = mint1 raw.
    const bal0 = readBalance(accounts[0]);
    const bal1 = readBalance(accounts[1]);
    if (bal0 == null || bal1 == null || bal0 === 0n || bal1 === 0n) return null;

    // price = (bal1/10^dec1) / (bal0/10^dec0) = mint1 per mint0 — same convention as _poolToPriceSol.
    // BigInt-scaled to 1e12 before the (small, safe) Number downcast so big vaults keep the ratio.
    const price = Number(bal1 * (10n ** BigInt(dec0)) * 1_000_000_000_000n / (bal0 * (10n ** BigInt(dec1)))) / 1e12;
    if (!isFinite(price) || price <= 0) return null;

    // Reuse the existing CPMM orientation (mint1=SOL → price; mint0=SOL → 1/price; USD → null).
    const updatedPool = {
      ...(pool ?? {}), poolAccount, type: 'raydium-cpmm', poolType: 'raydium-cpmm',
      price, _vault0Balance: Number(bal0), _vault1Balance: Number(bal1),
      updatedAt: Date.now(), ts: Date.now(),
    };
    const derived  = _poolToPriceSol(updatedPool, mint);
    const priceSol = derived?.priceSol;
    if (!priceSol || !isFinite(priceSol) || priceSol <= 0) return null;

    // Refresh Redis so subsequent monitor ticks hit the cache (path 1/2) instead of RPC.
    redis.set(`circuit:pool:${poolAccount}`,  updatedPool, 300).catch(() => {});
    redis.set(`circuit:pool-by-mint:${mint}`, poolAccount, 86400).catch(() => {});
    redis.set(`circuit:price-sol:${mint}`, { priceSol, source: 'raydium-cpmm:rpc', ts: Date.now(), poolAccount }, 120).catch(() => {});
    return priceSol;
  } catch { return null; }
}

// ── Jupiter Price API v3 fallback ─────────────────────────────────────────────

function _httpsGet(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: timeoutMs }, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch { reject(new Error('bad JSON')); }
      });
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.on('error', reject);
  });
}

async function _jupiterPrice(mint) {
  try {
    // Fetch the token AND wSOL in one call, then derive priceSol = tokenUsd / solUsd.
    // The USD cancels out, so this works even if the sol-price oracle is momentarily
    // unpopulated — no dependency on getSolPrice() for the SOL conversion itself.
    const data = await _httpsGet(`${JUPITER_PRICE_BASE}?ids=${mint},${SOL_MINT}`, JUPITER_TIMEOUT);
    // v3 response: { MINT: { usdPrice, liquidity, decimals, ... } } — no outer "data" wrapper
    const entry = data?.[mint];
    if (!entry) return null;
    const priceUsd = parseFloat(entry.usdPrice ?? entry.price);
    if (!priceUsd || !isFinite(priceUsd)) return null;
    // Prefer Jupiter's own wSOL quote from this same response; fall back to the cached oracle.
    const solUsd = parseFloat(data?.[SOL_MINT]?.usdPrice ?? data?.[SOL_MINT]?.price) || solPrice.getSolPrice();
    if (!solUsd) return null;
    const priceSol = priceUsd / solUsd;
    if (!isFinite(priceSol) || priceSol <= 0) return null;
    return { priceSol, priceUsd, source: 'jupiter-v3', ts: Date.now(), ageMs: 0 };
  } catch { return null; }
}

// ── Pool-based price derivation ───────────────────────────────────────────────

// True if `pool` actually references `mint` on either side. Used to reject a poisoned or
// stale pool-by-mint pointer before deriving a price from it — a wrong pointer would otherwise
// mis-attribute some other token's price to `mint` (e.g. a pump pool hijacking USDC/WSOL).
function _poolHasMint(pool, mint) {
  if (!pool || !mint) return false;
  return pool.baseMint === mint || pool.quoteMint === mint ||
         pool.mint0    === mint || pool.mint1     === mint ||
         pool.coinMint === mint || pool.pcMint    === mint ||
         pool.mintA    === mint || pool.mintB     === mint;
}

// Given a pool record from circuit:pool:{account}, compute priceSol for the non-SOL token.
// Supports: raydium-amm-v4, raydium-cpmm, raydium-clmm, orca-whirlpool,
//           pump-bonding-curve, pumpswap.
function _poolToPriceSol(pool, requestedMint) {
  if (!pool) return null;
  const type = pool.type ?? '';

  if (type === 'raydium-amm-v4') {
    // price field = SOL per coinToken (when pcMint=SOL)
    if (pool.price > 0 && isFinite(pool.price)) {
      const tokenMint = pool.coinMint ?? pool.mint0;
      if (tokenMint === requestedMint) return { priceSol: pool.price, reserves: _ammReserves(pool) };
      // Inverted orientation
      if ((pool.pcMint ?? pool.mint1) === requestedMint && pool.price > 0) {
        return { priceSol: 1 / pool.price, reserves: null };
      }
    }

  } else if (type === 'raydium-cpmm' || type === 'raydium-clmm') {
    if (pool.price > 0 && isFinite(pool.price)) {
      const mint0 = pool.mint0;
      const mint1 = pool.mint1;
      if (mint1 === SOL_MINT && mint0 === requestedMint) return { priceSol: pool.price, reserves: null };
      if (mint0 === SOL_MINT && mint1 === requestedMint) return { priceSol: 1 / pool.price, reserves: null };
    }

  } else if (type === 'orca-whirlpool') {
    // price = mintB per mintA
    if (pool.price > 0 && isFinite(pool.price)) {
      if (pool.mintB === SOL_MINT && pool.mintA === requestedMint) return { priceSol: pool.price, reserves: null };
      if (pool.mintA === SOL_MINT && pool.mintB === requestedMint) return { priceSol: 1 / pool.price, reserves: null };
    }

  } else if (type === 'pump-bonding-curve') {
    // Graduated tokens: bonding curve reserves are frozen at migration, not traded against.
    // Returning a price from them produces stale/wrong values — fall through to PumpSwap paths.
    if (pool.complete) return null;
    // priceRaw = lamports per raw token. SOL price (6-dec token) = priceRaw * 1e6 / 1e9
    const vSol   = Number(pool.virtualSolReserves ?? 0);
    const vToken = Number(pool.virtualTokenReserves ?? 1);
    if (vToken > 0 && vSol > 0) {
      // Assume 6 decimals (Pump.fun default); price-feed applies decimals if known
      const decimals = pool.tokenDecimals ?? 6;
      const priceSol = (vSol / 1e9) / (vToken / Math.pow(10, decimals));
      if (priceSol > 0 && isFinite(priceSol)) {
        return { priceSol, reserves: { vSol, vToken, complete: pool.complete ?? false } };
      }
    }

  } else if (type === 'pumpswap') {
    // Defense-in-depth: only price this pool for its own token (baseMint). A poisoned or stale
    // pool-by-mint pointer could route a different mint here, and unlike the raydium/orca branches
    // above (which validate === requestedMint) this branch would otherwise mis-attribute the
    // token's price to whatever mint was requested. Return null so resolution falls through.
    const baseMint = pool.baseMint ?? pool.mint0;
    if (baseMint && requestedMint && baseMint !== requestedMint) return null;
    // PumpSwap pool layout: baseMint = token (6 dec), quoteMint = WSOL (9 dec).
    // _vault0Balance = token raw (baseVault), _vault1Balance = WSOL lamports (quoteVault).
    // Hardcode decimal values — don't trust dec0/dec1 from potentially stale pool records.
    const tokenRaw = pool._vault0Balance; // token raw (baseVault, 6 dec)
    const wsolLam  = pool._vault1Balance; // WSOL lamports (quoteVault, 9 dec)
    if (tokenRaw != null && wsolLam != null && Number(tokenRaw) > 0 && Number(wsolLam) > 0) {
      const priceSol = (Number(wsolLam) / 1e9) / (Number(tokenRaw) / 1e6);
      if (priceSol > 0 && isFinite(priceSol)) {
        return {
          priceSol,
          reserves: {
            coinReserve:  tokenRaw,
            pcReserve:    wsolLam,
            coinDecimals: 6,
            pcDecimals:   9,
          },
        };
      }
    }
  }
  return null;
}

function _ammReserves(pool) {
  if (!pool.coinReserve && !pool.pcReserve) return null;
  return {
    coinReserve:  pool.coinReserve,
    pcReserve:    pool.pcReserve,
    coinDecimals: pool.coinDecimals,
    pcDecimals:   pool.pcDecimals,
  };
}

// Normalize the SOL side of any pool's reserves to a human SOL float.
// Used by circuit-agent monitor for LP-drain detection — only the SOL quantity matters.
// AMM/CPMM: pcReserve is raw SOL lamports (pcDecimals=9).
// Pump.fun: vSol is raw lamports.
function _toSolReserve(reserves) {
  if (!reserves) return null;
  if (reserves.pcReserve != null) {
    const sol = Number(reserves.pcReserve) / Math.pow(10, reserves.pcDecimals ?? 9);
    return (isFinite(sol) && sol > 0) ? sol : null;
  }
  if (reserves.vSol != null) {
    const sol = reserves.vSol / 1e9;
    return (isFinite(sol) && sol > 0) ? sol : null;
  }
  return null;
}

// ── USD enrichment ────────────────────────────────────────────────────────────

function _withUsd(result) {
  if (!result) return result;
  const sol = solPrice.getSolPrice();
  const usd = (sol && result.priceSol) ? result.priceSol * sol : null;
  // Jupiter already provides priceUsd directly — don't overwrite with a computed value
  // unless it's absent (avoids double-conversion error).
  const priceUsd = result.priceUsd ?? usd;
  return { ...result, priceUsd, solUsd: sol };
}

// ── Velocity gate (in-process, per-mint) ─────────────────────────────────────
// Cross-verifies with Jupiter when any non-indexer source returns a price that
// deviates >15% from the last resolved price for that mint. Catches phantom prices
// from any source (stale records, RPC glitches, future bugs) before they reach agents.
//
// Design notes:
//   - 'indexer' source (Geyser-written, slot-level accuracy) is fully trusted — no gate.
//   - Restart-aware: first tick for a mint seeds _lastPriceSol without gating.
//   - Fails open: if Jupiter is unavailable, the original price is returned unchanged.
//   - _lastPriceSol resets on service restart (process memory) — acceptable since indexer
//     writes price-sol at 120s TTL and will re-seed the gate within the first ticks.
const _lastPriceSol = new Map();    // mint → last validated priceSol
const VELOCITY_GATE_PCT = 0.15;     // 15% single-tick deviation triggers Jupiter cross-check

async function _velocityGate(mint, result) {
  if (!result?.priceSol) {
    if (!result) _lastPriceSol.delete(mint);
    return result;
  }

  const last = _lastPriceSol.get(mint);
  if (!last) {
    // First tick for this mint — set baseline, skip gate (no prior reference point).
    _lastPriceSol.set(mint, result.priceSol);
    return result;
  }

  const deviation = Math.abs(result.priceSol - last) / last;
  // A normal move (within gate) or the slot-accurate indexer is trusted and updates the baseline.
  let baselineOk = deviation <= VELOCITY_GATE_PCT || result.source === 'indexer';
  if (!baselineOk) {
    const jup = await _jupiterPrice(mint).catch(() => null);
    if (jup?.priceSol) {
      const jupDev = Math.abs(jup.priceSol - last) / last;
      if (jupDev <= VELOCITY_GATE_PCT) {
        // New price is an outlier vs last, but Jupiter agrees with last → phantom; return Jupiter's.
        const anomalyPriceSol = result.priceSol;
        const anomalySource   = result.source;
        result = { ...jup, source: `jupiter-corrected(${anomalySource})` };
        console.warn(
          `[velocity-gate] ${mint.slice(0, 8)} phantom: ${anomalySource} said ${anomalyPriceSol.toExponential(3)}` +
          ` (${(deviation * 100).toFixed(0)}% from last ${last.toExponential(3)})` +
          ` but Jupiter=${jup.priceSol.toExponential(3)} — returning Jupiter`
        );
      }
      baselineOk = true; // Jupiter-corrected OR Jupiter also moved (legitimate) — either way validated
    }
    // else: Jupiter unavailable → fail open (return the original) but DON'T poison the baseline with an
    // unvalidated outlier, or the next correct price would look like the anomaly.
  }

  if (baselineOk) _lastPriceSol.set(mint, result.priceSol);
  return result;
}

// ── Main resolution ───────────────────────────────────────────────────────────

async function _resolvePrice(mint) {
  const now = Date.now();

  // 1. Direct SOL price key (freshest — written by circuit-indexer on every pool update)
  const cached = await redis.get(`circuit:price-sol:${mint}`);
  if (cached?.priceSol > 0) {
    const reserves = cached.coinReserve ? {
      coinReserve:  cached.coinReserve,
      pcReserve:    cached.pcReserve,
      coinDecimals: cached.coinDecimals,
      pcDecimals:   cached.pcDecimals,
    } : null;
    return _withUsd({
      priceSol:    cached.priceSol,
      source:      cached.source ?? 'indexer',
      ageMs:       now - (cached.ts ?? now),
      poolAccount: cached.poolAccount ?? null,
      reserves,
      solReserve:  _toSolReserve(reserves),
    });
  }

  // 2. Pool-by-mint reverse index → derive price from pool state.
  // pool-by-mint has a 24h TTL (set by circuit-indexer or refreshed here on RPC fetch),
  // so the pool address is known long after the 120s price-sol key expires.
  const poolAccount = await redis.getString(`circuit:pool-by-mint:${mint}`);
  let pool = null;
  if (poolAccount) {
    pool = await redis.get(`circuit:pool:${poolAccount}`);
    // Reject a poisoned/stale reverse index: the referenced pool must actually contain `mint`.
    // Protects both the pool-derived path below and the PumpSwap RPC fallback.
    if (pool && !_poolHasMint(pool, mint)) pool = null;
    if (pool) {
      const derived = _poolToPriceSol(pool, mint);
      if (derived?.priceSol > 0) {
        return _withUsd({
          priceSol:    derived.priceSol,
          source:      `pool-derived:${pool.type ?? 'unknown'}`,
          ageMs:       now - (pool.updatedAt ?? now),
          poolAccount,
          reserves:    derived.reserves ?? null,
          solReserve:  _toSolReserve(derived.reserves),
        });
      }

      // PumpSwap pool record found but vault balance(s) null — fetch live via RPC.
      // Covers the 0-60s window where the pool record exists but _vault1Balance was never
      // populated (indexer needs two separate vault updates to fill both balances).
      if (pool.type === 'pumpswap' || pool.poolType === 'pumpswap') {
        const priceSol = await _rpcFetchPumpSwapPrice(poolAccount, pool, mint);
        if (priceSol > 0) {
          return _withUsd({ priceSol, source: 'pumpswap:rpc', ageMs: 0,
            poolAccount, reserves: null, solReserve: null });
        }
      }

      // CPMM pool record found but vault balance(s) stale/null — fetch live via RPC (parity with the
      // PumpSwap path above). Without this a missed CPMM vault has no refresh and drops to Jupiter.
      if (pool.type === 'raydium-cpmm' || pool.poolType === 'raydium-cpmm') {
        const priceSol = await _rpcFetchCpmmPrice(poolAccount, pool, mint);
        if (priceSol > 0) {
          return _withUsd({ priceSol, source: 'raydium-cpmm:rpc', ageMs: 0,
            poolAccount, reserves: null, solReserve: null });
        }
      }
    }
  }

  // 2.5. Pump.fun bonding curve PDA lookup.
  // Primary: the gRPC stream writes circuit:pool:{bcAddress} on every bonding curve transaction.
  // Fallback: if that record has expired (60s TTL, quiet tokens) fetch live from Triton RPC.
  // Either way we never reach DexScreener for tokens still on the bonding curve.
  const bcAddress = _pumpBondingCurveAddress(mint);
  if (bcAddress) {
    let bcPool  = await redis.get(`circuit:pool:${bcAddress}`);
    let bcSource = 'pump-bonding-curve';

    if (!bcPool) {
      // Cache miss — gRPC record expired. Fetch live from Triton RPC.
      bcPool   = await _rpcFetchBondingCurve(bcAddress);
      bcSource = 'pump-bonding-curve:rpc';
    }

    if (bcPool) {
      const derived = _poolToPriceSol(bcPool, mint);
      if (derived?.priceSol > 0) {
        // Non-graduated token: seed the reverse index so future quiet periods can find
        // this bonding curve without re-deriving the PDA every tick.
        redis.set(`circuit:pool-by-mint:${mint}`, bcAddress, 86400).catch(() => {});
        return _withUsd({
          priceSol:    derived.priceSol,
          source:      bcSource,
          ageMs:       now - (bcPool.updatedAt ?? now),
          poolAccount: bcAddress,
          reserves:    derived.reserves ?? null,
          solReserve:  _toSolReserve(derived.reserves),
        });
      }

      // derived is null → token is graduated (complete=true).
      // If pool-by-mint was previously poisoned with the bonding curve PDA (from a bad seed
      // before this fix), clear it now so the next call can discover the PumpSwap pool fresh.
      if (bcPool.complete && poolAccount === bcAddress) {
        redis.del(`circuit:pool-by-mint:${mint}`).catch(() => {});
      }
    }

    // 2.6. PumpSwap vault RPC fallback (graduated tokens).
    // The bonding curve PDA exists for all pump.fun tokens. If we reach here:
    // - bcPool was null or complete=true → token has graduated to PumpSwap.
    // - We need the PumpSwap pool address from step 2's pool-by-mint lookup.
    // - Skip if poolAccount is the bonding curve PDA itself (it fails the PUMPSWAP_PROGRAM
    //   owner check in _rpcFetchPumpSwapPrice — we already cleared it above, so the next
    //   call will use Jupiter while the indexer rediscovers the PumpSwap pool via Geyser).
    if (poolAccount && poolAccount !== bcAddress) {
      // pool may be null (pool record expired) — _rpcFetchPumpSwapPrice handles that
      // by fetching the pool account itself to get vault addresses, then vault balances.
      const priceSol = await _rpcFetchPumpSwapPrice(poolAccount, pool, mint);
      if (priceSol > 0) {
        return _withUsd({
          priceSol,
          source:      'pumpswap:rpc',
          ageMs:       0,
          poolAccount,
          reserves:    null,
          solReserve:  null,
        });
      }
    }
  }

  // 3. Jupiter Price API v3 — universal last resort for unindexed mints.
  // Returns USD price; converted to SOL using the local SOL/USD poller.
  // The circuit-indexer covers all major Solana DEX programs via Geyser, so reaching
  // here means this is either a newly launched token or an exotic/low-volume pool.
  const jup = await _jupiterPrice(mint);
  if (jup) {
    // Seed Redis so immediate retries hit the cache (path 1) instead of Jupiter again.
    redis.set(`circuit:price-sol:${mint}`, {
      priceSol:    jup.priceSol,
      priceUsd:    jup.priceUsd,
      source:      'jupiter-v3-cached',
      ts:          Date.now(),
      poolAccount: null,
    }, 30).catch(() => {});
    return _withUsd({ ...jup, solReserve: null });
  }

  return null;
}

// Public single-mint entry point: resolution chain + velocity gate.
async function resolvePrice(mint) {
  const result = await _resolvePrice(mint);
  return _velocityGate(mint, result);
}

// Batch resolution — returns object keyed by mint
async function resolvePrices(mints) {
  const results = {};
  await Promise.all(mints.map(async (mint) => {
    results[mint] = await resolvePrice(mint);
  }));
  return results;
}

module.exports = { resolvePrice, resolvePrices };
