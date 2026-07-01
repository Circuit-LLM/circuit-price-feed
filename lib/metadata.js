// lib/metadata.js — resolve token name/symbol, Redis-cached. Handles BOTH metadata schemes:
//   • classic SPL (Tokenkeg)  → Metaplex Token Metadata PDA (a separate account)
//   • Token-2022 (TokenzQd)   → the TokenMetadata extension stored ON the mint account
//
// The reserve price feed knows a token's decimals/supply (from the mint's base bytes) but NOT its
// name/symbol, so /scan candidates come back as symbol '?'. This resolves them on demand so the
// scan — and therefore agent logs, the dashboard scanner, and published swarm signals — show real
// tickers. A Redis cache (positive 24h / negative 1h) means only NEW mints hit RPC, batched via
// getMultipleAccounts (≤100/call): typically one RPC per scan, near-zero once warm.
'use strict';

const { PublicKey } = require('@solana/web3.js');
const redis = require('./redis');

const METADATA_PROGRAM = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'); // Metaplex
const TOKEN_2022       = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const TOKEN_CLASSIC    = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const META_TTL_SEC = parseInt(process.env.CIRCUIT_META_TTL_SEC ?? '86400', 10);   // 24h — resolved
const NEG_TTL_SEC  = parseInt(process.env.CIRCUIT_META_NEG_TTL_SEC ?? '3600', 10); // 1h  — no metadata
const RPC_URL = () => process.env.CIRCUIT_RPC_URL;

function _metadataPda(mint) {
  try {
    const [pda] = PublicKey.findProgramAddressSync(
      [Buffer.from('metadata'), METADATA_PROGRAM.toBuffer(), new PublicKey(mint).toBuffer()],
      METADATA_PROGRAM,
    );
    return pda.toBase58();
  } catch { return null; }
}

// Read a borsh String (u32 LE length prefix + utf8), null-trimmed. Returns [value, nextOffset].
function _readString(buf, o, max) {
  const len = buf.readUInt32LE(o); o += 4;
  if (len > max || o + len > buf.length) return [null, -1];
  return [buf.slice(o, o + len).toString('utf8').replace(/\0/g, '').trim() || null, o + len];
}

// Metaplex Metadata PDA: key(1) · update_authority(32) · mint(32) · name · symbol · uri · …
function _parseMetaplex(buf) {
  try {
    if (!Buffer.isBuffer(buf) || buf.length < 1 + 32 + 32 + 4) return null;
    let o = 1 + 32 + 32;
    const [name, o2] = _readString(buf, o, 128); if (o2 < 0) return null;
    const [symbol]   = _readString(buf, o2, 64);
    return (symbol || name) ? { symbol, name } : null;
  } catch { return null; }
}

// Token-2022 mint: base(82) · padding · account_type@165 · TLV extensions@166.
// TokenMetadata extension (type 19) value: update_authority(32) · mint(32) · name · symbol · uri · …
function _parseToken2022(buf) {
  try {
    if (!Buffer.isBuffer(buf) || buf.length < 166 + 4) return null;
    let o = 166;
    while (o + 4 <= buf.length) {
      const type = buf.readUInt16LE(o);
      const len  = buf.readUInt16LE(o + 2);
      o += 4;
      if (o + len > buf.length) break;
      if (type === 19) {                       // TokenMetadata
        let p = o + 32 + 32;                    // skip update_authority + mint
        const [name, p2] = _readString(buf, p, 128); if (p2 < 0) return null;
        const [symbol]   = _readString(buf, p2, 64);
        return (symbol || name) ? { symbol, name } : null;
      }
      o += len;
    }
    return null;
  } catch { return null; }
}

async function _getMultipleAccounts(pubkeys) {
  const url = RPC_URL();
  if (!url || !pubkeys.length) return [];
  const resp = await fetch(url, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getMultipleAccounts', params: [pubkeys, { encoding: 'base64' }] }),
    signal:  AbortSignal.timeout(6_000),
  });
  if (!resp.ok) throw new Error(`rpc getMultipleAccounts ${resp.status}`);
  const j = await resp.json();
  return j?.result?.value ?? [];
}

function _cache(out, mint, meta) {
  const entry = meta && (meta.symbol || meta.name)
    ? { symbol: meta.symbol ?? null, name: meta.name ?? null }
    : { symbol: null, name: null };
  out[mint] = entry;
  redis.set(`circuit:meta:${mint}`, entry, entry.symbol ? META_TTL_SEC : NEG_TTL_SEC).catch(() => {});
}

/**
 * Resolve { symbol, name } for a list of mints. Returns a map keyed by mint. Never throws —
 * resolution is best-effort; a miss or RPC blip just leaves the caller's '?' fallback in place.
 */
async function resolveSymbols(mints) {
  const out = {};
  const uniq = [...new Set((mints ?? []).filter(Boolean))];
  if (!uniq.length) return out;

  // 1. Cache lookup — one pipelined read.
  const miss = [];
  try {
    const r = await redis.getClient();
    if (r) {
      const pipe = r.pipeline();
      for (const m of uniq) pipe.get(`circuit:meta:${m}`);
      const res = await pipe.exec();
      for (let i = 0; i < uniq.length; i++) {
        const raw = res[i]?.[1];
        if (raw) { try { out[uniq[i]] = JSON.parse(raw); continue; } catch { /* fall through */ } }
        miss.push(uniq[i]);
      }
    } else {
      miss.push(...uniq);
    }
  } catch { return out; }
  if (!miss.length || !RPC_URL()) return out;

  // 2. Pass A — fetch the mint accounts; the owner tells us which metadata scheme applies.
  //    Token-2022 carries metadata on-mint (parse now); classic SPL needs its Metaplex PDA (pass B).
  const needMetaplex = [];
  for (let i = 0; i < miss.length; i += 100) {
    const chunk = miss.slice(i, i + 100);
    let accts;
    try { accts = await _getMultipleAccounts(chunk); } catch { continue; }
    for (let k = 0; k < chunk.length; k++) {
      const mint = chunk[k], val = accts[k];
      if (!val) { _cache(out, mint, null); continue; }
      if (val.owner === TOKEN_2022)        _cache(out, mint, _parseToken2022(Buffer.from(val.data[0], 'base64')));
      else if (val.owner === TOKEN_CLASSIC) needMetaplex.push(mint);
      else                                  _cache(out, mint, null);
    }
  }

  // 3. Pass B — Metaplex PDAs for the classic SPL mints.
  const pairs = needMetaplex.map(m => [m, _metadataPda(m)]).filter(([, p]) => p);
  for (let i = 0; i < pairs.length; i += 100) {
    const chunk = pairs.slice(i, i + 100);
    let accts;
    try { accts = await _getMultipleAccounts(chunk.map(([, p]) => p)); } catch { continue; }
    for (let k = 0; k < chunk.length; k++) {
      const [mint] = chunk[k], val = accts[k];
      _cache(out, mint, val?.data?.[0] ? _parseMetaplex(Buffer.from(val.data[0], 'base64')) : null);
    }
  }
  return out;
}

module.exports = { resolveSymbols };
