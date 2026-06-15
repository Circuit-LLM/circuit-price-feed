// lib/slippage.js — Estimate actual SOL received selling into a constant-product pool.
//
// Used by circuit-agent's checkPosition() to abort phantom take-profit exits:
// if estimated fill < break-even, the stale price reading is not fillable.
//
// Supports raydium-amm-v4 and raydium-cpmm reserve-based estimates.
// CLMM and Orca (tick-based) fall back to a simple price-impact approximation.
'use strict';

const AMM_FEE = 0.0025; // Raydium AMM v4 default fee (0.25%)

/**
 * Estimate SOL received for selling tokenAmountRaw raw units into an AMM v4 / CPMM pool.
 *
 * @param reserves  {coinReserve, pcReserve, coinDecimals, pcDecimals}
 *                  coinReserve/pcReserve are raw atomic units (string or number)
 * @param tokenAmountRaw  raw atomic units to sell (number or string)
 * @param fee             pool fee fraction (default 0.0025)
 * @returns { estimatedSolOut, priceImpactPct, effectivePriceSol }
 */
function estimateAmmSolOut(reserves, tokenAmountRaw, fee = AMM_FEE) {
  const coinR  = Number(reserves.coinReserve);
  const pcR    = Number(reserves.pcReserve);
  const tokenIn = Number(tokenAmountRaw);

  if (!coinR || !pcR || !tokenIn) return null;

  // Constant product: k = coinR * pcR
  // tokenIn after fee: amountInWithFee = tokenIn * (1 - fee)
  const amountInWithFee = tokenIn * (1 - fee);
  const solOutRaw = (pcR * amountInWithFee) / (coinR + amountInWithFee);

  const solOut = solOutRaw / Math.pow(10, reserves.pcDecimals ?? 9);
  if (!isFinite(solOut) || solOut <= 0) return null;

  // Mid-price (no impact): pcR/coinR in SOL per raw token, then adjust decimals
  const midPriceSol = (Number(pcR) / Math.pow(10, reserves.pcDecimals ?? 9))
                    / (Number(coinR) / Math.pow(10, reserves.coinDecimals ?? 6));
  const expectedSolOut = midPriceSol * (tokenIn / Math.pow(10, reserves.coinDecimals ?? 6));
  const priceImpactPct = expectedSolOut > 0
    ? ((expectedSolOut - solOut) / expectedSolOut) * 100
    : null;

  const tokenInHuman  = tokenIn / Math.pow(10, reserves.coinDecimals ?? 6);
  const effectivePriceSol = tokenInHuman > 0 ? solOut / tokenInHuman : null;

  return { estimatedSolOut: solOut, priceImpactPct, effectivePriceSol };
}

/**
 * Estimate tokens received for buying with solAmountHuman SOL into an AMM v4 / CPMM pool.
 * Symmetric inverse of estimateAmmSolOut — SOL is the input, token is the output.
 *
 * @param reserves        {coinReserve, pcReserve, coinDecimals, pcDecimals}
 * @param solAmountHuman  human SOL (e.g. 0.05)
 * @param fee             pool fee fraction (default 0.0025)
 * @returns { estimatedTokensOut, priceImpactPct, effectivePriceSol }
 */
function estimateAmmTokenOut(reserves, solAmountHuman, fee = AMM_FEE) {
  const coinR   = Number(reserves.coinReserve);
  const pcR     = Number(reserves.pcReserve);
  const solIn   = solAmountHuman * Math.pow(10, reserves.pcDecimals ?? 9); // SOL in raw lamports

  if (!coinR || !pcR || !solIn) return null;

  // Constant product: tokensOut = coinR * solInWithFee / (pcR + solInWithFee)
  const solInWithFee  = solIn * (1 - fee);
  const tokensOutRaw  = (coinR * solInWithFee) / (pcR + solInWithFee);
  const tokensOut     = tokensOutRaw / Math.pow(10, reserves.coinDecimals ?? 6);

  if (!isFinite(tokensOut) || tokensOut <= 0) return null;

  // Mid-price: coinR/pcR = tokens per SOL lamport, adjust decimals
  const midPriceTokensPerSol = (Number(coinR) / Math.pow(10, reserves.coinDecimals ?? 6))
                             / (Number(pcR)   / Math.pow(10, reserves.pcDecimals   ?? 9));
  const expectedTokensOut = midPriceTokensPerSol * solAmountHuman;
  const priceImpactPct    = expectedTokensOut > 0
    ? ((expectedTokensOut - tokensOut) / expectedTokensOut) * 100
    : null;

  // effectivePriceSol = SOL per 1 UI token (what you paid)
  const effectivePriceSol = tokensOut > 0 ? solAmountHuman / tokensOut : null;

  return { estimatedTokensOut: tokensOut, priceImpactPct, effectivePriceSol };
}

/**
 * Simple price-impact approximation for CLMM/Orca where we don't have raw reserves.
 * Uses the formula: impact ≈ tokenValueSol / (2 * liquidityDepthSol).
 * Returns null if liquidity is unavailable.
 */
function estimateImpactApprox(tokenValueSol, poolLiquidityUsd, solPriceUsd) {
  if (!poolLiquidityUsd || !solPriceUsd) return null;
  const depthSol = poolLiquidityUsd / solPriceUsd;
  if (depthSol <= 0) return null;
  const impact = (tokenValueSol / (2 * depthSol)) * 100;
  return { priceImpactPct: Math.min(impact, 100) };
}

module.exports = { estimateAmmSolOut, estimateAmmTokenOut, estimateImpactApprox };
