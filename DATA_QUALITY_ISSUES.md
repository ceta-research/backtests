# Data Quality Issues

Known data quality issues affecting backtests. All strategies in this repo are affected unless noted otherwise.

Last updated: 2026-03-09

---

## ASX (Australian Securities Exchange)

**Status:** Excluded from all backtests
**Issue:** FMP `adjClose` has incorrect stock split/consolidation adjustments
**Severity:** Fatal (produces 58%+ CAGR artifacts)

**Evidence:**
- 314 stocks with max/min adjClose ratios exceeding 1,000x
- IIQ.AX (INOVIQ Ltd): adjClose oscillates between ~25 and ~15,000
- NCR.AX: 11.7 million x price ratio (0.0001 to 1,635.10)
- Produces extreme annual returns: 2005 (+740%), 2008 (+698%)

**Root cause:** FMP's adjClose field doesn't properly apply stock split/consolidation adjustments retroactively for ASX stocks. The raw close and adjClose values are identical even across dates where splits clearly occurred. The stock-split-calendar endpoint also returns 0 events for affected symbols.

**Impact on backtests:**
- Interest Coverage: 58.04% CAGR (impossible, artifact)
- QARP: -0.08% CAGR, 94% cash (only 3 invested periods)
- Low P/E: Not tested

**Verification query:**
```sql
SELECT s.symbol, p.companyName,
    MIN(s.adjClose) as min_price,
    MAX(s.adjClose) as max_price,
    ROUND(MAX(s.adjClose) / NULLIF(MIN(s.adjClose), 0), 1) as price_ratio
FROM stock_eod s
JOIN profile p ON s.symbol = p.symbol
WHERE p.exchange = 'ASX'
AND s.adjClose > 0
GROUP BY s.symbol, p.companyName
HAVING price_ratio > 1000
ORDER BY price_ratio DESC
```

**Filed:** FMP bug report (2026-03-03) documenting 314 affected ASX symbols.

---

## SAO (Sao Paulo Stock Exchange, Brazil)

**Status:** Excluded from all backtests
**Issue:** Same FMP `adjClose` issue as ASX
**Severity:** Fatal (produces 3,250% single-year returns)

**Evidence:**
- 20+ stocks with >1,000x price ratios in 2007 alone
- CTNM3.SA: max adjClose 132,118,525 vs min 37.90 (3.5 million x ratio)
- CEDO3.SA: 880,395x ratio
- CGAS3.SA: 701,818x ratio
- LUXM3.SA: 147,122x ratio

**Root cause:** Same as ASX. Reverse splits and consolidations not applied retroactively to adjClose.

**Impact on backtests:**
- Interest Coverage: 39.44% CAGR (artifact), 3,250% single-year return in 2013
- QARP: Not tested
- Low P/E: Not tested

**Filed:** Included in FMP bug report (2026-03-03).

---

## JPX (Japan Exchange Group)

**Status:** Data quality issue resolved. Not yet added to backtests — adjClose quality unverified.
**Previously excluded for:** No FY (annual) financial data in warehouse
**Resolved:** 2026-03-09 — FY data confirmed present (4,016 symbols, 2,645 qualifying on EV/EBITDA screen)

**Evidence of fix:**
- FY tables (key_metrics where period='FY'): 4,016 distinct JPX symbols
- EV/EBITDA screen (evToEBITDA > 0 < 10, ROE > 10%): 2,645 qualifying symbols — sufficient universe

**Remaining before adding to backtests:**
- Verify adjClose data quality (check for split/consolidation artifacts, same test as ASX/SAO)
- Run price ratio check: `MAX(adjClose)/MIN(adjClose)` for JPX symbols, flag any > 1,000x
- If clean, add `("japan", ["JPX"])` to GLOBAL_PRESETS in backtest.py scripts

---

## LSE (London Stock Exchange)

**Status:** Data quality issue resolved. Not yet added to backtests — adjClose quality unverified.
**Previously excluded for:** No FY financial data in warehouse
**Resolved:** 2026-03-09 — FY data confirmed present (3,701 symbols, 2,378 qualifying on EV/EBITDA screen)

**Evidence of fix:**
- FY tables: 3,701 distinct LSE symbols
- EV/EBITDA screen: 2,378 qualifying symbols

**Remaining before adding to backtests:**
- Verify adjClose data quality (same test as JPX above)
- If clean, add `("uk", ["LSE"])` to GLOBAL_PRESETS in backtest.py scripts

---

## SGX (Singapore Exchange)

**Status:** Excluded from all backtests
**Issue:** Profile query returns 0 symbols
**Severity:** Fatal (no universe to screen)

**Evidence:**
- `SELECT COUNT(*) FROM profile WHERE exchange = 'SGX'` returns 0
- SGX symbols may be stored under a different exchange code, or not ingested

**Fix required:** Investigate correct exchange code for SGX in FMP data. May need pipeline update.

---

## Notes

### Exchanges confirmed clean
US_MAJOR (NYSE+NASDAQ+AMEX), BSE, NSE, STO, TSX, SHZ, HKSE, SET, XETRA, SHH, SIX, TAI, KSC, SES, OSL, MIL, KLS, JKT

### JPX and LSE — FY data fixed, adjClose unverified
FY data now exists for both exchanges (resolved 2026-03-09). adjClose quality check still needed before adding to backtests. See JPX and LSE sections above.

### JNB (Johannesburg Stock Exchange) — moderate quality concern
**Status:** Included with documented caveat
**Issue:** 71 of 269 JNB symbols (26%) have historical max/min adjClose ratios > 100x. Extreme cases: ADW.JO (283,472x), SEB.JO (150,837x), BEL.JO (147,778x).

**Evidence:**
```sql
SELECT symbol, ROUND(MAX(adjClose)/NULLIF(MIN(adjClose),0),0) as ratio
FROM stock_eod
WHERE symbol IN (SELECT symbol FROM profile WHERE exchange = 'JNB') AND adjClose > 0
GROUP BY symbol HAVING ratio > 100 ORDER BY ratio DESC LIMIT 20
-- Returns 20 rows, top ratio: 283,472x
```

**Assessment:** Not a fatal data quality issue for PEG ratio backtest because:
- MCap > $1B filter excludes the vast majority of JSE micro-caps driving these ratios
- `filter_returns()` (min_entry_price=$1, max_single_return=200%) catches any split artifacts that pass screening
- Backtest results are clean: MaxDD -38.97% (lower than SPY -45.53%), no single-quarter return > 100%
- Sharpe 0.457 — consistent with a functioning strategy

**Origin:** JSE has a large micro-cap tail; extreme ratios likely reflect genuine small-cap appreciation in ZAR over 25 years, not unadjusted splits. Unlike ASX/SAO (where mid/large-caps were affected), the problematic JNB symbols are filtered by market cap.

**Content action:** JNB included in PEG ratio backtest and dedicated regional blog with data quality disclosure.
**Checked:** 2026-03-05

### KSC (Korea) transient error
KSC had a transient parquet download error ("No magic bytes found at end of file") during initial testing. Re-run succeeded. Full data exists (1,022 symbols, 152,626 FY rows). No data quality issue.

### Shared data quality guards (data_utils.filter_returns)
Added 2026-03-03. `filter_returns()` in `data_utils.py` provides reusable price data quality filtering:
- `min_entry_price=1.0`: Skips stocks with entry price < $1 (catches bad adjClose, penny stock artifacts, symbol reassignments)
- `max_single_return=2.0`: Skips stocks with single-period return > 200% (catches price data artifacts)

Used by: asset-growth. Should be adopted by all future strategies. Existing strategies (qarp, low-pe, interest-coverage) can be retrofitted.

### How to check a new exchange
Before adding an exchange to a backtest, run these checks:

1. **FY data exists:** `SELECT COUNT(DISTINCT symbol) FROM key_metrics WHERE period='FY' AND symbol IN (SELECT symbol FROM profile WHERE exchange='XXX')`
2. **Price data clean:** Check for extreme ratios: `SELECT symbol, MIN(adjClose), MAX(adjClose), MAX(adjClose)/NULLIF(MIN(adjClose),0) as ratio FROM stock_eod WHERE symbol IN (...) GROUP BY symbol HAVING ratio > 100`
3. **No single-quarter return > 100%** after running the backtest
4. **CAGR is plausible** for the exchange and time period
