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

**Status:** Clean — included in backtests with filter_returns() protection
**Previously excluded for:** No FY (annual) financial data in warehouse
**Resolved:** 2026-03-09 — FY data confirmed present (4,016 symbols)
**adjClose verified:** 2026-03-10 — 10 stocks above ¥100B threshold have bad ratios, handled by market cap filter + filter_returns()

**Evidence:**
- FY tables (key_metrics where period='FY'): 4,016 distinct JPX symbols
- 10 stocks above market cap threshold with extreme adjClose ratios — all filtered by filter_returns(max_single_return=200%)
- Confirmed clean in pe-compression backtest (2026-03-10) and defensive-quality backtest (2026-03-10)

---

## LSE (London Stock Exchange)

**Status:** Clean — included in backtests with filter_returns() protection
**Previously excluded for:** No FY financial data in warehouse
**Resolved:** 2026-03-09 — FY data confirmed present (3,701 symbols)
**adjClose verified:** 2026-03-10 — 17 stocks above £500M threshold have bad ratios, handled by market cap filter + filter_returns()

**Evidence:**
- FY tables: 3,701 distinct LSE symbols
- 17 stocks above market cap threshold with extreme adjClose ratios — all filtered by filter_returns()
- Confirmed clean in pe-compression backtest (2026-03-10) and defensive-quality backtest (2026-03-10)

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

## XLY (Consumer Discretionary ETF) — Split Artifact Dec 2010

**Status:** Handled via divergence guard in sector-pe-compression backtest
**Affects:** `sector-pe-compression/backtest.py` (and any other sector ETF strategies)
**Issue:** FMP `stock_eod` XLY `adjClose` drops from ~$31.22 to ~$15.70 on Dec 20, 2010 — a 2:1 stock split with no backward price adjustment
**Severity:** Produces spurious -43% quarterly return (Q4 2010: XLY -43.2% vs SPY +11.4%, -54% divergence)

**Evidence:**
- Dec 20, 2010: adjClose goes from $31.22 (Dec 17) → $15.70 (Dec 20)
- Exact 2:1 ratio ($31.22 / $15.70 = 1.989... ≈ 2.0)
- Creates Q4 2010 XLY return of -43.2% — physically impossible for a diversified sector ETF

**Root cause:** FMP `adjClose` for XLY is split-forward adjusted from some dates but the Dec 2010 2:1 split was not applied retroactively to pre-split prices.

**Fix:** Divergence guard in `sector-pe-compression/backtest.py`:
```python
MAX_ETF_DIVERGENCE = 0.45  # skip ETF if |ETF_return - SPY_return| > 45% in a quarter
if abs(raw_return - spy_ret) > MAX_ETF_DIVERGENCE:
    continue  # skip this ETF for this quarter, fall back to SPY
```
Q4 2010: |XLY -43.2% - SPY +11.4%| = 54.6% > 45% threshold → correctly filtered.

**Impact without fix:** 2010 annual return -17.8%; with fix: +6.65% (SPY held for Q4 2010).

**Checked:** 2026-03-10

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

**defensive-quality strategy (sector-04):** JNB excluded entirely. Not a data quality issue — the defensive sector universe (Consumer Defensive, Utilities, Healthcare) historically never produced 10+ qualifying stocks per July rebalance period. Max: ~8 qualifying stocks (2018-2019 peak). Strategy correctly stayed 100% cash all 25 periods. JNB removed from `presets_to_run` in `defensive-quality/backtest.py`. Excluded from all content (blog, LinkedIn, Reddit).

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

---

## Exchange-listed universes are not domicile universes (measured 2026-08-13)

Screens select every company **listed** on an exchange. Outside the US, most
qualifying names are foreign companies' secondary listings, not local
businesses. Measured on the fcf-yield screen:

| Screen | Locally domiciled | US-domiciled |
|--------|-------------------|--------------|
| XETRA 2015 | 3 of 36 | 32 |
| XETRA 2010 | 7 | 19 |
| LSE 2020 | 20 of 93 | 46 |
| LSE 2010 | 23 | 31 |

Those secondary lines are also largely untradeable: **18.6% of XETRA and 20.8%
of LSE daily rows carry zero volume** (NYSE: 1.1%), with quoted prices frozen
for days and then jumping. That is the same defect behind the phantom
+205-464% single-period returns that produced XETRA's old "+219% in 2019".

### How much it changes results

`fcf-yield/backtest.py --domicile-filter` restricts the universe to companies
whose `profile.country` matches the exchange's home country. Excess CAGR vs the
local benchmark, as-listed vs domicile-restricted:

| Market | As listed | Domicile-only | Delta | Cash periods |
|--------|-----------|---------------|-------|--------------|
| XETRA  | +4.01% | **-3.60%** | -7.61pp | 5 -> 20 of 25 |
| SIX    | +2.56% | **-1.00%** | -3.56pp | 9 -> 24 of 25 |
| LSE    | +8.75% | +7.70% | -1.05pp | 3 -> 6 |
| HKSE   | +3.67% | +3.43% | -0.24pp | unchanged |
| JPX    | +4.52% | +4.52% | 0.00pp | unchanged |
| Canada | +2.08% | +2.07% | -0.01pp | unchanged |
| NSE    | +0.33% | +0.33% | 0.00pp | unchanged |

**The effect is concentrated in small and mid-sized European markets.** Germany
and Switzerland both flip from positive to negative excess: their domestic
universes are too thin to fill a 30-stock screen, so the screen fills with
foreign lines and the apparent alpha belongs to the listing venue rather than to
German or Swiss companies. Deep domestic markets (Japan, Canada, India) and
genuinely regional exchanges (Hong Kong) are unaffected.

**Practical rule:** treat any thin-universe European result as suspect until
re-run with `--domicile-filter`. A high cash-period count under the filter is
the tell.

### Sweden is NOT affected (measured 2026-08-13)

Sweden was the largest open exposure in the sweep (27 topics carry a Sweden
blog) and had never been tested. It is now measured on two independent
instruments and is **clean on both**:

| Instrument | As listed | Domicile-only | Delta | Invested periods |
|------------|-----------|---------------|-------|------------------|
| price-to-sales STO (90 invested periods) | 10.65 / 10.69% | 10.36% | **-0.31pp** | 90 -> 90, unchanged |
| fcf-yield STO (thinly invested) | 0.92% | 0.23% | -0.69pp | 5 -> 3 of 25 |

Two as-listed figures are shown because **these backtests are not deterministic**: see
the note below. The delta is roughly 8x the run-to-run noise, so it is real but small.

Neither flips sign. The price-to-sales run is the reliable one: 90 invested
periods before and after, so the screen still fills entirely from domestic
names. The fcf-yield run agrees in direction but is low-powered, invested in
only 5 of 25 periods even before the filter, and should not be leaned on.

**Sweden behaves like Japan, Canada and India, not like Germany and
Switzerland.** This matters for the mechanism: the domicile effect bites when
the domestic universe cannot fill the screen, not because a market is European
or small. Stockholm has enough domestic listings to fill a 30-stock screen;
Frankfurt and Zurich do not.

**The diagnostic is the invested-period count, not the country.** If it is
unchanged under `--domicile-filter`, the screen was already domestic and the
result stands. If cash periods jump, the apparent alpha belonged to foreign
secondary listings. Check that number before assuming any market is safe or
suspect.

### Backtests are not bit-reproducible (found 2026-08-13)

Running the same backtest twice with identical arguments does not give the same number.
Measured on `price-to-sales --preset sweden`, three runs minutes apart:

| Run | CAGR |
|-----|------|
| pre-edit code | 10.68% |
| post-edit code, domicile OFF | 10.65% |
| post-edit code, domicile OFF, repeat | 10.69% |

**Spread 0.04pp across identical configurations.** The stored
`exchange_comparison.json` for the same screen says 10.96%, a further -0.29pp away, so
published figures do not reproduce exactly either. Invested periods and the benchmark
series were identical in every run, so this is not a universe or benchmark difference.

Two consequences:

1. **A difference under ~0.05pp is noise, not a finding.** Do not attribute it to a code
   change without re-running both arms more than once. This is how the domicile port to
   `price-to-sales` was cleared: the 0.03pp gap against pre-edit code looked like a
   regression until a repeat run landed on the other side of it.
2. **Published numbers drift.** The -0.29pp gap between the stored result and a fresh
   rerun is larger than the noise floor and is a separate, unexplained issue. Do not
   silently "correct" a blog to a fresh rerun without establishing which is right.

Shared helpers now live in `data_utils.py`: `EXCHANGE_COUNTRY` (36 exchanges),
`domicile_countries()` and `domicile_sql_condition()`. `--domicile-filter` is
implemented on `fcf-yield` and `price-to-sales`; 84 of 93 topics share the
identical `exchange IN ({ex_filter})` universe filter, so porting it further is
a mechanical edit rather than per-backtest work. It stays opt-in and default
OFF; **no published result uses it.**

---

## Closed-end funds contaminate revenue-ranked screens (measured 2026-08-16)

Closed-end funds, ETFs, BDCs and SPACs book investment income as `revenue`, so any
screen that ranks or filters on revenue growth buys them. `profile.isFund` and
`profile.isEtf` are set correctly but no backtest filters on them by default.

Measured on `small-cap` (top 30 by FY revenue growth), share of US portfolio slots
held by funds and ETFs:

| Period | Fund share of holdings |
|--------|------------------------|
| 2000-2004 | 0% |
| 2013 | 33% |
| 2020 | **84%** |
| full 25 years | 32% of all slots |

`small-cap/backtest.py --exclude-funds` (opt-in, default off) measures the sensitivity:

| Market | As screened | Funds excluded | Delta |
|--------|-------------|----------------|-------|
| US     | 7.82% CAGR, Sharpe 0.303 | **4.22%, Sharpe 0.087** | -3.60pp |
| Canada | +4.29% excess | +2.95% | -1.33pp |
| UK     | +2.41% excess | +3.64% | +1.23pp |
| all others | | | under 0.5pp |

**The sign is not stable across strategies.** On `revenue-accel` removing funds
*helped* by +0.70pp; here it *hurts* by 3.60pp, because the funds damp the bad years
(2015: funds +16.1% against operating companies -18.1%) and so raise the geometric
mean while lowering the arithmetic one. Do not assume the direction, measure it.

`isFund` / `isEtf` have **zero NULLs** on US profile rows, so a `= false` filter is
safe there. Check before using it on another exchange: `NULL = false` is NULL in SQL
and would silently drop every row with an unset flag.

Screening logic was left unchanged on both topics and the sensitivity was disclosed
in the content instead. Live screens are a different matter: published share links
should carry `isFund = false AND isEtf = false AND isActivelyTrading = true` plus an
Asset Management / Shell Companies industry exclusion for BDCs and SPACs, which the
plain flags do not catch.

**A ROE gate makes it worse, not better** (measured 2026-08-11 on the US universe):
adding `returnOnEquity > 0.10` to a plain revenue-growth rank took funds from 8/30 to
16/30, because a fund whose holdings marked up posts a high ROE. Revenue rank paired
with a quality gate is the high-risk shape.

**Still unchecked.** Only `revenue-accel` and `small-cap` carry an `isFund` diagnostic.
These rank on a revenue-derived metric with no guard at all: `market-share`,
`ocf-growth`, `revenue-surprise`, `fcf-conversion`, `owner-earnings`, and
`price-to-sales` (ranks ascending, so funds with tiny "sales" look ultra-cheap, opposite
direction and same cause). Measure `--exclude-funds` on each when its topic next comes
up for a rerun.

---

## remove_price_oscillations is a no-op for window-fetch backtests (measured 2026-08-16)

`data_utils.remove_price_oscillations()` deletes rows where adjClose spikes 3-5x for
a day or two and reverts. It does real work on the raw table and still changes
nothing, for any backtest that fetches prices only in windows around rebalance dates.

Measured on `small-cap` XETRA by fetching once, running with the filter monkeypatched
to a no-op, then applying it and running again:

| Arm | CAGR | Avg holdings |
|-----|------|--------------|
| filter off | 6.68% | 18.8 |
| filter on (3,966 rows deleted across 459 symbols) | 6.68% | 18.8 |

LSE, the exchange the filter commit flagged as worst-affected, moved excess +0.21pp.

The mechanism: prices are fetched in 10-day windows around each rebalance date, so
`LAG`/`LEAD` inside the filter crosses a year-long gap at every window boundary and
flags the **first row of each window** for any name that moved 30%+ over the year.
With `offset_days=1` those rows are never read by `get_prices()`.

**Do not attribute a rerun delta to this filter without measuring it.** On `small-cap`
the entire delta from the March run was FMP coverage drift, not the filter. Diagnostic
pattern: monkeypatch the filter to a no-op, fetch, run, apply the filter, run again on
the same cached table.
