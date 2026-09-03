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

**sector-rotation strategy (reversion-04):** JNB excluded 2026-08-17, same shape as
above and as the existing SES exclusion. The signal needs 5 sectors each holding
5+ stocks with a valid 12-month return before it will run. The JNB large-cap
universe (marketCap > ZAR 10B) never reaches that bar before 2017:

```sql
-- sectors with >=5 large-cap symbols carrying quarter-start prices, per year
-- 2000-2002: 2 qualifying sectors.  2003-2016: 4.  2017-2023: 6.  2024-25: 4.
```

Result: 81 of 104 quarters forced to cash, leaving 23 investable quarters clustered
in 2018-2023. A "2000-2025" claim on that base is not defensible, so JNB is dropped
from `presets_to_run`, from `generate_charts.py`, and from all content.

Note on the superseded numbers: the pre-2026-08 JNB entry reported +11.50% for 2004
and +114.81% for 2005. The universe cannot produce those years, so the old South
Africa row (11.72% CAGR, +3.69% excess) should be treated as an artifact rather than
a result that drifted. `results/returns_JNB.json` is kept on disk as evidence.
**Checked:** 2026-08-17

**sector-momentum strategy (sector-01):** JNB excluded 2026-08-19, same root cause.
This is the top-2 mirror of the strategy above and it shares the same minimums
(5 qualifying sectors, 5+ stocks each, 10+ stocks in the portfolio), so the same
thin JNB universe forces the same outcome: **82 of 104 quarters in cash**, leaving
22 investable quarters clustered in 2018-2025, with a 1.76% CAGR that is a cash
drag figure rather than a strategy result. The universe is only 96 symbols with
sector data. Evidence kept at `sector-momentum/results/returns_JNB.json`.

Note the count: the pre-fix run reported 85 cash quarters, but 3 of those were
trailing stubs counted by the `cash_periods` bug described below, not real cash
decisions. 82 is the corrected figure. Dropped from `presets_to_run` in
`sector-momentum/backtest.py`, from the content blog set, and from every
"14 exchanges" string. The study is now 13 exchanges.

**Generalisation:** any sector-ranked strategy carrying the 5-sectors x 5-stocks
minimum will collapse on JNB. Check the invested-quarter count before reporting a
JNB row, in either direction (top-N or bottom-N).
**Checked:** 2026-08-19

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

### sector-momentum: Germany clean, Switzerland weakens but holds (measured 2026-08-19)

A sector-ranked screen buys every qualifying stock in the top 2 sectors rather
than a fixed top-N list, so it has far more room to fill from domestic names than
the 30-stock value screens above. Both European markets stay invested and neither
flips sign:

| Market | As listed | Domicile-only | Delta | Invested quarters |
|--------|-----------|---------------|-------|-------------------|
| XETRA  | +4.22% | **+6.66%** | +2.44pp | 104 -> 104, unchanged |
| SIX    | +5.40% | +3.53% | -1.87pp | 99 -> 94 |

**Germany moves the opposite way from the fcf-yield result**: restricting to
domestic names roughly halves the universe (59.4 -> 30.5 avg holdings) and the
result *improves*, so the published listed-universe figure is the conservative
one. Switzerland gives back about a third of its edge and loses 5 quarters to
cash, which is a real dependence on foreign lines but not a sign flip.

**This does not contradict the fcf-yield finding, it bounds it.** The domicile
effect bites when the domestic universe cannot fill the screen. A fixed-size
top-30 screen on XETRA cannot fill domestically; a sector screen with a 10-stock
floor can. So the exposure is a property of the screen's size demand, not of the
exchange alone. Check the invested-period count per strategy, not per market.

**The diagnostic is the invested-period count, not the country.** If it is
unchanged under `--domicile-filter`, the screen was already domestic and the
result stands. If cash periods jump, the apparent alpha belonged to foreign
secondary listings. Check that number before assuming any market is safe or
suspect.

### Event studies are far worse, and it forced a live retraction (measured 2026-08-28)

`analyst-revision` is an event study on FMP `stock_grade`, and the contamination is
much heavier than on any fundamental screen measured so far. A screen at least
ranks whatever is domestically available; an event study inherits whatever the
data vendor covers, and analyst grade coverage concentrates hard on US names.

| Exchange | Domiciled locally | US-domiciled | Other foreign |
|----------|-------------------|--------------|---------------|
| XETRA | **1.5%** | **88.1%** | 10.4% |
| LSE | 6.2% | 85.7% | 8.1% |
| SIX | 8.2% | 74.2% | 17.6% |
| TSX | 90.5% | 7.4% | 2.0% |

The most-graded XETRA tickers are the Frankfurt lines of US mega-caps: `NFC.DE`
(Netflix, 647 events), `INL.DE` (Intel, 609), `TL0.DE` (Tesla, 557), `APC.DE`
(Apple, 526). The published "+1.63% German upgrade drift vs the DAX" was those.

**The invested-period diagnostic above does not work here.** An event study has no
rebalance calendar and never holds cash, so nothing degrades visibly. Use the
direction test instead.

#### The direction test: run the signal AND its inverse

**If both come out the same sign against the benchmark, you are measuring the
benchmark.** A real signal cannot pay off in both directions.

| Exchange | US-domiciled, after UPGRADES | after DOWNGRADES |
|----------|------------------------------|------------------|
| XETRA | +1.485% (sig) | +0.649% (sig) |
| LSE | +1.038% (sig) | +0.250% (ns) |
| TSX | +6.705% (sig) | +5.342% (sig) |

Canada is the clearest case precisely because it is otherwise the cleanest market:
7.4% of TSX events are US-domiciled and that sliver returns +6.71% after upgrades
and +5.34% after downgrades against the TSX Composite. Nobody reads +5.34% after a
downgrade as an analyst effect.

This tell was present in the published Germany and UK tables from March (Germany's
downgrade CAR was +0.562% at T+63, t=3.65) and was reviewed past, because the review
verified arithmetic against the results JSON and never asked whether the universe
matched the headline. **Add the direction test before publication.** It costs one
extra aggregation.

#### Domicile-only cannot always replace the result

Restricting `analyst-revision` to German companies leaves 155 upgrades across 31
names, with Deutsche Bank at 54 of them and 116 of 326 total events in 2021. That
supports no conclusion in either direction. Cause: FMP records German companies'
grades against the PRIMARY ticker, not the local line. `SAP` has 536 grade records
vs 240 for `SAP.DE`; `SIEGY` 21 vs `SIE.DE` 17; `BMW.DE` has 6 across 14 years.
Exchange-screening therefore selects exactly the tickers where domestic analyst
coverage is absent. Report that honestly rather than publishing the thin sample.

#### Reusable check

`analyst-revision/domicile_analysis.py` joins each event to `profile.country` and
recomputes CAR per domicile group using the study's own winsorization. It
generalises to any topic that writes per-event CSVs. SQL-only version is Q9 in
`ts-content-creator/content/_ready/momentum-05-analyst-revision/backtest.sql`.

**Unchecked and likely affected**, same `stock_grade` + exchange-filter pattern:
`upgrade-cluster` (same table, start here), `pead`, `revenue-surprise`,
`pre-earnings`, `beat-streaks`, `stock-split`, `index-recon`, `ma-arbitrage`,
`spinoff`.

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

### Sector-ranked screens have the same problem, via one sector (measured 2026-08-17)

`sector-rotation` does not rank on revenue, so it looked exempt. It is not. FMP files
closed-end funds and ETFs under sector **Financial Services**, and on US large caps
that sector is **79.5% funds**: 3,378 of 4,248 symbols (2,449 `isFund`, 929 `isEtf`).
It is also 59% of the whole US large-cap universe by count, so it dominates any
per-sector average.

| Sector | US large caps | funds | ETFs |
|--------|--------------:|------:|-----:|
| Financial Services | 4,248 | 2,449 | 929 |
| Technology | 525 | 0 | 0 |
| Healthcare | 490 | 0 | 0 |
| every other sector | 139-465 | 0-3 | 0-1 |

Consequences: the "Financial Services" row of the 12-month sector ranking is mostly a
fund average, and in the 19 of 104 quarters where that sector lands in the bottom two,
the portfolio is mostly funds. `sector-rotation/backtest.py --exclude-funds` (opt-in,
default off) measures it on US:

| | As screened | Funds excluded | Delta |
|---|---|---|---|
| Universe | 7,199 symbols | 3,815 | -47% |
| Avg holdings | 569 | 305 | -46% |
| CAGR | 10.60% | **11.72%** | +1.12pp |
| Sharpe | 0.324 | **0.365** | +0.041 |

Sign is positive here (removing funds *helps*), opposite to `small-cap`. Third topic,
third direction: measure, never assume. Screening logic left unchanged, disclosed in
the content.

**Still unchecked.** Only `revenue-accel`, `small-cap` and `sector-rotation` carry an
`isFund` diagnostic.
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

---

## Cost tiers are USD but market caps are local currency (found 2026-08-17)

**Status:** Capability added to `costs.py`, callers NOT yet migrated
**Affects:** every backtest that calls `tiered_cost()` (80 of them)
**Severity:** Minor. Understates costs outside the US, concentrated in Asia.

`DEFAULT_TIERS` in `costs.py` is calibrated in USD ($10B / $2B breakpoints), but
FMP reports `profile.marketCap` in each company's **local** currency, and every
caller passes it straight through as `tiered_cost(mcap)`. The universe filter
already handles this correctly via `cli_utils.get_mktcap_threshold()`, which
returns a local-currency floor (₹20B for NSE, not $1B). The cost model does not.

So a ¥1tn Japanese company (about $6.7B) reads `1e12 >= 1e10` and is charged the
top 0.1% tier instead of 0.3%.

**The effect is concentrated, not uniform:**

| Currency scale | Exchanges | Effect |
|---|---|---|
| Numerically large vs USD | JPX, KSC, NSE/BSE, TAI/TWO, SET, HKSE, STO, SHH/SHZ | Under-charged, most holdings fall to 0.1% |
| Near parity | XETRA, SIX, PAR/AMS/MIL/BME, TSX | Tiered roughly as intended |
| GBP | LSE | **Over**-charged; a £8B (~$10.1B) name gets 0.3% where USD calibration says 0.1% |

**Published numbers are not wrong.** They are what the code produced, and every
blog states costs as "size-tiered model" without quoting thresholds. The
sector-rotation video scripts now carry the currency caveat explicitly.

**Fix status.** `costs.py` gained `FX_PER_USD` and `get_fx_per_usd(exchanges)`,
and `tiered_cost()` takes `fx_per_usd=1.0`. **The default preserves historical
behaviour exactly** (verified against the old implementation across boundary
cases), so nothing has changed yet. Migrating a caller is one line:

```python
fx = get_fx_per_usd(exchanges)          # resolve once, outside the loop
cost = tiered_cost(mcap, fx_per_usd=fx)
```

**Do this per strategy at its next scheduled rerun, not as a bulk sweep.**
Flipping all 80 at once desyncs every published result in the corpus from the
code in one step. Expect non-US CAGR to move down slightly when a strategy is
migrated (more holdings at 0.3-0.5% instead of 0.1%), and LSE to move up. That
delta is the fix landing, not data drift.

**Migrated so far:** `sector-momentum` (2026-08-19), the first caller. Note the
direction is set by the currency, not by "non-US": `FX_PER_USD` is above 1 for
INR (83), JPY (150) and KRW (1350), which scales the USD thresholds up and pushes
holdings into more expensive tiers, but it's **below 1** for GBP (0.79) and EUR
(0.92), which scales them down and makes LSE and XETRA holdings cheaper. So the
migration alone moves those two markets *up*. Don't read a European CAGR rising
after migration as a bug.

---

## An absent row in classify_chart_bug.py is not a pass (measured 2026-08-19)

`scripts/classify_chart_bug.py` only classifies topics that `topic_is_affected()`
returns true for. That gate needs one of two things: a result carrying a
`benchmark_name`/`benchmark` that isn't SPY, or a non-US series inside
`exchange_comparison.json` that differs from the US one.

`revenue-surprise` had **neither**, so it printed no row at all, and a `grep` for
it came back empty. It was not fine. Its `build_output()` never wrote a benchmark
field, and its `exchange_comparison.json` was a stale pre-local-benchmark run
still keyed `BSE_NSE`. Meanwhile the published comparison chart had been plotting
excess-vs-SPY under the label "Excess vs SPY" for two months while every number in
the blog was excess-vs-local-benchmark, so India rendered positive where the table
said -1.87% and the UK rendered negative where the table said +2.24%.

Three lessons, in order of how much they cost:

1. **Empty grep output means "not evaluated", not "OK".** Confirm the topic
   actually appears in the classifier's output with an explicit verdict. If it
   doesn't appear, that is the finding.
2. **Make results self-describing.** Add `benchmark` and `benchmark_name` to
   `build_output()` when reworking any topic. Without them the results cannot be
   audited later and the topic is invisible to the classifier. Stamping is safe
   only when the name is recomputed from `get_local_benchmark(exchanges)` AND the
   series is verified to differ from the US one; a hand-typed stamp on a stale run
   is the income-quality/sustained-roic failure.
3. **Regenerating charts is not enough. Open the PNG.** Both published charts here
   were internally consistent with a *previous* run and looked entirely plausible.

### Related: --frequency annual was silently generating quarterly dates

`revenue-surprise/backtest.py` passed `months=DEFAULT_REBALANCE_MONTHS` for every
frequency, so `--frequency annual` produced quarterly rebalance dates while
`periods_per_year=1` annualized as though there were one period a year. Any
frequency comparison run that way is meaningless. Check other topics that hardcode
a `months=` argument alongside a `--frequency` flag. Fixed by pinning months only
when `frequency == DEFAULT_FREQUENCY`.

---

## Exit-side survivorship in the invested branch (recorded 2026-09-03, NOT fixed)

**Status:** Open. Pre-existing on `main`, deliberately left unchanged by the B005
post-price floor guard.
**Affects:** all 66 floor topics, and every topic that averages `filter_returns`'
output.

When the book clears the floor at entry, the period return is the mean of the
names that *survived to the exit date*. `filter_returns` drops a name whose EXIT
price is missing, so a holding that delisted to zero contributes **nothing**
rather than -100%. It also drops a name whose realised return exceeds
`max_single_return`, which is upside-only, so that drop removes winners. The two
pull in opposite directions and neither is measured.

This is not a new defect. It is what every topic did before the floor guard
existed, and the B005 correction preserves it byte-for-byte on purpose: that
change is about thin books **at entry**, and quietly altering exit-side behaviour
under cover of a look-ahead fix would move published numbers in the flattering
direction with no re-run to justify it.

**It needs its own decision and its own re-run.** The options are not equivalent:
drop-to-zero (assume a total loss), drop-to-benchmark (assume the position was
liquidated at the index), or exclude the period from the series entirely. Pick
one deliberately, then re-run; do not let it be settled as a side effect of some
other change.

**Related quirk, same area.** If a book clears the floor at entry but every name
later loses its exit price, the invested branch's
`sum(returns) / len(returns) if returns else 0.0` writes `portfolio_return 0.0`
with `stocks_held 0`, which `metrics.period_accounting` then counts as a cash
period anyway. Pre-existing and unchanged, but the corrected guard makes it
reachable by a second route, so decide it alongside the above.

> **That sentence was true of 62 topics and false of four, until 2026-09-03.**
> `capex-efficiency:336,338`, `graham-timing:369`, `sector-momentum:374` and
> `sector-rotation:355` had no `if ... else 0.0` fallback, so the zero-survivor
> period did not write 0.0 — it raised `ZeroDivisionError` and killed the run.
> The old exit-conditioned guard hid this by cashing out before the invested
> branch was reached; correcting the guard made it reachable. All five sites
> now carry the fallback and the paragraph above holds everywhere. An auditor
> reading this section before that fix was told they were safe when they were
> not, which is why it is recorded here rather than only in a commit message.
> Covered by scenario S6 in `scripts/test_floor_guard.py`, on both routes (lost
> exit prices, and every survivor tripping `max_single_return` — the second is
> the only one that reaches capex-efficiency, whose "no prices for either leg"
> pre-guard absorbs the first).

**Bounded imprecision in the entry leg itself (pre-existing, not a regression).**
The cash rule reads the ENTRY leg only, never the exit leg. That is the claim
that holds. It is *not* the same as "strictly true on the rebalance date":
`get_prices` searches forward up to `window_days=10`, so a name whose first
trade is nine days after the rebalance counts as buyable; and
`remove_price_oscillations` deletes rows using `LEAD(adjClose, 1)` and
`LEAD(adjClose, 2)`, so whether a day's price survives into the map depends on
the two bars after it. Both are identical on main — `filter_returns` read the
same map — so neither is introduced here, and the forward window pushes periods
from cash to invested, the conservative direction for this guard. Worth stating
because the surrounding argument is entirely about precision of claims.

---

## No committed result can be attributed to the cash rule yet (recorded 2026-09-04)

**Status:** Open, and it clears only by re-running. The code side is done.
**Affects:** the whole committed corpus — 2,884 period records, of which **0**
carry the entry funnel.

> **2,884, not 2,987.** A working tree can report 2,987 because
> `graham-timing/results/us.json` holds 103 more and is UNTRACKED — `.gitignore`
> line 12 is `*/results/`, and that one file was never force-added. 2,884 is
> what a clean checkout sees, so 2,884 is the number. Same trap as
> `volume-confirmed-momentum/results/vcm_osl.json`, which made
> `scripts/scan_results_invariant.py` report "the scanner lost its teeth" on
> every checkout but the author's; that one was fixed by committing the file.
> Quote the clean-checkout figure or the next reader cannot reproduce it.

Every period record of every floor topic now writes `screened`,
`entry_buyable` and `min_stocks` beside `stocks_held`, so a cash period can be
attributed to the screen (never priced) or to the post-price guard (priced,
book below the floor), and an invested period carries the count it was bought
on. The schema is in `data_utils.py` above `period_state()`; gate 13 in
`scripts/verify_floor_guard.py` keeps it present and entry-knowable; S7 in
`scripts/test_floor_guard.py` keeps it describing the period it sits in.

**None of that reaches anything already published.** Nothing was re-run, so the
funnel exists only in code. Measured with `scripts/scan_results_invariant.py`:
2,987 records skipped as pre-funnel, 0 checked. Until a leg is regenerated, the
only way to tell a pre-screen cash period from a guard-fired one in a committed
artifact is still git forensics or a re-run — and for five of the six topics
where the guard actually fires (fcf-growth, pe-compression, cyclical-timing,
sector-momentum, sector-rotation) it is not possible at all, because they
serialize only the aggregate `cash_periods`, which lumps the two together.
graham-timing is the exception; it is the sole topic that commits `period_data`,
which is why it was the only one auditable during B005.

The scanner prints the two counts on every run. When the first re-run lands,
"funnel records 0 checked" starts moving. If it does not move after a re-run,
the re-run did not pick up this code.

---

## Open, found during B006 (period accounting), NOT fixed there

All were found while reworking period counting. None is a counting bug in the
new helper, and fixing any of them changes published numbers or reaches outside
this repo, so each needs its own re-run or its own owner.

### graham-timing scores an unpriceable period as a 0% benchmark return

`graham-timing/backtest.py:379`:

```python
spy_returns.append(bench_return if bench_return is not None else 0.0)
```

Every other topic drops a period the benchmark cannot price (it never enters
`valid`, so it never reaches `compute_metrics`). This one substitutes 0.0 and
feeds it in. The period is then treated as one where the benchmark returned
exactly nothing, which is a claim about the market rather than an admission of
missing data.

Consequences: excess CAGR, alpha, beta, win rate and up/down capture are all
computed against a benchmark series containing invented zeros. On an exchange
with a late-starting local index this is not a rounding matter. `^OSEAX` begins
2013-03-05, so a 2000-2025 Oslo run would substitute zeros for roughly half the
window.

It also means the topic reports no window truncation, correctly, because under
this substitution nothing is ever dropped. `window_truncated` is False and that
is honest about the accounting; it is the returns that are wrong.

Fix is to drop the period as every other topic does, then re-run. That will move
the published numbers, which is why B006 left it alone.

### capex-efficiency/merge_results.py publishes an invented invested_periods

`capex-efficiency/merge_results.py:105` emits:

```python
"invested_periods": len(period_returns),
```

with no `cash_periods` anywhere in the file. That number is the count of periods
that produced a return, i.e. executed periods, published under a name that means
"periods where the strategy held stock". Any leg that sat in cash is counted as
invested.

It cannot be fixed in place: this script merges per-exchange CSVs, and those CSVs
do not carry a cash count. The topic is already `SCHEMA_EXEMPT` in the floor
guard for the related reason that its per-period data lands in
`results/returns_*.csv` rather than JSON. It is listed in
`scripts/verify_floor_guard.py` under `ACCOUNTING_EXEMPT` with a pointer here.

Fix is to have the topic write a cash count into its CSVs, then re-run.

Note that B005 made this overstatement WORSE, which the entry above does not
say. Commits `0d36652` and `a0760d4` turned two bare `continue`s into cash rows
(return 0.0, stocks_held 0) that previously never reached the CSV at all, and
`8a2b5b5` removed the `msg == "invested"` filter that main carried. So cash
periods now both reach the CSV and compound into the series, and every one of
them is published as an invested period. It is the only site in the repo where
a period B005 makes cash is miscounted by the B006 regime.

### oversold-quality charts render 202% cash until its OSL leg is re-run

`oversold-quality/generate_charts.py` used to carry a hand-rolled filter
(`>= 0  # drop rows with broken benchmark coverage (e.g. OSL)`) that hid the
broken row rather than fixing it. B006 removed it, on the reasoning that a
visibly wrong chart beats a silently dropped exchange.

That is only safe if the next person to regenerate charts knows what they will
see. The committed pre-B006 record (`oversold-quality/results/exchange_comparison.json`,
OSL: n_periods=50, cash_periods=101, invested_periods=-51) has no
`total_rebalances`, so the `.get("total_rebalances") or .get("n_periods")`
fallback divides by the truncated 50 and prints `100 * 101 / 50 = 202%`.

**Re-run the OSL leg before regenerating oversold-quality's charts.** The 202%
is a pre-B006 record being rendered by post-B006 code, not a new defect, and it
disappears the moment the record carries a real `total_rebalances`.

### sector-momentum and sector-rotation count unrunnable trailing periods as cash

Both run a quarterly rebalance grid to `BACKTEST_END + 1` (2026-10) while their
price fetch caps at `2026-03-01` (`sector-momentum/backtest.py:152`,
`sector-rotation/backtest.py:149`). The trailing periods cannot be run, but
neither topic skips them: they are recorded as `portfolio_return: 0.0` with
`stocks_held: 0` (sector-momentum:303/338, sector-rotation:286/320), which is
the same shape as a genuine cash period.

So they count as cash, and under the B006 convention they also count toward
`total_rebalances`. An AST census confirmed no `results.append` anywhere in this
repo writes a None `portfolio_return` (204 dict literals checked), so the
`executed = [r for r in results if r["portfolio_return"] is not None]` filter
excludes nothing today. Comments in both topics and in `metrics.py` previously
claimed it did; they have been corrected.

Committed data shows every leg at `n_periods=104` against a 107-iteration grid,
so the inflation is 3 periods per exchange. On re-run, sector-rotation ASX goes
3/104 (2.9%) to 6/107 (5.6%) and sector-momentum HKSE 4/104 (3.8%) to 7/107
(6.5%): roughly a doubled cash rate, none of it honest cash.

**This matters for how the re-run is read.** The general note that cash rates
rise under convention (B) because pre-benchmark cash periods re-enter the count
is true and is the fix working. For these two topics, about 3 periods per leg is
NOT that; it is phantom.

Fix is to cap the rebalance grid at the price-fetch date, or to skip rather than
append past it. Either changes published numbers and needs a run to verify, so
B006 left it alone.

### ts-content-creator's fact-check gates use the wrong cash denominator

Two scripts in the sibling content repo validate published copy against these
results JSONs, and both predate the convention:

- `ts-content-creator/scripts/check_pct_tokens.py:30` computes
  `d["cash_periods"] / d["n_periods"] * 100`. Under convention (B) that mixes
  populations. On the real committed `52-week-low/results/returns_OSL.json`
  (n_periods=50, total_rebalances=95, cash_periods=84, invested_periods=11) it
  yields 168.0% and 22.0% where the true rates are 88.4% and 11.6%. Those values
  go into the `canon` set the checker validates against, so the effect is
  inverted: a writer who correctly states Norway's 88.4% cash rate gets it
  rejected as unsourced, while 168.0% is blessed as canonical. Fix is
  `d.get("total_rebalances") or d["n_periods"]`, the same fallback used at
  `nse_arena/framework.py:338` and `oversold-quality/generate_charts.py:299`.
  Note also that the divide is unconditional, so a leg with `n_periods == 0`
  raises ZeroDivisionError. None exists in committed data today, but under
  convention (B) a fully-truncated leg is a reportable record rather than a
  suppressed one, which makes that case reachable.

- `ts-content-creator/scripts/check_nonpct_tokens.py:164-180` matches
  `"sat in cash for X of the Y years"` and compares only `X` against
  `cash_periods`; `m.group(2)`, the year count, is never referenced. It is
  therefore blind to precisely the window length defect (b) corrupts, and
  "sat in cash for 84 of the 25 years" passes cleanly. Fix is to compare the
  denominator against `total_rebalances` and report a mismatch with the leg's
  `window_label`.

Both are outside this repo and outside every gate here, so B006 did not touch
them. **They must be fixed before the first re-run topic is fact-checked**, or
the gate will reject correct numbers and accept wrong ones.

### Re-run population, and the 13 live posts that depend on it

`scripts/scan_results_invariant.py` flags **22 arithmetically impossible records
across 13 topics** at this commit. Every one is an OSL (Oslo) leg except
`qarp` JNB, which is the separate 100%-cash class: JNB has no
`LOCAL_INDEX_BENCHMARKS` entry, falls back to SPY and prices nothing.

`^OSEAX` starting 2013-03-05 is the whole cause, re-verified against the
warehouse on 2026-09-03: `^OSEAX` 2013-03-05 (3,377 rows), `^TWII` 1997-07-02
(7,179), `^SET.BK` 1982-01-04 (11,247). It is the only `LOCAL_INDEX_BENCHMARKS`
symbol starting after 2000. The blocker also named TAI and SET as suspect; they
are not.

| topic | published post | n_periods | cash | invested | years |
|---|---|---|---|---|---|
| deleveraging | risk-03-deleveraging (2026-02-05) | 50 | 53 | -3 | 12.5 |
| earnings-consistency | growth-02-earnings-consistency (2026-02-26) | 12 | 25 | -13 | 12.0 |
| etf-underowned | etf-03-underowned (2026-03-28) | 12 | 20 | -8 | 12.0 |
| ev-ebitda | value-03-ev-ebitda (2026-01-17) | 11 | 13 | -2 | 11.0 |
| ev-ebitda-relative | timing-02-ev-ebitda-relative (2026-02-09) | 11 | 25 | -14 | 11.0 |
| fcf-compounders | cashflow-04-compounders (2026-03-13) | 12 | 25 | -13 | 12.0 |
| fcf-yield | value-04-fcf-yield (2026-04-05) | 12 | 23 | -11 | 12.0 |
| garp | factor-08-garp (2026-02-07) | 50 | 73 | -23 | 12.5 |
| oversold-quality | reversion-03-oversold-quality (2026-01-24) | 50 | 101 | -51 | 12.5 |
| qarp | factor-02-qarp (2026-01-01) | 0 | 51 | -51 | 0.0 |
| relative-strength | momentum-07-relative-strength (2026-02-23) | 50 | 65 | -15 | 12.5 |
| value-momentum | factor-03-value-momentum (2026-03-23) | 24 | 29 | -5 | 12.0 |
| volume-confirmed-momentum | momentum-08-volume-confirmed (2026-02-24) | 24 | 40 | -16 | 12.0 |

> **This table covers defect (a) — impossible arithmetic — ONLY.** The
> invariant check flags `invested_periods < 0`. Silent window truncation,
> defect (b), leaves no arithmetic trace whenever `cash_periods <= n_periods`,
> so it is structurally invisible to that check and the 13 here are not the
> whole population. `scripts/scan_results_invariant.py` now carries a second
> detector for it (`truncated_legs`): inside one `exchange_comparison.json`
> every leg shares a rebalance grid, so the file's maximum `n_periods` is
> `total_rebalances`, and a leg materially below it was cut. Measured
> 2026-09-03: **18 truncated legs, every one OSL. 9 also carry a negative
> `invested_periods` and are in the table above. 9 do not.** Of those 9,
> `52-week-low` is the correct prior art and `garp` is already in the re-run
> set via its `returns_OSL.json`, leaving **7 topics outside the documented
> population**: `52-week-high` (50 of 103), `etf-rebalancing` (12 of 20),
> `pe-compression` (11 of 25), `peg-ratio` (50 of 103), `price-momentum`
> (24 of 51), `price-to-book` (11 of 25), `yield-gap` (11 of 25).
>
> Because their `invested_periods` was never negative, **nothing ever
> suppressed them**: `peg-ratio/generate_charts.py` and
> `price-to-book/generate_charts.py` gate on `invested_periods > 0` and OSL
> passes, so those comparison charts already render a 2013-2025 Norway bar
> among 2000-2025 siblings, live today. The re-run population is the UNION of
> both detectors. Two cautions for whoever works it: the published peg-ratio
> comparison post's numbers do not match its current committed results file, so
> it needs the same per-post verification the 13 above got; and `yield-gap`'s
> `returns_OSL.json` says n=25 while its `exchange_comparison.json` node says
> n=11, so only the comparison node — the one the charts read — is truncated.

**The count of affected live posts is 13, not the 4 the blocker estimated.** All
13 are published. 12 make a substantive Norway claim in their comparison blog:
8 as a table row or figure (growth-02, etf-03, value-03, timing-02, factor-08,
reversion-03, factor-03, momentum-08) and 4 in prose only (risk-03,
cashflow-04, value-04, momentum-07). qarp's bad leg is JNB, so it carries no
Norway row at all. Note the shared dividend-disclosure paragraph names "Oslo All
Share" in almost every comparison blog; those boilerplate mentions are excluded
from this count. Two mappings are not in
`scripts/copy_charts_to_content.py`'s `TOPIC_DIRS` and were resolved from the
results filenames instead: `fcf-yield` publishes as `value-04-fcf-yield` (its
results files are literally named `value-04-fcf-yield_norway.json`), and
`etf-underowned` as `etf-03-underowned`.

The published cash rates **cannot be reproduced from the committed records** and
must be re-derived after the re-run rather than relabelled. Examples:
reversion-03 states "98-99% cash" where `cash/n_periods` is 202%; timing-02
states "100% cash across all 25 years" where the record is 25 cash against
`n_periods` 11; factor-03 states "57% cash" against 29/24 = 121%.

Two specific published claims are wrong on their face, beyond the window label:

- `value-03-ev-ebitda`, comparison blog line 163: "Results for markets like
  Norway and Israel before 2005 are based on fewer companies than later years."
  There is no pre-2005 Norway benchmark coverage at all. `^OSEAX` begins
  2013-03-05, so the sentence asserts coverage that does not exist. Its Norway
  row (line 48) also sits under a 2000-2025 heading with no truncation note.
- `timing-02-ev-ebitda-relative`, line 57: "The Oslo All Share series in this
  dataset starts in 2014." It starts 2013-03-05.
- `momentum-07-relative-strength`, line 28, is a third and subtler class:
  "Norway (OSL) ... had more than 5 consecutive years of zero qualifying stocks
  at the start of the backtest period." That is defect (b) restated as a finding
  about the market. The early years are missing because `^OSEAX` cannot price
  them, not because the screen found nothing. Any prose that explains a
  benchmark coverage gap as strategy behaviour needs re-checking, not just
  relabelling.

`factor-03-value-momentum` (lines 44, 108) and `reversion-03-oversold-quality`
(line 58) already disclose the 2013 start and are the least wrong of the set;
their cash rates still need re-deriving.

Correcting these is an editorial task and needs Ghost care (bulk republish
unpublishes first, and a 5xx mid-flight strands a post as a 404). B006
deliberately touched no post. Relabel-versus-drop for each Oslo leg is also an
editorial call and is deliberately not made here.

### Seven cohort topics still count cash over the metrics window

`altman-z`, `asset-light`, `cash-conversion`, `income-quality`,
`margin-expansion`, `roe-dupont` and `sustained-roic` emit
`"cash_periods": {"high": n, "low": m}` from 14 sites that all read
`sum(1 for p in valid if ...)`. That is convention (A), the rejected one: on a
truncated leg it understates the cash rate, and these records carry no window
provenance at all.

They are excluded from gate 8 by structure, with a documented rationale: a
cohort mapping has no single `invested_periods` to derive and no single measured
window. That rationale covers the DERIVATION but is silent on the SCOPE, and the
scope half still applies. Gate 7 cannot see them either, because they bind to
`high_cash`/`low_cash` rather than `cash_periods`.

Latent, not live: none of the seven has an OSL or Norway results file, and
`^OSEAX` is the only `LOCAL_INDEX_BENCHMARKS` symbol starting after 2000
(verified: `^OSEAX` 2013-03-05, `^TWII` 1997-07-02, `^SET.BK` 1982-01-04).

Flipping the 14 counts needs per-topic verification that each cohort predicate's
keys exist on unpriced rows, which is not a mechanical edit, so B006 left them.

### Truncated legs would have re-entered every ranked comparison chart

Fixing `invested_periods` removes an **accidental filter**.
`invested_periods > 0` is the standard "did this leg produce data" gate in 42
`generate_charts.py` files. Under the old arithmetic a benchmark-truncated leg
produced a NEGATIVE `invested_periods` and silently failed it. Under
`invested_periods = total_rebalances - cash_periods` it is always `>= 0`, so the
leg re-enters — measured over roughly half the window its siblings are measured
over, under a footer still printing the full span.

Measured on the committed corpus, **7 legs flip from excluded to included**, all
OSL, and the direction is uniformly flattering:

| topic | invested_periods | n of grid | max drawdown | sibling median |
|---|---|---|---|---|
| deleveraging | -3 → 50 | 50 of 103 | -24.24 | -58.65 |
| ev-ebitda | -2 → 12 | 11 of 25 | -14.74 | -46.07 |
| oversold-quality | -51 → 2 | 50 of 103 | 0.00 | -47.45 |
| relative-strength | -15 → 38 | 50 of 103 | -26.96 | -56.11 |
| value-momentum | -5 → 22 | 24 of 51 | -14.72 | -48.57 |
| volume-confirmed-momentum | -16 → 9 | 24 of 49 | -19.24 | -40.13 |
| yield-gap | 0 → 14 | 11 of 25 | -7.33 | -45.29 |

CAGR is above the sibling median in 6 of the 7. `chart_comparison_cagr` sorts by
CAGR, so Norway would land near the top of a ranked bar chart as a high-return,
low-drawdown market. `pe-compression` is an eighth case that flips only against
its stricter `MIN_INVESTED_PERIODS = 10` gate (2 → 16).

**Fixed by excluding truncated legs**: every one of those 42 files now gates on
`window_truncated` as well, enforced by gate 12 in
`scripts/verify_floor_guard.py`. This is deliberately NOT a new editorial
decision — it reproduces on purpose what the old arithmetic did by accident. It
is also a no-op on today's corpus: no committed results file carries
`window_truncated`, so `.get(..., False)` is falsy everywhere and the charts are
byte-identical until a re-run.

**Two consequences to expect after the re-run, and one open call for a human.**
Once re-run records carry `window_truncated = True`, the ~8 currently-rendered
truncated legs from the defect-(b) list above (`peg-ratio` OSL and the rest)
will DROP out of ranked comparisons. That is intended: it is the fix for the
live 2013-2025-Norway-bar-under-a-2000-2025-header defect. But the predicate is
blunt — it excludes ANY truncated leg, including a trivially truncated one.
**Whether a truncated leg should instead appear WITH its measured window
annotated on the bar is a real editorial question and it is deliberately left
open.** `window_truncated` / `window_label` are already carried into every
results record and already read by the 54 stdout printers, so the chart layer
only needs to read them if that is the call.

### Bisect warning: three interior commits publish mismatched denominators

B005 and B006 must land as ONE unit. b005's tip folds in b006's 52-week-low hunk
so the branch tips and the merge product are all correct, but three interior
commits are not: **`9fadc81`, `75bf3a5` and `037ad42`** count cash over `valid`
while `build_output` still derives `invested_periods` from `len(results)`. On
52-week-low — the only affected topic — that overstates invested periods by
4-5x (true 11, would publish 45-56). Nothing is published from an interior
commit, because this branch runs no backtests and commits no results, so the
only exposure is a bisect or a cherry-pick landing on one of the three. Do not
cherry-pick them in isolation; squash-merge b005 if the interior states must not
reach the mainline at all.

A second window is bisect-unsafe for a different reason. Between **`bc1772d`**
(which corrected the cash rule) and the commit that added the `if ... else 0.0`
fallbacks, `capex-efficiency`, `graham-timing`, `sector-momentum` and
`sector-rotation` raise `ZeroDivisionError` on a zero-survivor period. See the
boxed note under "Exit-side survivorship" above.
