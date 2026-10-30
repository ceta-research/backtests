# Data Quality Issues

Known data quality issues affecting backtests. All strategies in this repo are affected unless noted otherwise.

Last updated: 2026-10-30

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

**defensive-quality strategy (sector-04):** not a data quality issue. The defensive-sector universe (Consumer Defensive, Utilities, Healthcare) never produces 10 qualifying stocks at a July rebalance: at most 8 pass in any year, so the strategy stays in cash in all 25 periods. JNB is not in `presets_to_run` in `defensive-quality/backtest.py`; it is run on its own into `returns_JNB.json`, which reads 25 cash years of 25 in the published file and in the October 2026 rerun, and it is too thin to test. (Updated 2026-10: this entry said JNB was excluded from all content; the comparison post reports it as 100% cash.)

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

Since 2026-10-30, `max_single_return=None` means no upper test. deleveraging and cyclical-timing pass it and use the data checks instead (see "Data checks in place of the return cap" below); the default stays 2.0 for every other caller.

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

**Update 2026-10: fcf-yield was rerun with the rule on by default.** Its XETRA leg now
holds Germany-domiciled companies only, its SIX leg Switzerland-domiciled only and its LSE
leg pence-quoted lines only (see "Rerun topics apply listing rules" below). The rerun also
carries the entry-side cash guard, the same-ISIN dedup and last-traded-price exits, so its
numbers aren't the domicile-only column above with one change. Published against rerun:

| Market | Cash years of 25 | CAGR | Excess vs local index |
|--------|------------------|------|-----------------------|
| XETRA  | 5 -> 22 | 9.05% -> 0.86% | +4.01 -> too thin to test |
| SIX    | 9 -> 25 | 4.30% -> 0.00% | +2.56 -> never invested |
| LSE    | 3 -> 8  | 9.98% -> 6.71% | +8.75 -> +5.48 |

Germany and Switzerland can't be tested on this screen at all. The UK lead is against the
FTSE 100, a price index.

### Sweden is NOT affected (measured 2026-08-13)

Sweden was the largest open exposure in the sweep (27 topics carry a Sweden
blog) and had never been tested. It is now measured on two independent
instruments and is **clean on both**:

| Instrument | As listed | Domicile-only | Delta | Invested periods |
|------------|-----------|---------------|-------|------------------|
| price-to-sales STO (90 invested periods) | 10.65 / 10.69% | 10.36% | **-0.31pp** | 90 -> 90, unchanged |
| fcf-yield STO (thinly invested) | 0.92% | 0.23% | -0.69pp | 5 -> 3 of 25 |

Two as-listed figures are shown because **these backtests were not deterministic** when
this was measured (fixed 2026-10-10, see the note below). The delta is roughly 8x the
spread measured on this screen, so it is real but small.

Neither flips sign. The price-to-sales run is the reliable one: 90 invested
periods before and after, so the screen still fills entirely from domestic
names. The fcf-yield run agrees in direction but is low-powered, invested in
only 5 of 25 periods even before the filter, and should not be leaned on.

**Sweden behaves like Japan, Canada and India, not like Germany and
Switzerland.** This matters for the mechanism: the domicile effect bites when
the domestic universe cannot fill the screen, not because a market is European
or small. Stockholm has enough domestic listings to fill a 30-stock screen;
Frankfurt and Zurich do not.

**Update 2026-10: both instruments are now too thin to test.** The domicile comparison
above was made on the old screens and has not been repeated (Stockholm still has no listing
rule). The "90 invested periods" it rested on came from the old price-to-sales screen,
which kept companies on stale filings. In the
October 2026 rerun price-to-sales STO is invested in 50 of 103 quarters (53 cash: 49 with
fewer than 10 names passing the screen, 4 under the entry-side cash guard), at 3.53% a
year against a published 10.96% with 13 cash quarters. Its books average 12.5 share lines,
and in 46 of the 50 invested quarters the ten recorded holdings include two lines of one
company (VOLV-A.ST with VOLV-B.ST, SSAB-A.ST with SSAB-B.ST). fcf-yield STO is invested in
2 of 25 years (5 before): the three years it lost (2010, 2011 and 2021) are guard years,
with 10 names passing the screen and 8 or 9 of them buyable. "Stockholm can fill a 30-stock
screen" was true of the old screens. It isn't true of price-to-sales after its
latest-filing rule and the guard, or of fcf-yield after the guard (fcf-yield has no
latest-filing rule).

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

### yield-gap: invested periods go UP under the filter (measured 2026-09-11)

Run alongside the buyable-at-entry floor (B005), which refuses a rebalance unless
10 screened names have a usable entry price. That changes what the domicile
filter does:

| Market | As listed | Domicile-only | Delta | Invested periods |
|--------|-----------|---------------|-------|------------------|
| XETRA  | +2.11% | +1.85% | -0.26pp | 22 -> **23** |
| SIX    | +5.53% | +3.67% | -1.86pp | 18 -> **20** |
| WSE    | -4.78% | -3.80% | +0.98pp | 14 -> 11 |

Germany and Switzerland become **more** investable when foreign listings are
removed, the opposite of the fcf-yield pattern. The reason is that the foreign
secondary lines are disproportionately the ones with no tradeable entry price, so
they were consuming top-30 slots and pushing the book below the floor without
contributing a position. Strip them out and the domestic names that remain are
fewer but all buyable.

**Consequence for the diagnostic:** once a strategy enforces a buyable-at-entry
floor, "cash periods jumped" is no longer the only tell, and a *drop* in cash
periods under `--domicile-filter` is evidence the listed universe was padded with
untradeable lines rather than evidence the filter is harmless. Read the excess
delta alongside it. Here neither European market flips sign, so the published
listed-universe figures stand, with the SIX edge understood as roughly a third
foreign-listing driven.

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

### Backtests were not bit-reproducible until 2026-10-10 (found 2026-08-13)

Running the same backtest twice with identical arguments did not give the same number.
First measured on `price-to-sales --preset sweden`, three runs minutes apart:

| Run | CAGR |
|-----|------|
| pre-edit code | 10.68% |
| post-edit code, domicile OFF | 10.65% |
| post-edit code, domicile OFF, repeat | 10.69% |

That spread was 0.04pp, with invested periods and the benchmark series identical in
every run. It was not the ceiling. On 2026-10-09, 28 legs were run twice on identical
code minutes apart: 24 matched and **4 differed, by up to 0.58pp CAGR**. asset-growth
LSE alternated between 6.04% and 6.62% over four runs, because its 2000 rebalance found
9 or 10 buyable names of 30 against a 10-stock floor, so the whole year flipped between
cash and invested. A leg sitting at its stock floor turns a one-name wobble into a cash
year.

**Cause.** The fetched data was identical; the selection wasn't.

1. **Ties in orderings that were not total.** DuckDB runs a query in parallel and breaks
   `ORDER BY` ties differently from run to run. Twin listings with identical
   fundamentals (SET `-R` NVDR lines, share classes, one company listed twice) tied
   exactly at the top-N cut, so a tie group straddling rank N picked arbitrary names.
   Latest-row dedups (`ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch
   DESC) = 1`) kept an arbitrary row when two filings shared an epoch.
2. **Local-time date bounds.** Window bounds were built from the machine's local
   midnight while bar epochs are UTC midnight, so which bars fell inside a window moved
   with the machine's timezone and daylight saving.

**Fix (2026-10-10).** Every ordering in the selection path is now a total order: top-N
cuts end on `symbol`, latest-row dedups break ties on the value columns they go on to
read, price picks break ties on `adjClose`, and rounded float aggregates that feed a
ranking are computed exactly. Epoch bounds use `data_utils.utc_epoch()`. Orderings that
were already total, cut sizes and thresholds are unchanged. The rules are D1-D4 in
`scripts/LTP_GATE.md`, whose `det` check runs a leg twice and requires identical JSON.

Two consequences:

1. **On current code, a rerun on the same data reproduces exactly.** A difference
   between two same-day runs of the same code means the data changed between the
   fetches: compare the logged row and symbol counts before suspecting the code. The old
   rule here, "a difference under ~0.05pp is noise", no longer applies, and it was too
   tight for older code anyway: results committed before 2026-10-10 can carry tie-order
   noise of the 0.58pp size above.
2. **Published numbers drift.** The stored `exchange_comparison.json` for the Sweden
   screen above says 10.96%, 0.29pp away from the reruns. That gap was never attributed
   and could include tie-order noise, but data restatement and backfill between runs
   move numbers on their own, by more the longer the gap. Do not silently "correct" a
   blog to a fresh rerun without establishing which is right.

Shared helpers now live in `data_utils.py`: `EXCHANGE_COUNTRY` (36 exchanges),
`domicile_countries()` and `domicile_sql_condition()`. `--domicile-filter` is
implemented on `fcf-yield` and `price-to-sales`; 84 of 93 topics share the
identical `exchange IN ({ex_filter})` universe filter, so porting it further is
a mechanical edit rather than per-backtest work. It stays opt-in and default
OFF in those two topics, and no published result of theirs uses it. The topics
rerun with last-traded-price exits apply a home-country rule automatically
instead, on XETRA and SIX legs; see the next subsection. fcf-yield and
price-to-sales are rerun topics since October 2026, so their XETRA, SIX and LSE
legs carry the listing rules whether or not the flag is given.

### Rerun topics apply listing rules on XETRA, SIX and LSE (2026-10)

When a topic is rerun with last-traded-price exits it no longer treats these exchanges as
listed universes. Three waves have been rerun this way, twenty-three topics in all, and their
results carry `exit_method`, `listing_filter` and `isin_dedup` keys: the first wave
(cyclical-timing, asset-growth, fcf-conversion, cash-conversion and graham-net-net), the
second (deleveraging, fcf-compounders, fcf-yield, revenue-accel, dividend-sustainability,
price-to-sales, dogs-of-dow, low-debt, dividend-growth, pe-mean-revert, rising-yield and
graham-number) and the third (52-week-high, dcf-discount, defensive-quality,
pairs-fundamentals, pairs-zscore and pairs-multi-pair; the pairs topics also carry
`company_dedup`). The third wave also reran the Norway leg of 13 other topics in drop mode on
their own code, without these rules (under "The 13 Norway legs" below). The rules,
with what was measured on the first wave's previous holdings:

| Leg | Rule | Measured on the previous runs' holdings |
|---|---|---|
| XETRA | Germany-domiciled companies only | 74-78% foreign lines; 50% or more in every invested year |
| SIX | Switzerland-domiciled companies only | 22-34% foreign on average, mostly 2015-2019 |
| LSE | lines quoted in pence (GBp) only | 51-81% foreign lines on average from 2018 |
| every leg | one line per ISIN | the same security held twice under two tickers |

**XETRA and SIX.** The most-held XETRA names were US large caps: P&G, General Mills
and Pfizer in asset-growth; F5, American Express and Autodesk in fcf-conversion; UPS,
Lockheed and Boeing in cyclical-timing. A disclosure can't fix a book whose market label
is wrong. SIX also held lines quoted in USD or EUR against the CHF-denominated SMI. On
these two exchanges `profile.country` and a primary-listing rule disagree on under 2% of
holdings, so the existing domicile condition is used.

**LSE: pence, not country.** `country IN ('GB')` fails both ways on LSE. It drops
LSE-primary companies registered abroad (Experian, Coca-Cola HBC, Wizz Air, Plus500,
TP ICAP: 7-9% of lines in the three ranked topics) and keeps foreign lines that carry
country GB (Willis Towers Watson's `0Y4Q.L`). The quote-currency rule drops the
`0XXX.L` lines, which FMP carries in the home currency, and the USD and EUR depositary
receipts, and keeps every pence-quoted line. Pence does not mean UK: BHP, Glencore and
Wheaton lines quoted in GBp stay. It also empties most of graham-net-net's old LSE
book, where 136 of 143 visible lines were not pence-quoted and 85 had a quote currency
different from the reporting currency (Halyk Bank, Samsung, Kazatomprom), where the old
screen compared price and NCAV in two currencies. Before 2018 the LSE books looked
domestic mainly because the foreign lines had no prices yet.

**graham-net-net: NCAV in the price's unit (every leg).** The rerun screen also restates
`grahamNetNet` (in `key_metrics.reportedCurrency`) in the unit of `adjClose`
(`profile.currency`): x100 on sub-unit quotes (GBp, ZAc, ILA), and the newest USDxxx
close at most 7 days old on or before the price bar when the currencies differ. So
pence lines are no longer screened against pounds (100x too strict), and its HKSE, TSX
and US books change through FX. A line with no currency on either side, no fresh rate,
or a depositary receipt (`profile.isAdr`) quoted in a currency other than its reporting
currency is dropped, not compared raw, and counted in `ncav_ccy_excluded` (receipts
separately). Receipts go because FMP's per-share basis for them is mixed: against their
home lines it's per receipt for TSM, PKX and RDY but 1.7-2.2x off for TAK and CAJ, and
the converted US top 30 picked up CMCM and CCM at 0.01-0.26x NCAV (8 line-years over
2001-2024). Receipts in their reporting currency (Amarin, Formula Systems) stay and carry
the same unverified basis as before.

**graham-net-net: the latest filing decides (fixed 2026-10).** The screen loaded only
filings with positive NCAV and applied the size floor before picking each company's latest
filing, so a company whose NCAV had turned negative, or that had shrunk below the floor,
kept qualifying on its last positive filing however old it was: PIF.TO on its FY2009 NCAV
from the 2012 through the 2024 rebalance, Dorel on FY1994, Kinross on FY1997, Gilat on FY1999. It now takes the
latest FY filing on or before the 45-day cutoff and no older than 540 days, then requires
NCAV > 0 and the size floor. With the other October 2026 changes in place, this fix alone
moved TSX from 12.83% to 5.72% a year (2001-2006 go to cash: only
2-4 genuine net-nets a year), Hong Kong rose from -2.31% to 3.19%, the US from 4.62% to
7.11% and India from 8.19% to 11.11%. The 45-day lag counts from the fiscal year end, not the
filing date, so a filing can occasionally be used before it was public.

**cash-conversion: a 10-name minimum per CCC bucket (2026-10).** It had no minimum book size,
so a bucket of 1-5 names was reported as a portfolio. After the XETRA/SIX listing rules its
early German and Swiss buckets held 1-5 names and drove -65% to -91% drawdowns. A bucket with
fewer than 10 names buyable at entry now sits in cash that year (decided before exits, so
last-traded-price fills never decide it). The thin years were not only German and Swiss:
India's buckets are cash in 2000-2004 (Mid CCC 14.61% -> 12.44%), South Africa's Low CCC went
from 9.27% to -1.16% with 14 cash years, Brazil's High CCC from 16.11% to 4.44%, and
Singapore's Mid CCC is cash in 23 of 25 years. The US, China and the UK never go to cash and
match the run without the floor.

**Second wave: selection defects found by the audits and fixed before the rerun.** All
seven topics below have been rerun on the fixed code, and their committed results carry the
fixes. The rules are in METHODOLOGY.md (Filing and Selection Rules). Each entry first sizes
the defect, as measured on the old screen before any rerun, and then says what the
installed results show. No leg was run with a fix switched off, so where an effect is
credited to a fix alone, it comes from a replay of the screen with and without the fix,
made on the warehouse before the rerun. A replay is not a backtest.

**price-to-sales: the latest filing decides (fixed 2026-10, #10; rerun).** The screen cached
only passing rows and took the latest of those, so a company kept qualifying on its last
filing with P/S under 1 and, separately, its last with ROE over 10%, however old. Between 45%
and 87% of top-30 slot-periods were held by a name whose latest filing fails the screen (US
62%, 1,927 of 3,090). It now takes the latest FY filing on or before the 45-day cutoff and
no older than 540 days, then applies the six tests. The US leg went from 11.66% to 14.41% a
year with no cash quarter in either file, but the fix is one of six changes between the two
files and no run isolates it.

**graham-number: Graham number in the price's unit, and the latest filing (fixed 2026-10,
#11; rerun).** The screen compared `adjClose` (the listing's quote currency) with
`key_metrics.grahamNumber` (per share, in the filing's reporting currency) and ranked on the
raw ratio. At five sampled rebalances 23 to 27 of the US top 30 were foreign-currency
reporters (Korean and Japanese ADRs ranked first on won and yen Graham numbers), and no
pence-quoted UK line could pass against a pound figure. The Graham number is now restated
in the price's unit before the test and the ranking, the way graham-net-net's NCAV is: x100
on sub-unit quotes, the newest USDxxx close at most 7 days old when the currencies differ.
Lines with no currency or rate, and depositary receipts quoted in a currency other than
their reporting currency, are dropped and counted in `gn_ccy_excluded`. Separately, the
fetch loaded only filings with a positive Graham number, so a company whose earnings or book
value turned negative kept qualifying on its last good year. It now takes the latest FY
filing within 540 days and then tests it. In the rerun the US leg went from 7.01% to 11.80%
a year with no cash year in either file. Its file counts 2,762 line-dates dropped under
`gn_ccy_excluded` over the 25 rebalances, all of them cross-currency depositary receipts,
and by the fix's screen check 71 of the old US book's 750 top-30 slots are still held. The
UK leg went from 5 cash years of 25 to none and South Africa from 25 to 7, because pence
and cent quotes can now pass. Inside the books, the split between the two rules, the dedup
and data revised since the published run isn't determined. A line that isn't an ordinary
share ranks first in the US book in 11 of 25 years (every year from 2014), in Canada's in 9
and in the UK's in 13 (under "Not fixed yet" below), so the US figure can't be credited to
the screen until a funds-excluded leg is run.

**rising-yield: the rising run must end at the latest year (fixed 2026-10, #11; rerun).** It
took each company's newest row that passed the three-year rising test, with no
age limit, and dropped non-paying years before counting "consecutive" years. Between 59% and
77% of top-30 slots sat on a row older than the last complete fiscal year; the 2024 top 30
held AJRD on its FY1989 row. The test must now hold on the latest FY row (within 540 days),
over three consecutive fiscal years, and a zero or missing yield breaks the run. The old screen
also read only fiscal years ending in the calendar year before the rebalance or earlier; the
rerun reads the latest year end at least 45 days old, so a March year-end company is screened in
July on the year that ended three months earlier, not fifteen. In the
rerun cash years went from 65 to 227 of 425 market-years over 17 markets, 181 of them on
the screen count and 46 on the entry-side guard, and CAGR fell in 15 markets (it rose in
the US and China). Nine markets became too thin to test. Of the eight still invested in at
least half their years, 5 are ahead of their benchmark (4 of the 5 against a price index),
1 ties and 2 trail; the published files read 12 of 17 ahead. By a replay of the screen
about 123 of the 162 added cash years come from the fix and about 36 would have appeared
without it. The replay modelled a draft of the fix that kept the published screen's bound
(fiscal year at most the rebalance year minus one), so for a March year-end, common in
India and Japan, it tested in July the year that ended 15 months earlier where the rerun
tests the one that ended 3 months earlier. Its cash years differ from the rerun's in 9 of
17 markets and its counts by up to 2 a market (224 against 227 in all), and it is weakest
for India and Japan, the two largest CAGR moves among the eight markets still tested.

**pe-mean-revert: the band is tested on the latest filing (fixed 2026-10, #11; rerun).** The
P/E band (3 to 50) was applied before the latest-row pick, so the row used was
the latest filing with an in-band P/E: 21% of US top-30 slots were not the company's latest
filing. Now the latest row within 540 days is picked first and the band tested on it. The
sector median still counts same-ISIN twins twice. In the rerun CAGR fell in 11 of 13
markets and cash years went from 76 to 136 of 325 market-years. By the audit's replay of
the screen the fix replaces 760 of 5,501 top-30 slots (13.8%; 165 of 750 in the US) and
accounts for 5 of the 60 added cash years (China 2, Germany 1, Sweden 2). Germany, Sweden
and Switzerland became too thin to test (17, 16 and 24 cash years of 25). Germany's and
Switzerland's come mostly from the listing rule: 10 or more home-country names pass the
screen in 10 of 25 years in Germany and in 4 in Switzerland. Of Sweden's 6 added cash years
2 are the fix by the replay (2015 and 2016) and one is the cash guard (2009: 9 of 10
screened names buyable before the fix). The other 3 (2007, 2012 and 2014) screen 6, 6 and
7 names on the replay's code without the fix, with no twin removed, where the published
screen passed 10 or more. Taiwan 2012 is the same case on a leg already too thin. Those 4
years come from data revised since August or another code change between the published
commit and the rerun; which isn't determined.

**dogs-of-dow: yield on the latest filing, size in one currency (fixed 2026-10, #11 and #13;
rerun).** Yield: a company that cut its dividend to zero kept ranking on its last
positive-yield filing. The latest ratios filing within 540 days must now show a yield above
zero, on the Dow 30 leg and the non-US legs. Size: the non-US legs took the top 30 by raw
`key_metrics.marketCap`, which is in each filer's reporting currency and contains corrupt
values. Over 16 non-US legs and 25 January entry dates, 25.5% of blue-chip slots had no
entry price, 4.4% carried a cap no company on that exchange has reached (Japan: 11 to 18 of
30 slots a year from 2006 were delisted small caps with caps like 3.8e18 yen), and 483 of
3,753 dog slots could not be bought. Mixed currencies also reordered the cut: a yen cap
ranked first in Hong Kong and a yuan cap first in Toronto. Size is now the smaller of the
cap restated into the leg's currency and entry close x share count, an entry price is
required, and a size above 15,000 times the leg's floor takes no slot (the largest real
size is about 7,100 times its floor, the smallest corrupt one about 27,000 times). On a
screen emulation over today's data, not a backtest, 3,177 of 10,868 blue-chip slots and
1,164 of 3,753 dog slots change, every slot is priced, cash leg-years go from 36 to 30, and
the ceiling removes 73 slots (Japan 71, India 1, Korea 1). 16 slots keep a size that looks
inflated but sits under the ceiling (13 in the UK), none of them a held dog. The rerun
matches the emulation: 30 cash market-years (25 in the published files), and `size_excluded`
records 73 line-years above the ceiling (Japan 71, India on BSE 1, Korea 1) and 29,990 that
took no slot for want of an entry price. The average number of lines held when invested
rose on all 16 non-US legs (3.6 to 9.8 in the published files, 9.3 to 10.0 now), and
`entry_buyable` equals `screened` in every one of the 395 invested market-years, so the
cash guard decides no year. All 5 added cash years have fewer than 5 names passing the
screen (the topic's floor is 5). On the non-US legs a line now needs an entry price to take
a blue-chip slot and so to count toward the screen, which the published code didn't
require, so the files don't show why those counts fell. By the size-rule emulation the
five were cash before the size rule too, and an emulation of the screen before that rule
found, at the January 2001 rebalance, 5 BSE lines above the floor, 4 of them with no entry
price, which points to the cash guard. Five legs moved by more than 2
points of CAGR: UK 2.01% to 7.62%, South Africa 10.29% to 6.52%, Germany 3.15% to 6.09%,
Canada 5.97% to 8.70% and India on BSE 14.60% to 12.50%. Why a book differs in a year
invested both times isn't determined from the files: the size rule, the yield fix, the
dedup, the listing rule (on XETRA, SIX and LSE) and six months of revised data all act on
the same top 30. The published per-market files for Germany, Switzerland, South Africa and
Saudi Arabia were never committed to this repository. Their published figures here come
from files of the same April 2026 run kept outside it, dated 2026-04-12 like the 13
committed files.

**low-debt: the prior year is the company's own (fixed 2026-10, #11; rerun).** The
prior-year filing window was 185 days wide and fixed to the rebalance date, so companies
with fiscal years ending between May 18 and November 13 were never screened on any leg: 55%
of size-eligible company-years on JNB, 20% on LSE pence lines, 12% in the US. The prior row
is now the company's own previous fiscal year, 300 to 430 days before its current row. In
the rerun the US book averages 232.4 names against 196.0 in the published file, and its
CAGR went from 7.57% to 7.64%. By a replay of the screen the fix changes the cash-year
count in 3 of 15 markets and no other: Germany 8 to 6, Canada 3 to 2 and South Africa 25 to
22, where 2016, 2018 and 2022 are now invested on 11, 12 and 10 names. The topic's moves
above 2 points (Germany -2.55, Sweden -2.68, Switzerland -5.85) come mostly from elsewhere:
by the replay the fix moves no cash year in Sweden or Switzerland and takes two out of
cash in Germany. 40 market-years went from invested to cash, 31 on the cash guard and 9 on
the screen count on the German and Swiss legs, and Singapore is now in cash in 23 of 25
years and too thin to test.

**dividend-growth: the streak ends at the last complete year (fixed 2026-10, #11;
rerun).** The streak counted increases since the last recorded cut and ignored missing
years, so a company that stopped paying kept its streak: 13 of Sweden's top 30 in July
2021 had paid nothing in 2020. The streak now has to end at the last complete calendar
year, and a year with no dividend, a cut, a flat year or a gap breaks it. By a replay of
the screen the fix alone sends three market-years to cash (Germany 2024, Sweden 2023,
Switzerland 2021) and none out, and it changes Sweden's book most: 54 of 320 top-30 slots
replaced. In the rerun Sweden's 2021 book holds 12 names of 17 screened, and that year's
return moved from -16.01% to -15.93%, which suggests the names that had stopped paying
returned about what the rest did that year. Germany, Switzerland and Sweden became too thin
to test (17, 20 and 15 cash years of 25): Germany through the listing rule, Switzerland
through the listing rule and the cash guard together, Sweden through the cash guard in
three years and the fix in one. South Africa is in cash in all 25 years.

**What the second wave's rerun showed (2026-10-13): the first six topics, 96 market
legs.** The published files of deleveraging, fcf-compounders, fcf-yield, revenue-accel,
dividend-sustainability and price-to-sales were written between March and August 2026. They
predate the entry-side cash guard (2026-09-03), the fix to the price oscillation filter
(2026-08-21, see that section below), the deterministic tie-breaks (2026-10-10), the listing
rules, the same-ISIN dedup and last-traded-price exits. So a published-to-rerun difference
mixes this wave's changes with earlier ones. The exit rule is the one change measured
alone, and it is small:

- Exit rule alone, on identical selection: 0.14 points of CAGR or less on 93 of the 96 legs
  (per-topic ranges under "Exit-side survivorship" below).
- Entry-side cash guard: 338 of the 2,023 cash periods in the new files are periods where 10
  or more names passed the screen and fewer than 10 could be bought (deleveraging 152,
  price-to-sales 105, revenue-accel 32, fcf-yield 30, dividend-sustainability 13,
  fcf-compounders 6). The published code stayed invested through periods like those.
- Listing rules: on XETRA, SIX and LSE legs only. deleveraging XETRA goes from 0 to 26 cash
  quarters of 103 (25 with fewer than 10 German names passing) and SIX from 0 to 33 (21).
- Not separable from the files: inside fully invested periods, the dedup, the price-filter
  fix, the tie-breaks and data revised since the published run. Where a market's move sits
  there, its cause is not determined.
- 37 of the 96 legs are invested in fewer than half their periods and are too thin to test:
  fcf-compounders 17 of 18 (every market but the US), fcf-yield 7, price-to-sales 7,
  deleveraging 2, revenue-accel 2, dividend-sustainability 2. A Norway leg is counted over its
  measured periods: fcf-compounders' is invested in 0 of 12 years and fcf-yield's in 2 of 12,
  while deleveraging's, invested in 26 of 50 quarters, is testable but not ranked.

**The other six topics, 93 market legs (2026-10-13).** The published files of low-debt,
dividend-growth, graham-number, pe-mean-revert, rising-yield and dogs-of-dow were written
between March and August 2026 as well, the oldest on 2026-03-09 (three graham-number
markets) and the newest on 2026-08-14 (pe-mean-revert). They predate the same list of
changes, and each also predates its own selection fix (entries above). Both vintages
cover the same 25 annual periods on every leg, so no difference below is a new period.

- Exit rule alone, on identical selection: 0.14 points of CAGR or less on 86 of the 93 legs,
  with 230 exits filled and none left unpriced (per-topic ranges under "Exit-side
  survivorship" below).
- Entry-side cash guard: 128 of the 755 cash years in the new files are years where enough
  names passed the screen and too few of them could be bought (rising-yield 46, low-debt
  38, dividend-growth 20, pe-mean-revert 17, graham-number 7, dogs-of-dow 0). The floor is
  10 names, and 5 in dogs-of-dow, where a line needs an entry price to take a blue-chip
  slot in the first place.
- Listing rules: German and Swiss legs lose years to the screen count. Cash years of 25,
  published to rerun, XETRA then SIX: dividend-growth 7 to 17 and 7 to 20, pe-mean-revert 0
  to 17 and 3 to 24, rising-yield 0 to 15 and 0 to 21, low-debt 0 to 6 and 1 to 8,
  graham-number 0 to 1 and 2 to 3. dogs-of-dow has no cash year on either leg in either
  file. Those counts include each topic's selection fix and the cash guard: in rising-yield
  the replay has 6 of XETRA's 15 added years and 6 of SIX's 21 appearing without the fix.
  On LSE cash years went from 0 to 5 in low-debt and in pe-mean-revert, from 0 to 8 in
  rising-yield (the replay credits all 8 to the selection fix, acting through the guard)
  and from 5 to 0 in graham-number, where no pence line could pass before.
- Not separable from the files: inside years invested both times, a topic's selection fix,
  the dedup, the price-filter fix, the tie-breaks and data revised since the published run.
  The published files hold no holdings and no per-year screen counts.
- 24 of the 93 legs are invested in fewer than half their years and are too thin to test:
  rising-yield 9 of 17 (Germany, Sweden, Switzerland, Australia, Korea, South Africa,
  Singapore, Brazil and Thailand, all nine newly), dividend-growth 7 of 14 (Germany,
  Switzerland and Sweden newly; Taiwan, China, Korea and South Africa already),
  pe-mean-revert 5 of 13 (Germany, Sweden and Switzerland newly; Taiwan and Thailand
  already), low-debt 2 of 15 (Singapore newly; South Africa already), graham-number 1 of 17
  (France, in cash in all 25 years in both files) and dogs-of-dow none of 17. Four of the
  24 have no invested year at all: South Africa in dividend-growth and in rising-yield,
  Singapore in rising-yield and France in graham-number. A too-thin leg's file still
  carries a CAGR, a Sharpe ratio and a drawdown. They mostly measure cash years and must
  not be read as results: pe-mean-revert Switzerland's 0.97% a year and 0.00% drawdown
  describe one invested year of 25.
- graham-number's Norway leg is window-truncated, not too thin. Its benchmark starts in 2014,
  so its metrics cover 11 of the 25 rebalances, and its record says so
  (`window_truncated: true`). It is invested in all 11 of those years (17 of the 25 in all).
  The leg isn't in the topic's comparison file.
- Benchmarks: the 25-year benchmark CAGR is the same to two decimals in both vintages on 92
  of the 93 legs. The exception is graham-number Singapore, whose benchmark changed from
  the S&P 500 to the STI. Single benchmark years did move (under "Not fixed yet").

**Third wave: defects found by the audits and fixed before the rerun (#16).** Each entry
sizes the defect on the old screen, as measured before any rerun, and then says what the
installed results show. As in the second wave, a figure credited to one fix comes from a
replay of the screen on the warehouse before the rerun, not from a backtest.

**52-week-high: one price basis, the last traded bar, ties by size (fixed 2026-10, #7 and
#16; rerun).** The published screen ranked `adjClose / MAX(high)`: a close back-adjusted for
every later dividend over a high that isn't, so a company scored lower the more it paid out
after the rebalance date. On the corrected ratio the share of the US book's names that are
dead today falls from 38.1% to 18.3%. The published run also took its signal bar from bars up
to 5 days after the rebalance date while entry was the next day, with no test of the bar's
age (fixed in #7), and cut ties at a ratio of exactly 1.0 in no fixed order. The ratio is now
the close over the 252-bar high on one split-adjusted basis, the signal bar is the latest
bar with volume above zero in the 10 days to the rebalance, and ties go to the larger market
cap, then the symbol. Sized before the rerun under the current wiring: the price basis changes
45.4% of 48,764 invested slots across the 18 legs, from 4.9% in Norway to 76.8% in the US. The
traded-bar rule changes 4.2% of holdings: 384 of 3,090 US slots (379 names leave, 324 of them
lines the warehouse flags as funds, which didn't trade and so sat on their high) and 494 of
3,090 in China (suspended lines). The tie rule fills 84 of 210 slots differently in China's 7
quarters with more than 30 names at 1.0. No run isolates the 5-day look-ahead. In the rerun
CAGR fell in 10 of 18 markets and rose in 8. The US went from 12.52% to 4.57% a year with no
cash quarter in either file, Japan from 8.43% to 2.56% and Germany from 8.23% to 5.01%, a tie
with the DAX. China's maximum drawdown, -92.34% in the published file, is -72.47% and runs
from October 2007 to January 2025: the book never regained its 2007 value. No market is too
thin to test.

**dcf-discount: latest filing first and a 50% cap (fixed 2026-10, #16; rerun).** The fetch
loaded only cash-flow rows with positive free cash flow in the market's currency, so a
company kept qualifying on its last positive year. The backtest also had no upper bound on
the FCF yield, while every SQL screen printed in the topic's posts caps it at 50%: in a replica
of the US screen 529 of 1,250 held slot-years were above it. #7 removed its `isActivelyTrading`
filter. In the replica over the 16 legs, 3,869 of 13,743 held slot-years (28.2%) leave the book,
1,634 (11.9%) through the latest-filing test alone, from 6.3% of a leg's slot-years in
Australia to 54.8% in China (46.3% in the US, most of it the cap). The replica's cash-year
counts match the rerun's on all 16 legs. Switzerland went from 3 cash years of 25 to 17 and
is too thin to test: by the replica 5 come from the campaign wiring, 12 once the latest filing
is tested and 17 with the cap, all through the cash guard on a universe of 245 Swiss lines,
where 13 of the 14 newly cash years pass 10 to 18 names and only 6 to 9 of them have an entry
price. In the replica every unbuyable Swiss name had no price bar in the entry window. The US
went from 7.91% to 7.44%, now a tie with the S&P 500 (+0.12 points). Brazil fell from 18.05% to
10.25% with four new cash years; its published file is the March 2026 run. The currency test
isn't a unit fix: `key_metrics.marketCap` is in the reporting currency too, so the test only
keeps the universe to companies that file in the market's currency.

**defensive-quality: the campaign rules (#16; rerun).** No selection defect of its own was
fixed in the third wave; #7 removed its `isActivelyTrading` filter and added an 18-month
limit on a filing's age. The published files are the April 2026 runs (China SHZ+SHH: March)
and predate the price-oscillation filter itself (9 April 2026). In the rerun 42 market-years
went from invested to cash, 31 on the cash guard and 11 on the screen count, all 11 on the UK
and German legs under the listing rule. Switzerland (15 cash years of 25) and Malaysia (14)
became too thin to test. The UK fell from 11.19% to 5.73% with 7 new cash years. The China
SHZ+SHH file was measured against the S&P 500 when published and is measured against the SSE
Composite now (5.28% against 2.43%).

**The pairs topics: a per-year universe and the defects behind it (fixed 2026-10, #16;
rerun).** pairs-fundamentals, pairs-zscore and pairs-multi-pair took their candidates from one
list: today's actively trading names, the 30 largest of each sector by today's market cap,
used for every year from 2005. In the design review's recomputation of that list, 98 of the
330 US names weren't listed in the 2005 formation year and 94 more were listed but outside
the new list; on LSE 295 of 330 weren't listed in 2005, most of them foreign lines with no
bars. Same-company pairs crowded the books: in the old list the top 20 pairs by 2019
correlation were two lines of one company in 20 of 20 cases on SET (NVDR twins at a
correlation of 1.0000), 14 on KSC and 10 on STO, and over the two sampled formation years 92
of 414 top-20 rows were same-company pairs, of which the same-ISIN dedup catches 1. The same
review found, and #16 fixed: a pair with an undefined (NaN) correlation passed the 0.70 filter
and sorted first, so all 20 XETRA slots in formations 2009 and 2019 went to pairs with a
frozen leg; each round trip was charged twice the cost its return implies; the cost tier
used today's cap in the reporting currency, so Japanese and Korean legs always paid the 0.1%
tier (on the 2015 candidates in US dollars the average one-way rate is 0.31% in Japan and
0.42% in Korea, against 0.10% for both); pairs-multi-pair exited on the bar that gave the
signal; the cash rule read year-end outcomes; pairs-fundamentals took its entry z-score from
the bar it traded on; and the rolling z-score could signal on a thin window. Each year's
candidates are now rebuilt (METHODOLOGY.md, Pairs Universe), one line per company, a NaN
correlation can't rank, costs are charged once in US-dollar tiers, and cash is decided on
pairs entered before the year trades. The results now record each year's candidate count,
the pairs formed and the number entered. In the rerun every market of the three topics trails
its benchmark, as in the published files, except that pairs-zscore South Africa goes from
27.94% (four years above +100% for a market-neutral book) to -0.55%. Which change drives a
market's move isn't determined: the gate ran only the new code, and the old files record no
pairs.

**What the third wave's rerun showed (2026-10-14): six topics, 90 market legs.** The
published files were written between March and June 2026 (52-week-high in June, dcf-discount
in May with Australia and Brazil in March, defensive-quality in April, the pairs topics in
March). Each topic's README and its old-against-new notes give the market-by-market detail.

- Exit rule alone, on identical selection: 0.14 points of CAGR or less on 63 of the 66 legs
  that have a drop book, with 245 exits filled and 8 left unpriced, all 8 in 52-week-high
  (per-topic ranges under "Exit-side survivorship" below). pairs-zscore and pairs-multi-pair
  have no drop book.
- Entry-side cash guard: 73 of the 431 cash periods in the 52-week-high, dcf-discount and
  defensive-quality files (14 of 184, 26 of 81 and 33 of 166). The pairs topics decide cash on
  pairs entered (fewer than 3), not on this guard.
- Listing rules: dcf-discount's German leg went from 1 cash year to 4 and its Swiss leg from
  3 to 17 (mostly the selection fix, above); defensive-quality's German leg from 0 to 10, its
  Swiss leg from 5 to 15 and its UK leg from 0 to 7; 52-week-high's three legs have no cash
  quarter in either file except Switzerland, which fell from 3 to 1, although the listing rule
  only removes names (cause not determined). In the pairs topics the German rule leaves 163 to
  243 candidates a year. The UK leg, whose old fixed list was mostly foreign lines with no bars,
  now forms 10 to 20 pairs a year in pairs-fundamentals, and its cash years fell from 13 to 2.
- Not separable from the files: inside years invested both times, a topic's selection
  change, the dedup, the price-filter fix, the tie-breaks and data revised since the published
  run.
- 10 of the 90 legs are invested in fewer than half their periods and are too thin to test:
  dcf-discount Switzerland; defensive-quality Taiwan, Switzerland, Sweden, Malaysia and South
  Africa; pairs-fundamentals South Africa, Thailand, Switzerland and Singapore. dcf-discount
  Switzerland, defensive-quality Switzerland and Malaysia and pairs-fundamentals South Africa
  became so in the rerun; pairs-fundamentals UK, Taiwan and Germany were too thin in the
  published files and are testable now. defensive-quality Sweden's Sharpe ratio of -14.357
  comes from one invested year and isn't a result.
- Benchmarks: every 52-week-high benchmark figure is identical in both vintages. dcf-discount's
  25-year benchmark CAGRs are unchanged on all 16 legs, but single years moved in 11 (SPY's
  2000 reads -23.68% against -26.23%), the pattern already listed under "Not fixed yet". Three
  pairs-fundamentals legs (Switzerland, Thailand, Singapore) and its Sweden leg were measured
  against the S&P 500 when published and against their own index now.

**The 13 Norway legs (2026-10-14).** The Norway leg of earnings-consistency, etf-rebalancing,
etf-underowned, ev-ebitda, ev-ebitda-relative, garp, oversold-quality, pe-compression,
peg-ratio, price-momentum, price-to-book, relative-strength and value-momentum was rerun with
`--exit-method drop`, so it matches the topic's other markets, which aren't rerun. It ran on
each topic's own code: no listing keys, no same-ISIN dedup and no `exit_method` key. The
repair is to the period accounting (under "Re-run population" below): 8 of the 13 published
records had a negative `invested_periods`, and every new one counts cash and invested periods
over all rebalances, carries `total_rebalances` and labels its 2013-2025 or 2014-2025 window.
Returns are unchanged in 7 and changed in 6, the largest garp's (9.24% to 3.56% a year: 12
measured quarters are now guard cash). Counted over their measured periods, 6 of the 13 are
too thin to test (earnings-consistency, etf-underowned, ev-ebitda-relative, garp,
oversold-quality, pe-compression). The other 7 are testable on their truncated windows and
aren't ranked with full-window markets, among them ev-ebitda (7 of 11 years invested),
relative-strength (38 of 50 quarters) and value-momentum (18 of 24 half-years).
etf-underowned's published comparison node was an older 20-period run and
is now measured over 12 of 20 rebalances, so it joined the truncated set.

Not fixed yet:
- `7386.T` (an LTP fill in cyclical-timing and fcf-conversion's JPX results, held in
  fcf-conversion's 2024 book) has price levels around 4 billion in `stock_eod`. Entry and exit
  are on the same scale, so its period return is sane, but the levels are corrupt upstream.
  `4298.T`, an LTP fill in low-debt's JPX results, has levels of the same kind: bought in
  July 2024 at about 19.3 billion and filled in June 2025 at about 28.5 billion.
- `graham-net-net/screen.py`, the live screen, still compares `adjClose` with
  `grahamNetNetTTM` across currencies.
- Each second-wave topic's live `screen.py` has the pre-fix shape. None of the 12 has
  changed since April 2026, so none carries the listing rules, the dedup or its topic's
  selection fix. Three of them don't start at all: `deleveraging/screen.py`,
  `revenue-accel/screen.py` and `low-debt/screen.py` each stop on a `SyntaxError` in an
  f-string before running anything.
- The size floor compares a reporting-currency market cap with a listing-currency threshold
  in every rerun topic, not only graham-net-net: the screens test the latest FY
  `key_metrics.marketCap` against a per-exchange figure in the exchange's currency
  (`get_mktcap_threshold`, or graham-net-net's own table). The non-US
  legs of dogs-of-dow are the one exception since #13. Measured on revenue-accel's screen
  before the rerun (25 rebalances): 1,133 of 8,031 top-30 slots (14%) carried a cap in a
  different currency from the floor (HKSE 57%, SES 34%, LSE 27%, TSX 25%, SIX 19%, US 18%,
  STO 7%). Restating the cap changes membership both ways: 25 of 750 US slots and 20 of 725
  LSE slots went to names under the floor, while 35 STO and 29 HKSE slots belonged to names
  the raw figure wrongly removed.
- Lines of one company that carry their own ISINs are not collapsed by the same-ISIN rule
  and can hold separate slots: Thai `-R` (NVDR) lines, preference and second share-class
  lines, and mutual-fund share classes. In the rerun books: price-to-sales SET's two
  invested quarters hold 10 lines of 7 companies (ROJNA, AP and CRC each beside their `-R`
  line); dividend-sustainability STO 2009 met its 10-name floor with 8 companies (SSAB-A
  with SSAB-B, ATCO-A with ATCO-B); dividend-sustainability TSX 2016 held GWO.TO with three
  of its preference lines; price-to-sales US has mutual-fund share classes (PRGTX, JTSSX
  and others) among its ten recorded names in 38 of 103 quarters. In the six topics rerun
  last, counted by ticker root, which is a lower bound and not an identity check:
  dogs-of-dow SET fills 81 of its 210 slots with a second line of a company already in the
  book (92 of the 210 are `-R` lines), dogs-of-dow Canada holds preferred-share lines
  in 96 of 249 slots, in all 25 years, and dogs-of-dow UK holds four preference lines
  (GACA.L, GACB.L, BP-A.L and BP-B.L) in 52 of 250 slots, in every year from 2005 and 4 of
  the 10 in six years; dividend-growth TSX holds CCL-A.TO with CCL-B.TO in 17 of 20
  invested years; rising-yield TSX holds two or more lines on one root in all 18 invested
  years (2019: 20 lines on 11 roots); pe-mean-revert STO has two lines on one root among
  its ten recorded names in all 9 invested years. The dogs-of-dow UK leg is that topic's
  largest move (+5.61 points), and its drawdown and volatility describe a book that is
  partly fixed-coupon preference shares. Book sizes in the results count lines, not
  companies.
- graham-number tends to rank such lines first. A preference line, a fund share class or a
  second share class that carries the company's Graham number against its own, lower price
  ranks below the ordinary share on price over Graham number and can rank first in its
  market. Among the ten cheapest names each results file records per year: tickers in the
  mutual-fund format (five letters ending in X) in the US in every year from 2014 to 2024,
  5 to 7 of the ten and 63 of 250 recorded slots; preferred-share series in Canada in 18 of
  25 years, up to 9 of the ten and 92 of 250 slots; Aviva and BP preference lines in the UK
  in 19 of 25 years, 44 of 250 slots; preferred-share codes in Korea in all 19 invested
  years, 123 of 190 slots; 0087.HK (Swire Pacific B) in Hong Kong in 24 of 25 years; and
  AMC.AX in Australia in 21 of 25 years, first in 9 of them, which the fix review traced to
  a per-share basis mismatch. The lines were recognised by ticker pattern and weren't
  checked against `profile`. The files record the ten cheapest of a
  year's names (the US book averages 29.2) and no name's return, so what these lines add to
  or take from a CAGR isn't determined. The topic has no fund-exclusion flag, and no run
  has left them out.
- The 45-day lag counts from the fiscal period end, not from a filing date, in every rerun
  topic. The screens read statements by `dateEpoch` (the period end), and no backtest or
  screen in this repository reads the date a financial statement was filed or accepted, so
  a filing can be used before it was public.
  METHODOLOGY.md said the screens used a filing date until 2026-10-13.
- The entry-price floor of 1.0 is in local quote units, so it's a different test in every
  market: it almost never binds on lines quoted in pence, South African cents, yen or won,
  and it's a real price filter in Singapore and Hong Kong dollars. Measured on
  revenue-accel's screen before the rerun, 118 of 331 screened SES slots (36%) and 58 of
  694 HKSE slots were under 1.0 at entry. Together with the entry-side cash guard that
  sends Singapore to cash: revenue-accel SES is in cash in 19 of 25 years (13 of them guard
  years) and deleveraging SES in 74 of 103 quarters (43 guard quarters). How many of those
  prices are real and how many come from back-adjustment wasn't measured.
- price-to-sales's published US figure keeps its mutual-fund share classes. One run without
  funds and ETFs, made on 2026-10-14 and not gated, gives 13.72% a year against 14.41%, still
  5.71 points ahead of the S&P 500 (see "Closed-end funds" below). The other markets weren't
  run that way.
- dogs-of-dow's US leg ranks today's Dow 30 members in every year. Its non-US legs lose
  some large companies that have no price history before 2026, and their blue-chip slots
  go to smaller names (#13). In the rerun 22 different tickers fill the US leg's 250 slots,
  and on JNB NPN.JO and MTN.JO fill none. The non-US books aren't always what "blue chips"
  suggests: Canada's slots are 39% preferred-share lines (above), and Hong Kong's book
  holds lines numbered 43xx.HK in 10 slots (HKSE has no listing rule). 7 of them are
  4335.HK and 4338.HK, which the size-rule audit identified as Intel's and Microsoft's Hong
  Kong lines; the other 3 are 4333.HK, which it didn't identify. dividend-growth's Hong Kong
  book holds 4333.HK, 4335.HK and 4338.HK in 7, 9 and 11 of its 16 invested years; the
  same numbering, not checked against `profile`.
- The 200% return cap removes a name from a year's average without replacing it, so a book
  that cleared the floor at entry can average fewer names than the floor. In dogs-of-dow's
  rerun it removed 25 names in 12 market-years: South Africa's +59.16% for 2005 is the
  average of 4 names, under the floor of 5, and Shenzhen's +139.50% for 2007 the average of
  6 of 10.
- cyclical-timing's expansion signal counts same-ISIN twins twice (the signal, not the
  holdings).
- A reused ticker can carry its current holder's ISIN across the previous company's
  whole price history (OSG bars are Overseas Shipholding but carry Ambac's ISIN; ACH bars
  are the Aluminum Corp of China ADR but carry Owens & Minor's). If both lines pass the
  same screen on the same date, the ISIN rule treats two companies as one.
- The reverse also happens: a renamed line can be held twice when its old and new tickers
  don't share an ISIN. graham-number's Brazil book has AXIA6.SA with ELET6.SA among its ten
  recorded names in 6 years, and MBRF3.SA with MRFG3.SA in 2024. Each pair looks like the
  old and the new ticker of one line (Eletrobras, Marfrig); that wasn't checked against
  the warehouse.
- A line whose price rows keep printing one price can be bought and then exits at that
  price. pe-mean-revert has three last-traded-price fills at exactly the entry price:
  GBGLOBAL.NS (bought January 2023 at 930.00, last bar 2023-05-23), CO (2.99, last bar and
  delisting 2023-06-09) and EVR.L (81.00, last bar 2023-08-11). EVR.L's delisting record is
  dated 2022-03-10, more than 30 days before the entry, so the fill ignores it, and the
  same fill is in graham-number's UK book. An unchanged price suggests a suspended or stale
  line. That wasn't checked against a source.
- A company's filings can start before its price rows do. A screen that picks on the
  latest filing surfaces such names, and the cash guard then sends the year to cash. In
  the replay of rising-yield's fixed LSE screen 342 of 604 top-30 slots were buyable, 159
  had no `stock_eod` row at all and 99 had a first bar after the entry window; all 8 of
  that leg's cash years are guard years. In its installed LSE file 304 of the 484 names
  screened in its 17 invested years (63%) had an entry price, so its book averages 17.9
  lines although 10 or more pence-quoted names pass the screen in all 25 years and the full
  top 30 in 12 of them. In pe-mean-revert 9 of 30 screened names had an entry price in
  China at the January 2009 rebalance, one short of the floor of 10, so the year is cash
  where the published file reported +100.79%. In Hong Kong 10 of 30 had one, and the year
  is invested at the floor (+62.62%). Why the rest had no price in the entry window wasn't
  checked.
- Single benchmark years differ between the published and the rerun files on four or five
  indices per topic, in offsetting neighbours that leave the 25-year CAGR the same: the
  Sensex, Nikkei 225, SSE Composite, KOSPI and OMX Stockholm 30 in the three topics that
  rebalance in July, the Sensex, Hang Seng, SSE Composite, KOSPI and SET Index in
  dogs-of-dow and pe-mean-revert, and the Sensex, Bovespa, KOSPI and Hang Seng in
  graham-number. The Sensex's 2000 reads -29.29% in low-debt's new file and -31.63% in the
  published one. That is the pattern of a different index bar being picked at a rebalance
  date. Whether the cause is the price-filter fix (the index passes through the same
  filter), the wider price window, the UTC date bounds or a revised row isn't determined.
- A leg with no local index in the price data is measured against the S&P 500 in USD, a
  local-currency return against a dollar index. In the six topics rerun last that is
  South Africa (JNB) in the five that run it, Shenzhen and Saudi Arabia in dogs-of-dow,
  and France in graham-number. Their excess figures aren't verdicts on the screen.

Not fixed yet, found in the third wave:
- Pairs universe: a delisted leg's zero-volume repeat bars can keep it in the formation-year
  universe until about 30 days before the year end (rare, #16).
- pairs-fundamentals counts a pair as entered on its entry prices alone, while pairs-zscore and
  pairs-multi-pair also require each leg to be inside its delisting bound.
- 52-week-high: an untraded same-day duplicate bar can still take one of the 252 rows of the
  high's window. The window orders a day's rows by close, then high (both lowest first), then
  volume (highest first), so a duplicate with a lower close than the traded bar, or the same
  close and a lower high, sorts before it and its high counts. It can't be the signal bar,
  which needs volume above zero.
- The pairs company key (ISIN, CIK, normalised name, ticker root) misses lines of one company,
  or one price series, under different names and ISINs, so `check_pairs_results.py` passes
  them. Named from the tickers and formation-year correlations, not from a data field:
  6501.T/9086.T in Japan (Hitachi against Hitachi Transport System's code, correlated at 0.996
  or more in 14 years: a price-data fault; formed in 16 to 18 years in each topic, and the top
  pair of pairs-multi-pair's Japan book in 17 of 20), 5844.T/8369.T (Kyoto Financial Group and
  Bank of Kyoto), SKG.L/SWR.L (Smurfit Kappa and Smurfit Westrock, the UK's top pair in 10
  years of pairs-multi-pair), BBL/BHP (BHP's two US listings), TATAMTRDVR.NS/TMPV.NS (Tata
  Motors' DVR and ordinary lines, 10 years), BEP-UN.TO/BEPC.TO (Brookfield Renewable units and
  exchangeable shares), and in pairs-fundamentals IAC/MTCH, DINO/HFC, CPG.TO/VRN.TO,
  ATRL.TO/SNC.TO, MATR.TO/SCL.TO and COPP-B.ST/VISC.ST, among others. A pair of one price series
  earns about nothing before costs and pays the cost on every trade, and inverse-volatility
  weighting gives it a large weight (an estimate: per-pair weights aren't stored). Japan's
  inverse-volatility CAGR in pairs-multi-pair trails its equal-weight CAGR by 0.67 to 0.72
  points at every book size, against 0.03 to 0.25 in the US. Whether to extend the key and
  rerun is an open decision.
- The pairs topics' Taiwan leg runs TAI and TWO, but no formed pair holds a TWO line in any
  year (0 of 296 formed pairs in pairs-fundamentals, 0 of 115 lines in pairs-multi-pair).
- The pairs universe has no size floor beyond the top 30 of a sector: the smallest admitted
  median traded value is 0.26 USD a day in the UK in 2005 and 2.04 USD in 2024, and 216 USD in
  Canada in 2020. A candidate with no fiscal-year market cap whose year ended 45 to 540 days
  before the trading year pays the 0.5% cost tier, which covers most early candidates in Korea (234 of 243 in 2005), Taiwan (219 of 225),
  Japan (174 of 311) and India (177 of 259).
- A last-traded-price fill can be a suspension's last price, not a delisting. China Vanke
  (000002.SZ) has its last bar on 2015-12-18, when its A shares were suspended (public record,
  not checked against the warehouse), and trades again later. It is filled at that bar, at
  17.12, in dcf-discount's China book (entered at 9.21), in pairs-fundamentals (entered at
  10.11) and in pairs-multi-pair, as it was in dogs-of-dow's Shenzhen leg (above). In
  pairs-fundamentals it is the whole +0.28-point exit-rule effect in China and the reason that
  leg's CAGR changes sign (-0.16% on the drop book, 0.12% on the ltp book).
- pairs-zscore and pairs-multi-pair South Africa: distinct companies correlate at 0.97 to 0.99
  in the recorded pairs (Standard Bank and Sanlam, SBK/SLM, at 0.9927 in 2005), the bad-bar
  pattern the design review found; the leg is invested in 11 of 20 years on the thinnest book of either topic and is measured
  against the S&P 500 in USD. The posts set the leg aside as unreliable data and don't rank
  it, and both topics' chart generators leave it out of the comparison charts with the footer
  "South Africa not ranked: unreliable price data"; at 11 of 20 years it isn't too thin, so
  the reasons are the data and the dollar benchmark against a book in rand.
- 52-week-high's entry floor reads `adjClose`, back-adjusted for later dividends: in
  Singapore's invested quarters it drops 1,053 of 2,620 screened names (40.2%), and the slots
  aren't refilled, so the book averages 17.6 lines. It decides 14 cash quarters (Italy 4,
  Malaysia 1, Singapore 9). Its effect on the result isn't measured.
- defensive-quality's US book holds FEMSA's ADR (FMX) in 24 of 25 years, although its vendor
  dividend yield is mis-scaled (a median of 26.5% in the pre-rerun audit against a US cut-off
  of 3% to 5%). No yield cap was added.
- Lines that aren't ordinary shares are a material part of several third-wave books (5% or
  more of the slots counted), recognised by ticker pattern and a hand list, not checked against
  `profile`. 52-week-high, over the ten names recorded per quarter: Thailand 27.4% (NVDR lines
  216 of 860, funds 20), Korea 20.7% (preferred lines), the UK 20.4% (investment trusts 182 of 1,030,
  preference shares 28), Canada 19.4% (preferred lines 185), the US 9.1% (closed-end funds,
  notes, preferred lines and warrants) and Germany 8.9% (preference shares). dcf-discount, over
  the ten names listed per year: Brazil 47.4% (preferred lines and units), Thailand 36.5%,
  Canada 31.2%, Korea 22.6%, the UK 10.5% and Germany 8.6%. defensive-quality, over every held
  line: Thailand 40.7% (NVDR lines, most beside the same company's ordinary line), Korea 26.1%,
  Germany 14.9%, the US 12.4% (utility notes, preferred ADRs and units, 10 to 12 of 24 to 28
  lines a year from 2020), Switzerland 9.3% (Roche's non-voting certificate beside its bearer
  share in every invested year, on a leg too thin to test) and Canada 7.5%. pairs-fundamentals'
  German formed pairs hold
  Porsche SE's preference line (PAH3.DE, its only listed line) in 23 of 460 slots (5.0%;
  5.2% with VVV3.DE). REIT and trust units are listed in each topic's notes and not counted.
- The 13 Norway legs ran without same-ISIN dedup. etf-rebalancing holds SRBNK.OL with
  SB1NO.OL in 6 of its 13 invested years, very likely one bank under its old and new ticker
  (ISIN not checked), and Schibsted's A and B shares, which have separate ISINs, sit together
  in peg-ratio's book in 39 of 50 measured quarters and in etf-rebalancing's in 2 years.
- The comparison charts of price-momentum, relative-strength and value-momentum never read
  `invested_periods` and read `window_truncated` only from 2026-10-15, so until then they drew
  Norway's 2013-2025 bar among 2000-2025 siblings as an ordinary, ranked bar, although a leg
  measured over a shorter window isn't ranked with full-window ones (all three are testable
  over their measured periods). The three scripts now leave Norway out and name it in a
  footnote ("Not ranked, benchmark covers only part of the window"). Their `charts/` folders
  are gitignored, so no image is committed, and the live momentum-07 and factor-03 comparison
  images were drawn before either fix and keep the Norway bar until those posts are
  republished. The scripts test no `invested_periods` gate, so gate 12 of
  `scripts/verify_floor_guard.py` doesn't cover them. peg-ratio's charts leave out its Norway
  leg, which is testable (+2.62 points), because their filter drops every truncated window.
  Leaving it unranked is the rule, but no footnote names it.
- The live screens don't match the reruns: `52-week-high/screen.py` still ranks
  `adjClose / MAX(high)` with no traded-bar rule, listing rule or dedup; `dcf-discount/screen.py`
  has no currency, fund, cap, listing or dedup rule; the pairs screens rank today's actively
  trading names.
- dcf-discount's Brazil and Australia legs are retraction pages and still read the price
  table whose split adjustments those pages blame, so whether their new figures are clean
  isn't determined.

**Same security, two tickers.** One ISIN keeps one line, before the top-N cut, so a
collapsed twin frees its slot for the next name. Per ISIN, the line buyable at entry
wins, then the one `profile` still marks as actively trading (a retired ticker's bars
stop at the rename), then the one quoted in the market's own currency (TSX HOM-UN.TO
over HOM-U.TO, HKSE 9988.HK over 89988.HK), then the alphabetically first symbol.
Pairs held twice before:
DHL.DE/DPW.DE, BBVA.DE/BOY.DE, DUFN.SW/AVOL.SW, 888/EVOK, BTRW/BDEV, and BC94/SMSN
(Samsung, 10 graham-net-net periods). Share classes have their own ISINs and stay.

Caveats:

- All three rules read today's `profile` (country, quote currency, ISIN), not its
  history, so a company that moved its domicile or listing is classed as it is now.
- The ISIN rule misses legacy lines FMP gives an old ISIN: Rotork ROR/76ID and
  Antofagasta ANTO/70GD.
- In cyclical-timing the XETRA, SIX and LSE rules also apply to the universe its
  expansion signal is computed on, so they can change which years are invested.
- Cash periods can move either way. fcf-yield XETRA went from 5 to 20 cash periods of 25
  under a domicile filter, while yield-gap became more investable because unpriced
  foreign lines had been taking top-30 slots (both above). Only the rerun shows which.
  The second wave shows both: fcf-yield XETRA is at 22 cash years of 25 after its rerun,
  while deleveraging LSE has no cash quarter in either file and its average book grew from
  13.2 to 20.8 lines (the price-filter fix can fill that book too, and the split between
  the two isn't determined). In the six topics rerun last, dogs-of-dow's XETRA, SIX and LSE
  legs have no cash year in either file and their average books grew from 6.1, 3.6 and 5.0
  lines to 10.0, 9.9 and 10.0 (with the topic's size rule), and graham-number LSE went from
  5 cash years to none (with its unit fix).
- Stockholm is left as listed. Its books are almost entirely domestic, and a country
  filter would wrongly drop ABB, AstraZeneca and Nokia.

**Topics outside the three waves have not been changed.** Their XETRA, SIX and LSE legs
still hold every listed line; read them with the measurements earlier in this section. The
13 topics whose Norway leg was rerun in the third wave are among them: only that leg changed.

---

## Data checks in place of the return cap (deleveraging and cyclical-timing, 2026-10)

**Status:** code in place for deleveraging and cyclical-timing (METHODOLOGY.md, Data Checks); their
committed results predate it until both topics are rerun. The other topics keep their existing cap
(200%, 300% in three; qarp, low-pe and sector-correlation have none) until their own rerun.

The 200% single-return cap drops a holding only when its return is high. Reading every holding
it dropped from these two topics' published books, and the holdings beside them, found real
moves the cap cut and errors it could not see, because the errors also produce large losses and
small gains. The error classes, with the cases that defined them:

**1. Holiday row sets.** The warehouse carries rows on dates an exchange was closed, sometimes
with prices from another regime and volume 0, sometimes with copied volume.
- JNB 2012-01-02: 8 of 100 lines traded, against 92 to 97 on the dates around it (2011-12-27:
  6 of 105). KIO.JO's row that day reads adjClose 49,422.39 at volume 0 against 14,826.95 the
  next day, so the JNB 2011 Q4 book exited KIO at +299.5% and the cap dropped it. On its first
  traded bar the exit is +19.9%. The same date gave RLO.JO +148.9% and SBK.JO +124.0%, which the
  cap kept. ABG.JO's row that day shows volume 406,612 and is a holiday row too.
- SET 2013-01-01: 24 lines traded, all on copied volume.
- NSE 2019-10-02: 393 of 403 rows traded against 1,562 to 1,582 on normal days, a holiday set
  with copied volume and copied closes.
- Open days that look thin: Saudi Sundays (2020-07-05 and 07-12) had 120 traded lines against
  185 to 186, and SET 2013-01-02 had 333 of 347 ordinary lines traded and 0 of 36 NVDR lines.
  One test separates all of these: a date's traded-line count against the median of the other
  dates in the same price window, with half as the line.

**2. Zero-volume stale bars.** A line can print a stale price at the wrong scale on a day it
did not trade. EASTW-R.BK opened the SET 2013 Q1 window with 2.74 and 2.73 at volume 0 (close
3.7) before trading at 8.25 (close 13.0) on 01-03: the book entered at 2.73 and the cap dropped
its +250.9%. On the first traded bar the return is about +16%. The same bars closed SET 2012 Q4
for the NVDR lines held then: EASTW-R.BK -67.4%, KBANK-R.BK -52.0% and MAJOR-R.BK -55.7%, all
kept. RAM.BK's SET 2013 Q3 window mixed zero-volume rows at adjusted factor 0.1822 with traded
rows at 0.8332, and the oscillation filter then deleted good rows; the cap dropped its +369.7%.
KBANK-R.BK's stale 2013-01-02 row has a wrong close as well (69.5 against 193.5 the next day),
so no test on the adjustment factor can see it; only the traded-bar rule does.

**3. NVDR twins and NVDR feeds.** Thai non-voting depositary receipts (X-R.BK) carry their own
ISINs, so the same-ISIN dedup kept both lines of one company: 34% of deleveraging's SET
positions were NVDR lines, often beside their ordinary line. 125 of 128 NVDR lines share the
first six ISIN characters with their ordinary line and have R as the tenth (BFIT-R, SF-R and
VNT-R do not). Old NVDR feeds publish real prices with no volume: ADVANC-R.BK had volume 0 on
every row from 2003 to 2007 while its prices matched ADVANC.BK bar for bar, KBANK-R.BK had no
traded row from 2004 to 2006 and TRUE-R.BK none from 2010 to 2012. Some ordinary lines do the
same (TRUE.BK in 2006: 259 rows, 3 with volume, prices moving daily). A rule that dropped every
zero-volume row would make these lines unbuyable for whole years, so a line keeps its
zero-volume rows in a window where it never traded and its close changes on two days or more
(items 6 and 7 cover the rest). BBL.BK has no rows at all in the 2012 Q4,
2013 Q1 and 2013 Q2 windows, so BBL-R.BK is the only line to hold then.

**4. Adjustment-factor regimes.** adjClose / close should change only on a dividend or split
that the close mirrors.
- TSG.JO, 2011-11-23: adjClose fell from 1,392 to 893 while the close rose from 1,401 to 1,610
  (factor 0.994 to 0.555). The JNB 2011 Q4 book held it at -39.6%; close to close it gained 7.0%.
- 3003.SR, 2020-10-26: a zero-volume halt row moved the factor by 2.112 while the close ratio
  over the step was 0.836; dividend_calendar dates a 13.51 SAR "dividend" on 10-28. The
  cyclical-timing Saudi 2020 book's +205.3% for it was dropped by the cap; close to close it
  gained 36.2%.
- SALEE.BK, 2015-05-20: adjClose and close both fell 4.17 times, with a 4:1 split in the
  calendar on 05-21. The factor did not move, so only the calendar shows the split was missed.
- 2603.TW, 2023-06-30: factor 0.453 to 0.825 with the close 155.0 to 93.5, the real NT$70
  dividend. A real step, which a flag must not be mistaken for an error.
- JNB's split factor oscillation in 2004-2005 (noted under JNB above) is the same class.
Check F records these steps on every held name (drift outside 1/1.05 to 1.15, or a split of
ratio 2 or more in the calendar during the holding), and every holding on the large-move list,
and changes nothing. A repair that divides
out the steps the close does not mirror is possible and is not in this version.

**5. Market caps inflated by later reverse splits.** `key_metrics.marketCap` can be built on a
price the warehouse later scaled up for reverse splits.
- SOS FY2019: weighted shares 593,233 times EPS 124 over net income 1.47M gives 50.0, and the
  split-adjusted close is 750 times the raw close at the period end. The 1.64B cap is about
  32.8M. It passed deleveraging's US floor in 2021 Q1, and the cap dropped its +231.9%.
- ZEST FY2019: price factor 150 and a median daily turnover of 0.002% of the cap. It passed
  cyclical-timing's US floor in 2019, and the cap dropped its +354.3%.
- HAO FY2024: a 200.9B cap with price factor 25, turnover 0.0016% of the cap, and shares x EPS
  10 times net income. Held in deleveraging US 2025 Q2.
- Also price factor 3 or more and turnover under 0.01% of the cap: CHRA FY2018 (22.5B), KDNY
  (1,040B), SIFY (35B), GRVY FY2008 (17.5B), TRC.BK FY2011 (76.5B THB).
- Large caps trade 0.04% to 0.7% of their cap a day across markets (Sinopec 600028.SS 3.9e-4,
  AAPL 7.3e-3, Samsung 1.7e-3, Toyota 1.6e-3, Tencent 1.6e-3, PTT 1.5e-3), so the 0.01% line
  holds across markets.
- Not caught: inflated caps on lines that never reverse-split later (APTO, GLMD, ALBT, NAAS).
  Where both price series are broken the check can exclude a sound row (RNAVAL.NS FY2011, close
  21,684 against a raw 0.287).
Check M runs at every market-cap floor. Without it, removing the cap would have booked SOS and
ZEST at their full returns.

**6. Found by the first review of the checks (same-day runs of all 32 legs, 2026-10-30).** Eight
cases changed the rules: five errors the checks introduced (7211.T, the thin share classes, the
deflatable caps, 4541.T, ELUX-A.ST), one gap that removing the cap exposed (Bonjour's netted
splits), and two that every arm already had (the flat lines with no volume, and CETX, TOPS and
PIXY with no turnover). Item 7 corrects three of the fixes.
- Turnover read on an inflated cap. 7211.T FY2003: FMP's 4.06T yen is 1.488bn unrestated shares
  times a close carrying the 2013 1:10 (k 10.03). Turnover was 7.77e-5 of that cap, so the row
  was ineligible, though it is 7.80e-4 of the 405bn cap after k. k is now applied first when it
  is within 1.5 times the price factor. HAO FY2024 (k 10 against a price factor of 25) keeps the
  earlier order and stays out.
- A thin share class on a real cap. BBD-A.TO, ERIC-A.ST and 02826K.KS carry their company's cap
  and trade thinly beside a liquid sibling (BBD-B, ERIC-B, 028260.KS, which listed in December
  2014 and has 9 traded bars at FY2014's end). The classes of a company carry the same FY cap in
  key_metrics, to the cent: ELUX-A and B, ERIC-A and B, 005930 and 005935, 028260 and 02826K in
  all 93 class-years read, and BBD-A and B. Among 1,896 pairs of same-market lines with an
  identical FY cap at the same period end, the pairs read were share classes, renamed or reused
  tickers, and preference lines, trust preferreds and warrants beside their common. Turnover is
  now summed over the lines that share a cap (since item 7, in two FYs or more).
- A deflatable cap excluded. 0653.HK FY2011 to FY2013 trade 2.9e-5 to 6.9e-5 of caps that are
  16.5 times their as-traded size (raw 1.31, 1.07, 1.86 times 3.455bn, 3.441bn, 3.537bn shares:
  4.53bn, 3.68bn, 6.58bn, over the HK$2bn floor); 1124.HK FY2008 is 30.9bn on a price factor of
  9.0 (3.43bn). A failing cap is now lowered by the price factor (and the later forward splits)
  and kept when turnover clears on the lower cap. A row that fails again stays ineligible:
  8038.T trades 3.9e-9 of a 1.8e16 yen cap, which a factor of 10 cannot repair. 1192.HK FY2016
  comes back at 2.98bn too (FMP's Titan share counts are inconsistent; read it on the rerun).
- Forward and reverse splits netting out. Bonjour's shares are restated 12 times for the 2010
  6:1 and 2:1, and its prices carry the 2024 1:20, so FY2006 to FY2009 show a close 1.375 times
  the raw one (3.30 against 2.40 on 2008-12-31) and passed under the 3.0 gate, while the later
  consolidations multiply to 600. The cyclical-timing Hong Kong book held it in 2008 and 2009
  at as-traded caps of 0.39bn and 0.63bn; the 2009 holding (+239%) is worth 0.68 points of
  excess. The gate now also reads the consolidations alone, and the cap lowers by 1.375 times 12
  (FY2009 lowers to 2.32bn and stays, its as-traded 2.31bn).
- No turnover at all. CETX FY2016 (cap $59.4bn, about $40m real), TOPS and PIXY have under 10
  traded bars, because their volumes round to 0 on prices scaled by millions. With a price
  factor of 3 or more the cap now lowers by it where turnover is unknown.
- A delisting record read as a split. 4541.T's splits_calendar holds 1:70,384,700 on 2023-03-29
  (the cash-out), with FMP's shares fixed at 5.09bn and prices scaled down to about 34 yen, so k
  reads 72 to 181 and the gate read a factor of 70 million. A record over 1:10,000 either way is
  now left out, so no factor opens the gate and FMP's cap stands (FY2011 161bn against a true
  104bn, FY2015 167bn).
- A flat line with no volume. A line with no traded row in a window kept every row, and the book
  bought stale quotes and suspension rows: BC88.L (Chemring, dead, close flat at 95 then 70 with
  adjClose steps of +17%, +4%, +106%, booked +105.9%), 76ID.L (Rotork, dead), 1114.HK (suspended
  2021-03-31 to 2022-10-04 at 7.30, resumed at 2.70), BR.BK 2015, GBGLOBAL.NS 2022 (930 flat). A
  flat window now loses its rows. 600694.SS's 2006 exit window holds flat halt rows on an
  adjClose step (17.06 against 13.65 at the last trade): the exit goes to the last-traded fill.
  TRUE.BK 2006-04 (7 closes) and the pre-2009 Thai NVDR feeds keep theirs. A window whose stale
  quote steps once kept its rows under this round's rule (BC88.L 2009-10, held then); item 7.
- A moved bar on another scale. T moved ELUX-A.ST's 2006 entry off a zero-volume row onto
  52.43 on 950 shares, while ELUX-B traded 95.62 on 1.48m. A's traded rows sat near half of B's
  from 2006-06-08 to the end of 2006 (monthly medians 0.53 to 0.55 from July, against 1.03 to
  1.15 before and after), and the two met again by 2007 (156.37 against 153.72). Booked +207.5% against +64.7%
  for B. A replacement bar more than 25% off its sibling's scale now moves too (narrowed in
  item 7). A's A/B ratio in 2008 and 2009 ran up to 1.38 on thin trading; the reference is the
  pair's median ratio in the cache's other windows within 400 days (one window each side on an
  annual leg), so a premium like that stays.
Still open after this round: Bonjour FY2003 passes turnover (4.6e-4 on a cap about 16.5 times
too high) and stays in the 2004 book; caps inflated with no later split (7386.T, 2.5 million
times its sales; 8038.T from FY2017); forward-split inflation (SALEE.BK FY2014); k near 1 where
FMP scaled EPS and shares together (DBGI FY2021 trades 3.9e-4 of its cap and stands; CET.TO);
caps in the reporting currency (BWMX, NAAS). Side effects to read on the rerun: Keihan 9045.T
FY2005 and FY2006 (no bars and 8 traded bars, on FMP caps of 182bn and 287bn yen; FY2007 reads k
1.0 on 293bn) lower under the floor; 4541.T FY2007 and FY2010 stand at FMP's cap, while their
true caps (89bn, 97bn) sit 3% to 11% under the floor; 0756.HK FY2010 comes back at 2.18bn on a
price factor of 10; BBD-A.TO and BBD-B.TO FY1997 to FY1999 (C$6.5bn to 10bn, no bars, later splits
6.25 to 12.5 and a 1:25) lower by 25 to C$0.26bn to 0.40bn, under the floor, though Bombardier's
cap then was real (no booked effect: deleveraging Canada 2000-01 is a cash period).

**7. Found by the second review of the checks (2026-10-30).** Three of item 6's fixes reached too
far, and one was too loose:
- The sibling test moved real prices. It ran in any window where T moved any row (a holiday row
  was enough) and tested both lines, so it moved the liquid common's rows too: LG Chem 051910.KS,
  deleveraging Korea 2004-04 (515k shares a day against the pref's 27k), lost its entry on a
  holiday-only window, because the common over pref ratio drifted from 2.24 to 1.44 in 18 months
  and still passed the 80% stability test. It moved Samsung Heavy's pref 010145.KS, held in
  deleveraging Korea: its 2011-10 and 2012-01 entry bars, and in 2023 (a meme pref at 36 times the
  common that fell to 4 to 11 times on 58k to 124k shares a day) its 2023-01 entry and its whole
  2023-07 window. It moved BP-B.L
  when BP.L fell (2016-01: a fixed-rate pref at 160.5 against 161), and 1935.HK, whose only link
  to 1789.HK is one coincidental FY2016 cap of 2,362,800,000. The reference also took a
  neighbouring window's wrong-scale prints: on ELUX-A's real rows the 2006 anomaly was 2007's
  reference, and all 7 correctly scaled 2007-07 rows moved. The test now asks only whether T's
  substitution landed on another scale: the line's moved bar must have opened its window and sat
  on the reference, the line must be the thinner one, the reference is the median of the other
  windows' medians (2 windows, 5 days, 80% of the days in the band), and a replacement moves only
  when it is off both the reference and the moved bar's own ratio. On the reviewed share-class
  rows (Korea, UK, Brazil and Sweden, plus ELUX's 2005 to 2009 rows), with the legs' closed
  dates, it moves ELUX-A's four 2006 rows and nothing else, and the unrelated pairs move nothing.
- Coincidental equal caps. About a dozen same-market pairs of unrelated companies share one FY
  cap over the floor (450080.KS and 454910.KS, CLS and WST, APLE and CZR, BCT.BK and EPG.BK,
  several in Hong Kong). Lines now count as share classes only with an identical cap in two FYs
  or more, for check M's turnover and for the sibling test. Real class pairs share 19 to 27.
  1859.HK and 6111.HK share two and still join.
- Missing turnover read as thin turnover. ERIC-B.ST has no rows before 2000-01-03, so Ericsson's
  FY1999 (SEK 786bn) read the thin A class alone and both lines became ineligible; the only gate
  open was the 2008 1:5 consolidation (price factor 1.25). A row is now made ineligible only when
  the price factor itself is 3 or more and every line of its group has bars; otherwise a failing
  cap lowers. ERIC-A.ST FY1999 lowers to 157bn and ERIC-B.ST to 218bn; 02826K.KS FY2008, FY2009
  and FY2011 to FY2013 (028260.KS listed in December 2014) lower to 5.26T won, over the floor.
  0653.HK FY2024 and 0760.HK FY2006 lower too, still under the floor. A failing row whose divisor
  is 1 cannot be lowered: it is written unchecked at FMP's cap, counted in cap_rows_flagged and
  listed in cap_unchecked (HAO FY2025, 388m, under the floor). 02826K.KS stays a known limit: its
  common 028260.KS has no stock_eod rows before 2014-12-17, and ERIC-B.ST's rows also start after
  FY1999's end (2000-01-03), so a line's first row cannot tell a line not yet listed from a feed
  that starts late.
- A stale quote that steps once. BC88.L's 2009-10 window reads 95 on 10-01 and 10-02, then 70
  for six days, all at volume 0: two closes, so it stayed buyable at 18.61 and was held. A line
  with no traded row now needs its close to change on two days or more in the window, day over
  day in date order, to keep its rows. A count of distinct closes is not the test, because
  pre-2009 Thai NVDR feeds can tick between two prices with no volume: BH-R.BK 2007-07 alternates
  48.0 and 48.5 (a stand-in held in deleveraging Thailand), and RATCH-R.BK 2005-07 alternates
  37.75 and 37.98 while RATCH.BK trades 38.68 and 38.91 on 0.6m to 2.3m shares. Of 532 NVDR
  windows from 2000 to 2009 with no traded row, 40 show three closes and 5 show two; four of the
  five change on two days or more and stay, and BCH-R.BK 2006-04 (1.55, then 1.61 once) leaves.
  The rule also removes BR.BK 2004-10 and 2005-04, GBGLOBAL.NS 2020-01 and most of 76ID.L's
  windows (one change or none). 76ID.L 2000-01, 2000-04, 2000-07 and 2000-10 keep theirs; none is
  held.

**Real moves the cap cut.** DAVE (deleveraging US 2025 Q2, +217.4%), 2609.TW (Taiwan 2021 Q2,
+332.6%), SANWARIA.NS (India 2012 Q1, +201.9%), and in cyclical-timing US UAL (+462%), SE
(+235%), NIO (+437%), CROX (+225%), AS (+207%) and BWMX (+440.0%; its market cap is in MXN, see
"Cost tiers are USD but market caps are local currency"). They traded on both bars, with no
factor step and no inflated cap, and the data checks keep them.

**Open, not fixed here.** Fund lines with volume 0 on every row stay in universes that hold them
(`profile.isFund`). Market caps stay in the reporting currency. A fiscal year can be used before
its filing date, because the 45-day lag runs from the period end (ZEST's FY2019, period end
2019-03-31, filed 2019-08-19, entered the 2019-07-01 screen). The latest-filing tie-break prefers
the larger of two rows for the same fiscal year. Preferred lines can be held beside their common
line (TSX, US). LTP fills have no per-date holiday test. Twelve other topics with SET legs still
hold NVDR lines beside their ordinary lines until their own reruns.

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

**Update 2026-10: revenue-accel's four funds-excluded legs were rerun** with the rest of the
topic (`results/diagnostics/returns_nofunds_*.json`). As screened against funds excluded,
CAGR: US 3.55% -> 4.27% (+0.72pp, +0.70 in the August files), UK 7.08% -> 8.24% (+1.16,
was +0.58), Canada 8.80% -> 8.59% (-0.21, was +0.13), Germany 3.51% in both (was +0.07; the
two Germany-only runs are identical in every year). The sign flipped in Canada between the
two vintages, which is the point of the paragraph above.

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

**Still unchecked.** Only `revenue-accel`, `small-cap`, `sector-rotation` and, since
October 2026, `price-to-sales` (US only) carry an `isFund` diagnostic.
These rank on a revenue-derived metric with no guard at all: `market-share`,
`ocf-growth`, `revenue-surprise`, `fcf-conversion` and `owner-earnings`. Measure
`--exclude-funds` on each when its topic next comes up for a rerun.

**price-to-sales: measured on the US leg (2026-10-14).** The rerun US book has mutual-fund
share classes (PRGTX, JTSSX, SMTAX, JMUEX, GEMAX, FLCSX) among its ten recorded lowest-P/S
names in 38 of 103 quarters, from April 2013 to April 2025. One run of the US leg with
`price-to-sales/backtest.py --exclude-funds` is committed as
`price-to-sales/results/diagnostics/returns_nofunds_NYSE_NASDAQ_AMEX.json`. It is a single
diagnostic run, made on 2026-10-14 from the code at `cf1decc` and not gated: no repeat run
checked that it reproduces. Both files measure the same 103 quarters with last-traded-price
exits, neither has a cash quarter, and the S&P 500 return is identical in every quarter:

| US | As screened | Funds excluded | Delta |
|----|-------------|----------------|-------|
| CAGR | 14.41% | 13.72% | -0.69pp |
| Excess vs S&P 500 (8.02%) | +6.39pp | +5.71pp | -0.68pp |
| Sharpe | 0.508 | 0.458 | -0.050 |
| Max drawdown (July 2018 to April 2020 in both) | -46.01% | -55.72% | -9.71pp |
| Fund-format tickers among the ten recorded names | 95 of 1,030 slots | none | |

Removing funds *hurts* here, as in `small-cap`: without them the US book returns +10.77% in
2020 against +20.60% and -1.76% in 2019 against +4.03%, so the fund share classes damped the
2018-2020 fall. The return differs in 13 of 26 years, all from 2013. The US result doesn't rest
on the funds: it is 5.71 points a year ahead of the S&P 500 without them. The other 15
markets weren't run without funds. fcf-conversion was rerun in the first wave without a
funds-excluded run; it has no such flag yet.

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

**Not a no-op everywhere (found and fixed 2026-08-21).** Five days after the measurement
above, the filter was found to delete valid rows in windowed fetches after all. It compared
each row with neighbours a whole rebalance interval away, and where it emptied a symbol's
window that stock dropped out of the rebalance: fcf-growth XETRA 2000 read -3.3% with the
bug and +27.9% without it. Neighbours must now sit within 10 days of each other
(`max_neighbor_gap_days`). Every windowed-fetch result produced between 2026-04-09, when
the filter landed, and the fix carries the bug. Most published files of the twelve
second-wave topics rerun in October 2026 were written in that window, so the fix is one
candidate cause of their published-to-rerun differences, and no run isolates it for them.
A few of their files are older, from March 2026, and predate the filter itself (six
fcf-compounders markets and graham-number's Australia, France, South Africa and Singapore
among them).

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

## The data holds almost no company that stopped trading before 2015 (US) or 2020 (elsewhere) (measured 2026-10-13)

**Status:** Open, not fixed. Whether older delistings can be loaded is being investigated.
**Affects:** every backtest that starts before 2015 on a US leg or before 2020 on any other
leg, which is every published 2000-2025 result. No exit rule changes it.
**Severity:** High for any statement about 2000-2014. The effect on returns isn't measured.

METHODOLOGY.md said until 2026-10-13 that delisted companies stay in the data and that only
pre-1990 delistings are thin. That was wrong. Counted in the daily price table (`stock_eod`):
symbols with a full year of price bars in 2004, by the year of their last bar.

| Market | Still trading | Last bar 2020-2025 | Last bar 2015-2019 | Last bar before 2015 |
|---|---:|---:|---:|---:|
| US | 3,707 | 470 | 174 | 10 |
| Japan (JPX) | 1,916 | 109 | 0 | 0 |
| UK (LSE) | 416 | 121 | 0 | 0 |
| India (NSE) | 465 | 47 | 0 | 0 |
| Hong Kong (HKSE) | 545 | 31 | 0 | 0 |
| Canada (TSX) | 300 | 53 | 0 | 0 |
| Germany (XETRA) | 284 | 45 | 0 | 0 |

Of 4,361 US symbols trading through 2004, 10 (0.2%) end before 2015. In the six other
markets none ends before 2020. The delisted-company list (`delisted_companies`), as we hold
it, has 10,012 symbols, of which 25 US and 11 non-US were delisted before 2015.

What that means:

- Every result for 2000-2014 in the US, and for 2000-2019 elsewhere, is measured on
  companies that survived at least to 2015 or 2020. The 2003 book of any screen could only
  pick companies still trading more than a decade later.
- Last-traded-price exits (next section) can only capture a company that stopped trading
  from about 2015 in the US and 2020 elsewhere. The second wave's fills agree. Of 142 exits
  filled on its first six topics' 96 market legs, all 27 US fills were priced at a last bar
  in 2017 or later and 89 of the 115 others at one in 2021 or later. The remaining 26 (17
  on China legs) have no delisting recorded at the time of the fill: 24 have no delisting
  record at all, and two (RSAB.L, filled at a 2006 bar in deleveraging and in
  price-to-sales) have a delisting dated 2024. Of 230 exits filled on the other six
  topics' 93 legs, 59 of the 60 US fills were priced at a last bar in 2017 or later and one
  in May 2016 (EMQ, in pe-mean-revert), and 134 of the 170 others at one in 2021 or later.
  The remaining 36 (26 on China legs) have no delisting recorded at the time of the fill
  either: 34 have no delisting record at all, and two (87IP.L, filled at a December 2007
  bar in graham-number and in pe-mean-revert) have a delisting dated 2024. The earliest is
  WTB.L, filled at a December 2000 bar in graham-number. They may be suspensions, gaps in
  the price rows or delistings the list doesn't hold; that wasn't checked symbol by symbol.
  The third wave's 245 fills (four topics with a drop book) agree too: 68 of the 74 US fills
  were priced at a last bar in 2017 or later, and 149 of the 171 others at one in 2021 or
  later. The six older US fills are four 2016 bars, UB at a November 2008 bar (its delisting
  record is dated 2021) and BNI at a February 2010 bar, delisted that day. The 22 older non-US
  fills (17 on China legs) carry no delisting record. One of them, China Vanke (000002.SZ),
  is a known suspension (under "Not fixed yet, found in the third wave").
- The US count for 2015-2019 (174) is well under the count for 2020-2025 (470), so the
  record for those years may be partial too. Not established.
- The price-table count covers the US and six other markets. Other markets weren't counted
  one by one; the delisted-company list is the evidence for them.
- We haven't established whether older delistings are missing at the vendor or only from
  our copy of the list. Until that is settled, a "survivorship-free" reading of any
  2000-2025 result here is wrong, and so is a claim that last-traded-price exits fixed
  survivorship for the whole window.

dcf-discount, defensive-quality and the three pairs topics no longer screen on
`isActivelyTrading = true` (removed in #7 and #16; their October 2026 reruns are the first
results without it), so a company that later stopped trading can be picked in the years
before it did, within the gap in the data described above. The pairs topics rebuild their
universe each year from what was listed and traded in the formation year: that removes the
size look-ahead, and survivorship is reduced, not fixed. dogs-of-dow's US leg still uses
today's Dow 30 members in every year.

---

## Exit-side survivorship in the invested branch (recorded 2026-09-03, fixed 2026-10)

**Status:** Fixed in code by last-traded-price (LTP) exits, now the default
(`data_utils.DEFAULT_EXIT_METHOD = "ltp"`); `--exit-method drop` keeps the rule described
here. A committed result carries the fix only once its topic is rerun, and it then writes
`"exit_method": "ltp"`. A results file with
no `exit_method` key used the drop rule: it predates the fix, or it is one of the 13 Norway
legs rerun with `--exit-method drop` in the third wave to match their topics' other markets.
The first rerun wave is cyclical-timing, asset-growth,
fcf-conversion, cash-conversion and graham-net-net. The second is deleveraging,
fcf-compounders, fcf-yield, revenue-accel, dividend-sustainability, price-to-sales,
dogs-of-dow, low-debt, dividend-growth, pe-mean-revert, rising-yield and graham-number. The
third is 52-week-high, dcf-discount, defensive-quality, pairs-fundamentals, pairs-zscore and
pairs-multi-pair, where a pair closes at the last bar both legs traded. All twenty-three have
been rerun and carry the key. The fix reaches only as far back as the data
holds delisted companies: about 2015 in the US and 2020 elsewhere (previous section).
**Affects:** all 66 floor topics, and every topic that averages `filter_returns`'
output. LTP is wired into all 75 topics that drop a held name for a missing exit price.

Under the drop rule (the only rule before LTP, and the default until 2026-10), when the book
clears the floor at entry, the period return is the mean of the names that *survived
to the exit date*. `filter_returns` drops a name whose EXIT price is missing, so a
holding that delisted to zero contributes **nothing** rather than -100%. It also drops
a name whose realised return exceeds `max_single_return`, which is upside-only, so that
drop removes winners. The two pull in opposite directions, and neither was measured
before LTP.

This is not a new defect. It is what every topic did before the floor guard
existed, and the B005 correction preserved it byte-for-byte on purpose: that
change is about thin books **at entry**, and quietly altering exit-side behaviour
under cover of a look-ahead fix would move published numbers in the flattering
direction with no re-run to justify it.

**It got its own decision and its own re-run.** Of the options (drop-to-zero,
drop-to-benchmark, exclude the period, last traded price), LTP was chosen.
Drop-to-zero is wrong for acquisitions, which most US delistings in the backtest
window look like, and turns data gaps into wipeouts; drop-to-benchmark pulls every result toward
the index. The rule, in `data_utils.LtpExits`: a name with an entry price but no exit
price exits at the last `adjClose` after its entry bar and on or before
min(exit date, delisting date + 7 days), cleaned by `remove_price_oscillations` like
the topic's own prices. A delisting more than 30 days before the entry bar belongs to
an earlier holder of the ticker and is ignored. No bar in that window, and the name
stays dropped as before. Fills are requested only for periods the entry-side cash
guard has already invested, so LTP never decides the entry-side cash guard, and they
pass through `filter_returns` like any other exit. `--exit-method drop|ltp|both`
selects the rule: `drop` reproduces the old one, and `both` publishes the drop book
with the LTP book beside it. Every fill is listed under `ltp.fills` in the results.

Measured 2026-10-09 on eight legs, both methods in one run per leg, on the books
before the listing rules, ISIN dedup and graham-net-net's NCAV currency fix: the
largest moves were graham-net-net LSE -0.99pp CAGR, 52-week-high US +0.72pp and
graham-net-net US -0.59pp. Direction is strategy-specific: distress and net-net legs
lose (their dead names were real wipeouts), US momentum gains (acquired names sat
flat through bad quarters).

Measured again 2026-10-13 on the second wave's 189 market legs, both methods on identical
selection in one run per leg (`ltp_portfolio.cagr` minus `drop_portfolio.cagr`):

| Topic | Legs | Exit rule alone, CAGR points | Exits filled | Left unpriced |
|---|---:|---|---:|---:|
| deleveraging | 16 | -0.13 to +0.08 | 44 | 0 |
| fcf-compounders | 18 | -0.12 to +0.14 | 9 | 0 |
| fcf-yield | 16 | -0.14 to +0.09 | 18 | 0 |
| revenue-accel | 15 | -0.21 to +0.45 | 20 | 0 |
| dividend-sustainability | 15 | -0.09 to +0.08 | 23 | 1 |
| price-to-sales | 16 | -0.10 to +0.26 | 28 | 0 |
| low-debt | 15 | -0.27 to +0.07 | 125 | 0 |
| dividend-growth | 14 | -0.14 to +0.21 | 10 | 0 |
| graham-number | 17 | -0.29 to +0.12 | 32 | 0 |
| pe-mean-revert | 13 | -0.19 to +0.19 | 27 | 0 |
| rising-yield | 17 | -0.10 to +0.12 | 29 | 0 |
| dogs-of-dow | 17 | -0.12 to +0.46 | 7 | 0 |

In the first six topics 93 of the 96 legs move by 0.14 points or less. The three that move
more are revenue-accel XETRA +0.45 (7 exits filled, entered 2018 and 2021-2023),
price-to-sales China +0.26 (6) and revenue-accel SES -0.21 (1 fill, on a book invested in 6
of 25 years, too thin to test).

In the other six, 86 of the 93 legs move by 0.14 points or less. The seven that move more:

- dogs-of-dow SHZ +0.46, from two fills of one stock. 000002.SZ was bought in January 2012
  and January 2015 and has its last bar on 2012-12-25 and 2015-12-18. Neither fill has a
  delisting record, and the stock is in the book in 13 years, the last 2024, so these
  aren't delistings. They look like trading suspensions or gaps in the price rows over the
  year end. Which of the two wasn't checked against a source.
- graham-number SES -0.29 and low-debt SES -0.27, one fill each of the same stock. HKB.SI
  was bought in January 2024 at 12.93 and in July 2024 at 10.00, and its last bar is
  2024-08-12 at 3.60, with no delisting record. The low-debt leg is invested in 2 of 25
  years and is too thin to test.
- dividend-growth China +0.21, from one fill: 000786.SZ, bought in July 2014 at 5.62, last
  bar 2015-04-09 at 15.29, no delisting record. It lifts that year from 77.61% to 87.04% on
  a book of 10 names, in a market invested in 11 of 25 years and too thin to test.
- pe-mean-revert Canada +0.19 (6 fills), Hong Kong +0.19 (2) and India -0.19 (4).

Measured 2026-10-14 the same way on the third wave's 66 legs with a drop book:

| Topic | Legs | Exit rule alone, CAGR points | Exits filled | Left unpriced |
|---|---:|---|---:|---:|
| 52-week-high | 18 | -0.21 to +0.05 | 176 | 8 |
| dcf-discount | 16 | -0.10 to +0.17 | 50 | 0 |
| defensive-quality | 17 | -0.07 to +0.08 | 15 | 0 |
| pairs-fundamentals | 15 | -0.10 to +0.28 | 4 pairs | 0 |

63 of the 66 legs move by 0.14 points or less. The three that move more: pairs-fundamentals
China +0.28, from one pair closed at China Vanke's last bar before a trading suspension (under
"Not fixed yet, found in the third wave"); 52-week-high Japan -0.21 (15 fills); and
dcf-discount US +0.17 (7 fills, 2016 to 2024, all names the removed `isActivelyTrading`
filter would have kept out). pairs-zscore and pairs-multi-pair have no drop book, so nothing
is measured for them: across their 24 legs 16 positions closed at a last common bar more than
10 days before the year end (8 in each topic).

rising-yield has no leg above 0.12 points. No leg in the wave moves by more than 0.46
points on the exit rule, so the larger moves between the published and the rerun files come
from the other changes; see "What the second wave's rerun showed" above.

**Still open after the fix.**
- `max_single_return` is still upside-only, so an acquisition premium above the cap
  still vanishes, filled or not.
- A name whose trading stopped before its price reached zero exits at its last trade,
  not at -100%.
- The delisting bound needs a `delisted_companies` row. Most LSE fills have none, so
  there the window runs to the exit date; `ltp.filled_without_delisting_row` counts
  them.

**Related quirk, same area.** If a book clears the floor at entry but every name
later loses its exit price, the invested branch's
`sum(returns) / len(returns) if returns else 0.0` writes `portfolio_return 0.0`
with `stocks_held 0`, which `metrics.period_accounting` then counts as a cash
period anyway. Pre-existing, and the corrected guard makes it reachable by a second
route; LTP leaves the rule as it is. Under `ltp` such a period can count as invested
instead, because fills give it holdings and the cash and invested counts read
`stocks_held`, so an ltp run can show fewer cash periods than a drop run of the same
screen.

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

**Status:** Partly cleared by reruns, and it clears only by re-running. The code side is done.
**Affects:** every committed result that has not been rerun. When recorded, that was the
whole corpus: 2,884 period records, of which **0** carried the entry funnel.

> **Update 2026-10-13.** `scripts/scan_results_invariant.py` now checks 19,653 period
> records that carry the funnel, in 18 topics: the first rerun wave except cash-conversion
> (asset-growth, cyclical-timing, fcf-conversion, graham-net-net), the twelve second-wave
> topics (deleveraging, dividend-growth, dividend-sustainability, dogs-of-dow,
> fcf-compounders, fcf-yield, graham-number, low-debt, pe-mean-revert, price-to-sales,
> revenue-accel, rising-yield), and volume-confirmed-momentum and yield-gap from their own
> reruns. All pass. The 2,884 records still without it are all graham-timing's. In those 18
> topics a cash period can now be attributed from the file. On the first six second-wave
> topics' 96 market legs, 338 of 2,023 cash periods are guard cash (10 or more names passed
> the screen, fewer than 10 buyable) and the other 1,685 had fewer than 10 names passing.
> On the other six topics' 93 legs, 128 of 755 cash years are guard cash and the other 627
> had fewer names passing than the topic's floor (10 names, and 5 in dogs-of-dow, which has
> no guard cash). Every other topic is as described below.
>
> **Update 2026-10-14 (third wave).** The scanner now checks 26,047 funnel records in 34
> topics: the 18 above, 52-week-high, dcf-discount and defensive-quality, and the 13 topics
> whose Norway leg was rerun. All pass. The three pairs topics write per-year pair counts
> (`pairs_formed`, `pairs_entered`) instead of this funnel, and their cash rule is fewer than 3
> pairs entered. In 52-week-high, dcf-discount and defensive-quality 73 of 431 cash periods
> are guard cash (14 of 184, 26 of 81 and 33 of 166).

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

**Resolved 2026-10-14 (third wave).** The OSL leg was rerun with `--exit-method drop`, and
`returns_OSL.json`, `oversold_quality_osl.json` and the comparison node now carry
`total_rebalances` 103 with cash 101 and invested 2, so the cash chart reads 98% (101 of 103).
Returns are unchanged (1.49% a year against 10.91% for the Oslo All-share index); the leg is
invested in 2 of its 50 measured quarters and too thin to test.

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

`scripts/scan_results_invariant.py` flagged **22 arithmetically impossible records
across 13 topics** when this was recorded (2026-09-03). Every one is an OSL (Oslo) leg
except `qarp` JNB, which is the separate 100%-cash class: JNB has no
`LOCAL_INDEX_BENCHMARKS` entry, falls back to SPY and prices nothing.

**As of 2026-10-13 it flags 15 records across 9 topics.** Four topics' records were
repaired by reruns: volume-confirmed-momentum (2026-09-12, which also removed its
`vcm_osl.json`) and deleveraging, fcf-compounders and fcf-yield (second rerun wave,
2026-10). Their Norway records now carry `total_rebalances` and `window_truncated: true`,
and cash + invested equals `total_rebalances`: deleveraging 77 + 26 of 103 quarters (50
measured), fcf-compounders 25 + 0 of 25 years (12 measured), fcf-yield 23 + 2 of 25 (12
measured). The legs stay benchmark-truncated, because `^OSEAX` still starts in 2013. Counted
over their measured periods, deleveraging is invested in 26 of 50 quarters and is testable
but not ranked with full-window markets, and fcf-compounders (0 of 12 years) and fcf-yield
(2 of 12) are too thin to test. The table keeps the records as they were; the last column
says which are repaired. The three second-wave posts have not been rewritten yet, so the
findings about live posts below still apply to them.

**As of 2026-10-14 it flags 1 record in 1 topic: `qarp` JNB.** The third wave reran the
Norway leg of the other eight topics in the table (and of five more) with `--exit-method
drop`, so it matches their other markets, which aren't rerun. Their records now carry
`total_rebalances`, a window label and `window_truncated: true`, and cash + invested equals
`total_rebalances`. Cash + invested of all rebalances, and measured periods: earnings-consistency
25 + 0 of 25 (12 measured), etf-underowned 20 + 0 of 20 (12), ev-ebitda 17 + 8 of 25 (11),
ev-ebitda-relative 25 + 0 of 25 (11), garp 89 + 14 of 103 (50), oversold-quality 101 + 2 of
103 (50), relative-strength 65 + 38 of 103 (50), value-momentum 29 + 22 of 51 (24). Counted
over their measured periods, five of the eight are too thin to test (earnings-consistency,
etf-underowned and ev-ebitda-relative never invest, garp invests in 14 of 50 quarters and
oversold-quality in 2 of 50); ev-ebitda (7 of its 11 measured years invested),
relative-strength (38 of 50 quarters) and value-momentum (18 of 24 half-years) are testable
but not ranked with full-window markets. Their posts haven't been rewritten yet either.

`^OSEAX` starting 2013-03-05 is the whole cause, re-verified against the
warehouse on 2026-09-03: `^OSEAX` 2013-03-05 (3,377 rows), `^TWII` 1997-07-02
(7,179), `^SET.BK` 1982-01-04 (11,247). It is the only `LOCAL_INDEX_BENCHMARKS`
symbol starting after 2000. The blocker also named TAI and SET as suspect; they
are not.

| topic | published post | n_periods | cash | invested | years | record now |
|---|---|---|---|---|---|---|
| deleveraging | risk-03-deleveraging (2026-02-05) | 50 | 53 | -3 | 12.5 | repaired 2026-10 |
| earnings-consistency | growth-02-earnings-consistency (2026-02-26) | 12 | 25 | -13 | 12.0 | repaired 2026-10 (drop mode) |
| etf-underowned | etf-03-underowned (2026-03-28) | 12 | 20 | -8 | 12.0 | repaired 2026-10 (drop mode) |
| ev-ebitda | value-03-ev-ebitda (2026-01-17) | 11 | 13 | -2 | 11.0 | repaired 2026-10 (drop mode) |
| ev-ebitda-relative | timing-02-ev-ebitda-relative (2026-02-09) | 11 | 25 | -14 | 11.0 | repaired 2026-10 (drop mode) |
| fcf-compounders | cashflow-04-compounders (2026-03-13) | 12 | 25 | -13 | 12.0 | repaired 2026-10 |
| fcf-yield | value-04-fcf-yield (2026-04-05) | 12 | 23 | -11 | 12.0 | repaired 2026-10 |
| garp | factor-08-garp (2026-02-07) | 50 | 73 | -23 | 12.5 | repaired 2026-10 (drop mode) |
| oversold-quality | reversion-03-oversold-quality (2026-01-24) | 50 | 101 | -51 | 12.5 | repaired 2026-10 (drop mode) |
| qarp | factor-02-qarp (2026-01-01) | 0 | 51 | -51 | 0.0 | open |
| relative-strength | momentum-07-relative-strength (2026-02-23) | 50 | 65 | -15 | 12.5 | repaired 2026-10 (drop mode) |
| value-momentum | factor-03-value-momentum (2026-03-23) | 24 | 29 | -5 | 12.0 | repaired 2026-10 (drop mode) |
| volume-confirmed-momentum | momentum-08-volume-confirmed (2026-02-24) | 24 | 40 | -16 | 12.0 | repaired 2026-09 |

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
>
> **As of 2026-10-13 the truncated set is the same 18 legs, split 6 and 12.**
> deleveraging, fcf-compounders and volume-confirmed-momentum no longer carry a
> negative `invested_periods`, so they moved from the first group to the second.
> Their rerun records say so themselves (`total_rebalances`, `window_truncated: true`,
> a `window_label`), and their charts gate on that field. yield-gap's rerun records
> do the same now, and its `returns_OSL.json` reads 11 of 25 like the comparison node,
> not n=25 as in the caution above. That leaves seven truncated legs with nothing in
> the record to say so: `52-week-high`, `etf-rebalancing`, `garp`, `pe-compression`, `peg-ratio`,
> `price-momentum` and `price-to-book`. fcf-yield's Norway leg is truncated too (12 of
> 25 measured) but is not in its curated comparison file, which is where the
> detector looks.
>
> **As of 2026-10-14 the truncated set is 19 legs, and every one labels itself.** The
> third wave reran 52-week-high (whose Norway node carries `window_truncated: true` now)
> and the Norway leg of 13 topics, which covers the six legs that also carried a negative
> `invested_periods` and the other six unlabelled ones. So no truncated leg carries
> defect (a) any more, and none is a legacy record without a label. The new leg is
> etf-underowned: its published comparison node was an older 20-period run, not
> truncated, and the rerun node is measured over 12 of 20 rebalances. Of the seven legs
> named above, four are testable on their truncated window (etf-rebalancing, peg-ratio,
> price-momentum, price-to-book) and two are too thin to test (garp, pe-compression);
> 52-week-high's Norway leg is invested in all 50 of its measured quarters and in 74 of 103
> in all. Its published `invested_periods` of 21 (50 measured minus 29 cash quarters, all
> before the window) is where a "58% cash periods" figure in the live comparison post comes
> from.

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

### Six cohort topics still count cash over the metrics window

(Seven until 2026-10: cash-conversion now has a per-bucket floor and emits `cash_periods` and
`invested_periods` for all four buckets, with cash + invested equal to the years measured.)

`altman-z`, `asset-light`, `income-quality`, `margin-expansion`, `roe-dupont` and
`sustained-roic` emit `"cash_periods": {"high": n, "low": m}` from sites that all read
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

**Update 2026-10-13: three of the seven rows have been rerun.** deleveraging,
volume-confirmed-momentum and yield-gap now carry `window_truncated: true` on their
Norway record, so the gate excludes those legs because the record says so, and the
"no-op" sentence above is true only of topics not yet rerun. deleveraging's Norway
leg is invested in 26 of its 50 measured quarters (77 of all 103 are in cash), so it is
testable but not ranked with full-window markets, and it reads 5.52% a year over those 50
quarters (was 9.85% with a reported 53 cash quarters). Its
charts leave the leg out as a truncated window. The other four rows (ev-ebitda,
oversold-quality, relative-strength, value-momentum) still describe committed legacy records.

**Update 2026-10-14: the other four rows have been rerun too** (the Norway leg, in drop mode,
in the third wave). Their records carry `window_truncated: true`, and their
`invested_periods` are 8, 2, 38 and 22 of all rebalances. Over their measured periods they
are invested in 7 of 11, 2 of 50, 38 of 50 and 18 of 24, so only oversold-quality's is too
thin to test. Two rows of the table above were wrong about the charts: relative-strength's
and value-momentum's chart scripts gated on neither `invested_periods` nor
`window_truncated`, so their comparison charts drew Norway both before and after the
`invested_periods` fix, and the live momentum-07 and factor-03 comparison images match a
render from the pre-rerun data (found in the third-wave install check). price-momentum's
script had the same gap. Since 2026-10-15 all three scripts leave a truncated leg out of their
comparison charts and name it in a footnote; the live images keep the Norway bar until those
posts are republished. The three scripts test no `invested_periods` gate, so gate 12 doesn't
cover them. The truncated legs that do drop out now include peg-ratio's Norway leg, which
the rerun makes testable (+2.62 points against the Oslo All-share index): the predicate
excludes every truncated window, the question decided below.

**Two consequences to expect after the re-run, and one call for a human (decided below).**
Once re-run records carry `window_truncated = True`, the ~8 currently-rendered
truncated legs from the defect-(b) list above (`peg-ratio` OSL and the rest)
will DROP out of ranked comparisons. That is intended: it is the fix for the
live 2013-2025-Norway-bar-under-a-2000-2025-header defect. But the predicate is
blunt — it excludes ANY truncated leg, including a trivially truncated one.
**Whether a truncated leg should instead appear WITH its measured window
annotated on the bar was a real editorial question, deliberately left open until
2026-10-15 (decided below).** `window_truncated` / `window_label` are already
carried into every results record and already read by the 54 stdout printers, so
the chart layer only needs to read them if that is the call.

**Decided 2026-10-15:** a leg measured over a shorter window is never ranked with
full-window legs, and its too-thin test counts its measured periods (METHODOLOGY.md,
"Reading a market's result").

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
