# Cash Conversion Cycle (CCC)

Backtest sorting stocks by Cash Conversion Cycle across 15 exchanges, 2000-2025.

## Signal

CCC = Days Sales Outstanding + Days Inventory Outstanding - Days Payables Outstanding

| Portfolio | Threshold | Idea |
|-----------|-----------|------|
| Low CCC | < 30 days | Capital-efficient: collect fast, hold little inventory, pay slow |
| Mid CCC | 30-90 days | Average working capital efficiency |
| High CCC | > 90 days | Capital-intensive: slow collections, heavy inventory |
| Low + Decreasing | < 30 days + YoY decline | Improving efficiency (strongest signal hypothesis) |

**Universe:** Non-financial stocks above an exchange-specific market cap threshold.
**Rebalancing:** Annual (April 1), equal weight. A portfolio with fewer than 10 names buyable at entry holds cash that year.
**Benchmark:** Each exchange's local index, in local currency (S&P 500 via SPY for the US). Where no local index data exists (South Africa), it falls back to the S&P 500 in USD.

## Backtest Setup

| Parameter | Value |
|-----------|-------|
| Period | 25 annual rebalances, April 2000 to April 2024, each held one year (last exit April 2025) |
| Financial data lag | 45 days before each rebalance |
| Execution | Next-day close after the rebalance date |
| Costs | Size-tiered transaction costs (0.1-0.5% per trade) |
| Minimum book | 10 names per portfolio with a usable entry price, counted after same-ISIN dedup. Below that, the portfolio holds cash (0%) that year |
| Entry floor | Entry price of at least 1.0 in the exchange's price unit (pence on LSE, cents on JSE) |
| Return filter | A single-stock return above 200% is dropped as a likely data error |
| Missing exit price | Last traded price on or before the exit date |
| Benchmark | Local index per market, in local currency (S&P 500 in USD for South Africa) |

The four portfolios are floored separately, so in the same year Low CCC can be invested while Mid CCC holds cash. The floor reads entry prices only, so it's decided before any exit is known.

## Results (2000-2025)

Low CCC beat its local index in 10 of the 14 markets that have one. That isn't much evidence for CCC: in 6 of those 10 (Germany, China, Japan, UK, Sweden and Taiwan), Mid CCC and High CCC beat the index too, and Mid CCC beat its index in 11 of 14. Across all 15 markets Mid CCC returned more than Low CCC in 10, and Low minus High CCC was positive in 7 and negative in 8. The relationship between working capital efficiency and returns isn't monotonic. In the US, Low CCC returned 6.09% a year against 7.33% for the S&P 500, Mid CCC 7.92% and High CCC 6.34%. The signal may work better as a secondary filter combined with quality or value factors.

Low CCC by market, sorted by excess return. South Africa is shown but not counted: there's no local index series, so it runs against the S&P 500 in USD.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Excess, invested years (pp) | Sharpe | Max drawdown | Benchmark max drawdown | Cash years | Avg stocks |
|---|---|---|---|---|---|---|---|---|---|---|
| Sweden (STO) | OMX Stockholm 30 | 8.67% | 2.40% | +6.27 | +2.17 | 0.275 | -38.0% | -64.9% | 3 of 25 | 25 |
| Canada (TSX, TSXV) | TSX Composite | 7.83% | 4.05% | +3.78 | +3.78 | 0.254 | -34.5% | -32.9% | 0 of 25 | 108 |
| Japan (JPX) | Nikkei 225 | 5.42% | 2.20% | +3.22 | +3.22 | 0.237 | -46.0% | -61.1% | 0 of 25 | 107 |
| Switzerland (SIX) | SMI | 4.50% | 2.08% | +2.42 | +1.80 | 0.235 | -38.0% | -43.2% | 5 of 25 | 14 |
| UK (LSE) | FTSE 100 | 3.22% | 1.15% | +2.07 | +2.06 | -0.013 | -48.9% | -41.9% | 0 of 25 | 68 |
| Hong Kong (HKSE) | Hang Seng | 2.81% | 1.28% | +1.53 | +1.54 | -0.007 | -41.0% | -49.1% | 0 of 25 | 151 |
| Germany (XETRA) | DAX | 5.75% | 4.51% | +1.24 | -1.65 | 0.159 | -50.2% | -65.1% | 4 of 25 | 32 |
| China (SHZ, SHH) | SSE Composite | 3.71% | 2.51% | +1.20 | +1.20 | 0.036 | -54.2% | -43.4% | 0 of 25 | 459 |
| Singapore (SES) | STI | 3.84% | 2.65% | +1.19 | +1.75 | 0.057 | -45.0% | -44.6% | 5 of 25 | 18 |
| Taiwan (TAI, TWO) | TAIEX | 4.13% | 3.05% | +1.08 | +0.11 | 0.186 | -29.1% | -57.1% | 7 of 25 | 54 |
| US (NYSE, NASDAQ, AMEX) | S&P 500 | 6.09% | 7.33% | -1.24 | -1.23 | 0.172 | -37.4% | -39.3% | 0 of 25 | 487 |
| India (NSE) | Sensex | 8.75% | 11.49% | -2.74 | -1.98 | 0.065 | -47.4% | -38.3% | 5 of 25 | 90 |
| Brazil (SAO) | Bovespa | 5.68% | 8.58% | -2.90 | +0.92 | -0.205 | -38.6% | -31.0% | 6 of 25 | 38 |
| Korea (KSC) | KOSPI | 0.40% | 4.44% | -4.04 | -1.58 | -0.168 | -33.5% | -40.0% | 8 of 25 | 39 |
| South Africa (JSE) | S&P 500 (USD, not counted) | -1.16% | 7.33% | -8.49 | -10.72 | -0.533 | -62.3% | -39.3% | 14 of 25 | 13 |

Returns are in local currency. A cash year earns 0% and still counts in the CAGR and drawdown. Avg stocks is the average book size in invested years. "Excess, invested years" compounds the portfolio and its benchmark over only the years the portfolio was invested, from the `annual_returns` in each results file, and takes the difference in CAGR. Those are rounded to 2 decimals, so for a market with no cash years it can differ from Excess by 0.01.

Read the excess column next to that one. Outside South Africa, every Low CCC cash year falls between 2000 and 2007, so the floor's effect depends on what the index did then. Germany, Sweden and Taiwan held cash while their indexes fell (the DAX lost 46.1% over Low CCC's four cash years, the OMX Stockholm 30 64.9% over three, TAIEX 21.5% over seven), and that is most or all of their excess: over the years it was invested, Low CCC trailed the DAX by 1.65 points a year and beat TAIEX by 0.11. Brazil, Korea and India went the other way: the Bovespa gained 131.5% over Brazil's six cash years, the KOSPI 105.8% over Korea's eight and the Sensex 30.7% over India's five. Brazil's Low CCC beat the Bovespa by 0.92 points a year when invested and trails it by 2.90 over the full period. Cash also damps drawdowns: TAIEX's -57.1% came in 2000-2002, while Taiwan's Low CCC was in cash. From 2007, when it was invested, the index's worst drawdown was -36.4% against Low CCC's -29.1%.

All four portfolios, CAGR with cash years in brackets:

| Market | Benchmark CAGR | Low CCC | Mid CCC | High CCC | Low + Decreasing | Low minus High (pp) |
|---|---|---|---|---|---|---|
| US (NYSE, NASDAQ, AMEX) | 7.33% | 6.09% | 7.92% | 6.34% | 5.84% | -0.25 |
| India (NSE) | 11.49% | 8.75% (5) | 12.44% (5) | 8.74% (5) | 5.70% (6) | +0.01 |
| Germany (XETRA) | 4.51% | 5.75% (4) | 5.79% (5) | 9.67% (3) | 3.35% (6) | -3.92 |
| China (SHZ, SHH) | 2.51% | 3.71% | 4.08% | 3.94% | 3.78% | -0.23 |
| Hong Kong (HKSE) | 1.28% | 2.81% | 2.20% (1) | -1.54% (4) | 3.10% (2) | +4.35 |
| Japan (JPX) | 2.20% | 5.42% | 5.57% | 6.23% | 4.05% (1) | -0.81 |
| Canada (TSX, TSXV) | 4.05% | 7.83% | 5.66% | 3.91% (4) | 8.05% | +3.92 |
| UK (LSE) | 1.15% | 3.22% | 8.42% | 5.48% | 4.36% | -2.26 |
| Switzerland (SIX) | 2.08% | 4.50% (5) | 0.63% (9) | 8.60% (3) | -0.80% (17) * | -4.10 |
| Sweden (STO) | 2.40% | 8.67% (3) | 6.27% (4) | 8.29% (4) | 1.48% (10) | +0.38 |
| Korea (KSC) | 4.44% | 0.40% (8) | 4.66% (7) | 1.05% (8) | 1.53% (9) | -0.65 |
| Brazil (SAO) | 8.58% | 5.68% (6) | 6.29% (5) | 4.44% (7) | 3.92% (8) | +1.24 |
| Taiwan (TAI, TWO) | 3.05% | 4.13% (7) | 6.61% (5) | 3.55% (7) | 3.27% (8) | +0.58 |
| Singapore (SES) | 2.65% | 3.84% (5) | 0.56% (23) * | 0.29% (12) | -2.36% (17) * | +3.55 |
| South Africa (JSE) | 7.33% (S&P 500, USD) | -1.16% (14) * | 2.33% (17) * | 0.51% (16) * | -0.62% (22) * | -1.67 |

\* Too thin to test: cash in 13 or more of the 25 years. Singapore's Mid CCC portfolio was invested in 2 years, its Low + Decreasing portfolio in 8, Switzerland's Low + Decreasing in 8, and South Africa's four portfolios in 3 to 11. Those CAGRs mostly measure the cash. Singapore's High CCC (12 cash years) and Sweden's Low + Decreasing (10) are close to that line.

Beating its own index, out of 14: Low CCC 10, Mid CCC 11, High CCC 7, Low + Decreasing 6. Low CCC's Sharpe ratio beat the benchmark's in 10 of 14 markets and its maximum drawdown was shallower in 8.

Cash years by market (the April rebalance that went to cash). The US, China and the UK were invested in all 25 years in every portfolio.

| Market | Low CCC | Mid CCC | High CCC | Low + Decreasing |
|---|---|---|---|---|
| India (NSE) | 2000-2004 | 2000-2004 | 2000-2004 | 2000-2005 |
| Germany (XETRA) | 2000-2003 | 2000-2004 | 2000-2002 | 2000-2005 |
| Hong Kong (HKSE) | - | 2000 | 2000-2003 | 2000-2001 |
| Japan (JPX) | - | - | - | 2000 |
| Canada (TSX, TSXV) | - | - | 2000, 2002-2004 | - |
| Switzerland (SIX) | 2000-2004 | 2000-2005, 2009-2010, 2012 | 2000-2002 | 2000-2005, 2007, 2009, 2011-2015, 2017-2018, 2020, 2023 |
| Sweden (STO) | 2000-2002 | 2000-2002, 2009 | 2000-2003 | 2000-2006, 2008-2009, 2014 |
| Korea (KSC) | 2000-2007 | 2000-2006 | 2000-2007 | 2000-2008 |
| Brazil (SAO) | 2000-2005 | 2000-2004 | 2000-2006 | 2000-2006, 2010 |
| Taiwan (TAI, TWO) | 2000-2006 | 2000-2004 | 2000-2006 | 2000-2007 |
| Singapore (SES) | 2000-2004 | 2000-2022 | 2000-2011 | 2000-2007, 2009, 2012-2016, 2018-2020 |
| South Africa (JSE) | 2000-2006, 2009-2012, 2014, 2021-2022 | 2000-2012, 2014-2015, 2021, 2023 | 2000-2013, 2016, 2022 | 2000-2015, 2019-2024 |

## Rerun October 2026

These results replace the previously published set. Four things changed. The first three are described in [METHODOLOGY.md](../METHODOLOGY.md):

- **Last-traded-price exits.** A holding with no price in the 10-day window after the exit date now exits at its last traded price instead of being dropped from the period. Across the 15 markets, 822 holdings in the Low, Mid and High CCC portfolios exited this way (Low + Decreasing is a subset of Low CCC), and 7 were still left without an exit price and dropped: 6 in the US and 1 in Canada. See [Survivorship Bias](../METHODOLOGY.md#survivorship-bias).
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only. See [Universe Construction](../METHODOLOGY.md#universe-construction).
- **Same-ISIN dedup.** On every market, a company listed under more than one ticker is held once, before the CCC buckets are formed.
- **10-name minimum per portfolio.** New for this topic. Before it, a portfolio held whatever passed the screen, down to a single name: Brazil's Mid and High CCC books and South Africa's four books all held one stock in some early years. The listing rules made this worse in Germany and Switzerland, where some early portfolios shrank to 1 to 4 names, with drawdowns as deep as -91.2% (Switzerland, Mid CCC). Now a portfolio with fewer than 10 names buyable at entry holds cash for that year.

Most of the change comes from the minimum. Low CCC beats its index in 10 of 14 markets, up from 8: Taiwan (-1.35 to +1.08 points) and Singapore (-0.94 to +1.19) moved to beating it. South Africa, which isn't counted, fell from +1.94 to -8.49 against the S&P 500. In Brazil, Mid and High CCC fell from 13.33% and 16.11% to 6.29% and 4.44% and now trail the Bovespa, and Low minus High CCC went from -8.47 to +1.24 points. Germany's Low CCC fell from 7.51% to 5.75% and still beats the DAX, but only because of its 2000-2003 cash years (see above). In the US, China and the UK no portfolio ever fell below 10 names, so the minimum changed nothing there and the other three changes account for every move: Low CCC moved from 5.94% to 6.09% in the US, 3.74% to 3.71% in China and 4.49% to 3.22% in the UK.

## Usage

Run from the repository root.

```bash
# One market: writes the per-market file the charts read
python3 cash-conversion/backtest.py --preset india --output cash-conversion/results/ccc_metrics_India.json

# All 15 published markets, one run each (preset:file name)
for pair in us:US_MAJOR india:India germany:XETRA china:China hongkong:HKSE japan:JPX canada:Canada \
            uk:LSE switzerland:SIX sweden:STO korea:KSC brazil:SAO taiwan:Taiwan singapore:SES southafrica:JSE; do
  python3 cash-conversion/backtest.py --preset "${pair%%:*}" --output "cash-conversion/results/ccc_metrics_${pair##*:}.json"
done

# Screen current low-CCC stocks (live TTM data)
python3 cash-conversion/screen.py
python3 cash-conversion/screen.py --preset india

# Generate charts from the per-market results files
python3 cash-conversion/generate_charts.py
```

- `--exit-method` defaults to `ltp` (last traded price). `--exit-method drop` drops a holding with no exit price instead.
- The listing rules apply automatically on the germany, switzerland and uk presets. There's nothing to pass.
- The presets define the published universes: canada is TSX + TSXV, taiwan is TAI + TWO, singapore is SES.
- `--global --output <file>` runs these 15 presets plus France in one process and writes only that one combined file, keyed by preset name (`NSE`, not `India`). It doesn't write the `ccc_metrics_*.json` files the charts read. France has too little CCC data and records `"No valid periods"`.
- `results/exchange_comparison.json` holds the 15 per-market results under their file names, plus France's error entry.
- Each run reads the live data warehouse, so a rerun can differ slightly from the committed results.

## Files

- `backtest.py` - Full historical backtest. Fetches data via API, caches in DuckDB, runs locally.
- `screen.py` - Live screen for low-CCC stocks using current TTM data. Single API call.
- `generate_charts.py` - Generates all charts from `results/ccc_metrics_*.json`. Cash years show as flat lines and labelled gaps.
- `results/` - Backtest results: one `ccc_metrics_<market>.json` per market, plus `exchange_comparison.json`.
- `charts/` - PNG charts (cumulative growth, annual returns, cross-exchange comparison).
