# Dividend Growth (Aristocrats) Backtest

Companies that have raised their dividend every year for at least five years, pay out less than 80% of earnings and generate positive free cash flow. Each July the screen buys up to 30 of them on a market, longest streak first, at equal weight.

## Signal

| Filter | Threshold | Source |
|--------|-----------|--------|
| Consecutive years of dividend increases, ending in the last complete calendar year | 5 or more | `dividend_calendar` (`adjDividend` summed by the calendar year of the ex-date) |
| Dividend payout ratio | 0% to 80% | `financial_ratios` (FY) |
| Free cash flow | Above 0 | `cash_flow_statement` (FY) |
| Market cap | Above the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`), in local currency ($1B in the US, ₹20B on NSE, ¥100B on JPX, €500M on XETRA) | `key_metrics` (FY) |

**Ranking:** longest streak first, then largest market cap, then symbol. Top 30.

**The streak.** Dividends are summed per calendar year. A year counts as an increase only if its total is above the total of the calendar year immediately before it. The run of increases must be at least 5 years long and must end in the last complete calendar year before the rebalance (2020 for the July 2021 rebalance). A year with no dividend, a cut, a flat year or a missing year breaks it.

The payout ratio, free cash flow and market cap each come from the company's latest fiscal-year row whose period ended at least 45 days before the rebalance date.

## Parameters

- **Rebalancing:** Annual, on July 1, executed at the next available close. 25 periods, July 2000 to July 2025.
- **Min stocks:** 10. The year is held in cash if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price. A cash year earns 0% and counts in the CAGR. The floor is checked at entry only. A name whose return is above the 200% single-stock cap is left out of that year's average and not replaced, so an invested year can average fewer names than were bought: India's 2020 book had 18 buyable names and averaged 15.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted.
- **Data filters:** An entry price below 1.0 in the exchange's price unit is unusable (pence on LSE). A single-stock return above 200% in a year is dropped as a likely data error.
- **Exits:** A holding with no price after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. South Africa has no local index series and is measured against the S&P 500 in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 3.5% UK).

## Results (July 2000 to July 2025)

Of the 13 markets in the comparison file, six were invested in at least half of the 25 years. Five of them beat their index: the UK, Canada, Hong Kong, India and Japan, all against price indices. The US is a tie: 0.21 points a year ahead of SPY, which includes dividends. Six more were invested in fewer than half the years and are too thin to test (China, Sweden, Germany, Taiwan, Switzerland, Korea). South Africa never invested. Australia is run and its file is committed, but it is not in the comparison file or the charts.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max drawdown | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------------|------------|------------|
| UK (LSE, pence lines) | FTSE 100 | 7.88% | 1.23% | +6.66 | 0.278 | -37.28% | 0 of 25 | 19.9 |
| Canada (TSX) | TSX Composite | 8.33% | 3.95% | +4.38 | 0.428 | -20.19% | 5 of 25 | 23.6 |
| Hong Kong (HKSE) | Hang Seng | 5.13% | 1.64% | +3.49 | 0.157 | -34.48% | 9 of 25 | 20.9 |
| India (NSE) | Sensex | 13.70% | 12.06% | +1.64 | 0.354 | -3.99% | 8 of 25 | 19.4 |
| Australia (ASX)\*\*\* | ASX 200 | 4.90% | 3.89% | +1.01 | 0.109 | -23.91% | 10 of 25 | 18.4 |
| Japan (JPX) | Nikkei 225 | 3.82% | 3.31% | +0.51 | 0.273 | -33.37% | 8 of 25 | 25.0 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 8.06% | 7.85% | +0.21 | 0.446 | -34.69% | 0 of 25 | 29.7 |
| China (SHZ + SHH)\* | SSE Composite | 3.37% | 2.43% | +0.94 | 0.047 | -11.94% | 14 of 25 | 22.9 |
| Sweden (STO)\* | OMX Stockholm 30 | 2.22% | 2.55% | -0.34 | 0.016 | -30.42% | 15 of 25 | 16.5 |
| Germany (XETRA, German companies)\* | DAX | 0.95% | 5.04% | -4.09 | -0.121 | -25.09% | 17 of 25 | 21.5 |
| Taiwan (TAI)\* | TAIEX | 2.43% | 4.09% | -1.65 | 0.132 | -18.62% | 19 of 25 | 12.0 |
| Switzerland (SIX, Swiss companies)\* | SMI | 1.17% | 1.74% | -0.58 | 0.114 | -2.27% | 20 of 25 | 12.6 |
| Korea (KSC)\* | KOSPI | 1.76% | 5.35% | -3.59 | -0.167 | 0.00% | 23 of 25 | 11.5 |
| South Africa (JNB)\*\* | S&P 500 (USD) | 0.00% | 7.85% | -7.85 | n/a | 0.00% | 25 of 25 | 0 |

\* Too thin to test: in cash for more than half of the 25 years. China was invested in 11 years, Sweden in 10, Germany in 8, Taiwan in 6, Switzerland in 5 and Korea in 2, so these CAGRs, excess figures, Sharpe ratios and drawdowns mostly measure the cash. They are not results for the screen.

\*\* Never invested: fewer than 10 names could be bought in every one of the 25 years. The 0.00% is 25 years of cash, and the benchmark is in another currency.

\*\*\* Australia is rerun with the other markets, but it is left out of `exchange_comparison.json` and the charts. ASX prices carry known `adjClose` split-adjustment errors (see [DATA_QUALITY_ISSUES.md](../DATA_QUALITY_ISSUES.md), ASX), so read this row as unverified.

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Avg stocks is the average book in invested years.

- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). The S&P 500 (through SPY) and the DAX include them too, so the US, Germany and South Africa rows are like for like on that point. The other eleven indices are price indices, so excess against them is overstated by roughly the index's dividend yield. Five of the six positive results are against price indices. Japan's +0.51 and India's +1.64 are small enough that they may not survive a total-return comparison. See [METHODOLOGY.md](../METHODOLOGY.md#benchmark-limitations) (Benchmark Limitations).
- **The US:** +0.21 points a year against a benchmark that includes dividends, over 25 annual observations. Treat it as a tie.
- **Drawdowns** are measured on the 25 annual returns, so a fall inside a year doesn't show. India's -3.99% is one year, 2011; the book was in cash until July 2008 and lost money in only one other year (2024, -0.37%). The UK's -37.28% is the holding year from July 2007, when the FTSE 100 lost 17.67%; the year after was flat (-0.01%).
- **Sharpe:** Germany's and Korea's are negative at a positive CAGR because cash years earn 0% against a risk-free rate of 2.0% and 3.0%.
- **India** has the highest CAGR, 13.70%, on 17 invested years. It sat in cash from 2000 to 2007, while the Sensex returned 12.06% a year over the whole period.

### Cash years

A year is in cash for one of two reasons: fewer than 10 names passed the screen, or 10 or more passed but fewer than 10 had a usable entry price. Each results file records both counts for every year (`screened`, `entry_buyable`). Years are the July each holding period starts.

| Market | Fewer than 10 passed the screen | 10 or more passed, fewer than 10 buyable (buyable of screened) |
|--------|--------------------------------|------------------------------------------|
| South Africa (JNB) | 19: 2000-2014, 2021-2024 | 6: 2015 (8 of 15), 2016 (7 of 13), 2017 (8 of 15), 2018 (8 of 13), 2019 (8 of 14), 2020 (8 of 10) |
| Korea (KSC) | 23: 2000-2018, 2020-2021, 2023-2024 | 0 |
| Switzerland (SIX, Swiss companies) | 13: 2000-2008, 2011-2012, 2014, 2017 | 7: 2009 (9 of 13), 2010 (7 of 12), 2013 (6 of 10), 2015 (8 of 12), 2016 (8 of 12), 2018 (6 of 10), 2021 (9 of 13) |
| Taiwan (TAI) | 18: 2000-2006, 2009-2015, 2017, 2021-2022, 2024 | 1: 2016 (9 of 10) |
| Germany (XETRA, German companies) | 16: 2000-2015 | 1: 2024 (8 of 10) |
| Sweden (STO) | 11: 2000-2005, 2010-2014 | 4: 2006 (6 of 10), 2008 (7 of 10), 2009 (7 of 10), 2023 (9 of 12) |
| China (SHZ + SHH) | 14: 2000-2013 | 0 |
| Australia (ASX) | 10: 2000-2006, 2012-2014 | 0 |
| Hong Kong (HKSE) | 8: 2000-2007 | 1: 2008 (8 of 10) |
| India (NSE) | 8: 2000-2007 | 0 |
| Japan (JPX) | 8: 2000-2006, 2012 | 0 |
| Canada (TSX) | 5: 2000-2004 | 0 |

The US and the UK were invested in all 25 years.

### What the books hold

- **Two share lines of one issuer.** Share classes and preference shares carry their own ISINs, so the same-ISIN rule keeps both, and a pair takes two of the 30 slots. Canada holds CCL-A.TO with CCL-B.TO in 17 of its 20 invested years. Germany holds FPE.DE with FPE3.DE in all 8 of its invested years, and HEN.DE with HEN3.DE in 5. Switzerland holds RO.SW with ROG.SW in all 5. Sweden holds an A and B pair (SCA, INVE, KINV, MTG, HOLM or SWEC) in 8 of its 10 invested years. Korea holds an ordinary and preference pair in both of its invested years.
- **Small books.** 18 invested years across 9 markets held 10 or 11 names, at the floor or one above it: Taiwan in 4 of its 6 invested years, Hong Kong in 2010 to 2012, India in 2008 and 2012.
- **Hong Kong** has no listing rule, so its book takes every line on HKSE that passes, whatever the company's home market.

## Rerun October 2026

These results replace the set published in June 2026. Seven things changed between the two runs. The listing rules, the dedup, the exit rule and the streak rule are described in [METHODOLOGY.md](../METHODOLOGY.md) (Universe Construction; Filing and Selection Rules; Limitations: Survivorship Bias).

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the year. 10 holdings in 7 markets exited this way, and none was left without an exit price; each results file lists them under its `ltp` key. On the same selection this moves CAGR by between -0.14 points (Australia) and +0.21 points (China). China's +0.21 is one holding, 000786.SZ in the 2014 year. 4 of the 10 have no delisting record (MBB.DE, HBLPOWER.NS, 000786.SZ, 4338.HK): the line stopped printing prices before the exit date, so a filled exit is not always a failed company.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market, before the top-30 cut.
- **The streak now ends at the last complete year.** Before, the streak counted increases since the last recorded cut and skipped missing years, so a company that had stopped paying kept its streak: 13 of Sweden's top 30 in July 2021 had paid nothing in 2020. Now a year with no dividend, a cut, a flat year or a gap breaks it, and the run must end in the last complete calendar year.
- **Entry-side cash rule.** The June results went to cash only when fewer than 10 names passed the screen, and otherwise averaged however many of them had prices. A year is now also cash when fewer than 10 of the screened names have a usable entry price. 20 of the 173 cash years in the 14 files are such years (Switzerland 7, South Africa 6, Sweden 4, Germany 1, Hong Kong 1, Taiwan 1).
- **Price-filter fix and price window.** The June run's price filter could delete valid prices. `remove_price_oscillations` compared each price row with the rows before and after it without checking that they were close in time. This backtest fetches prices only for the days after each July rebalance, so the row before the first price of a year was a year old, and a move of 30% or more between one July and the next could look like a one-day spike. Since commit `b815bb7` (21 August 2026) the filter only compares rows within 10 days of each other. The price window after each rebalance date also grew from 10 days to 11. No run isolates these two changes, so their effect on this topic is not measured. They can account for part of the difference in any invested year, in how many names each run could price, and in benchmark prices, which pass through the same filter.
- **Deterministic selection.** Ties at the top-30 cut break on the symbol, and date bounds are built at UTC midnight (see [Reproducibility](../METHODOLOGY.md#reproducibility)).

The large moves:

- **Germany** fell from 5.96% to 0.95% and is now too thin to test. Cash years rose from 7 to 17. With Germany-domiciled companies only, fewer than 10 names pass the screen in every year from 2000 to 2015; in June the book was invested from 2007. In 2024, 10 names pass and 8 can be bought.
- **Switzerland** fell from 3.55% to 1.17% and is now too thin to test. Cash years rose from 7 to 20. Fewer than 10 Swiss companies pass in 13 years, and in 7 more 10 to 13 pass but only 6 to 9 can be bought. The June book averaged 10.2 names when invested.
- **Sweden** fell from 4.54% to 2.22% and is now too thin to test. Cash years rose from 11 to 15. Sweden has no listing rule: 2006, 2008 and 2009 pass 10 names and can buy only 6 or 7 (the entry-side rule), and 2023 passes 12 and can buy 9. A replay of the screen made before the rerun, with the old streak against the new one and everything else equal, found the streak rule alone takes 2023 from 15 buyable names to 9, Germany's 2024 from 13 to 8 and Switzerland's 2021 from 11 to 9, and moves no other year into or out of cash.
- **South Africa** went from -0.98% to 0.00%. Its six invested years in June (2015 to 2020) averaged 8.2 names, under the 10-name floor. Under the entry-side rule those years have 7 or 8 buyable names and are cash, so the book never invests.
- **Hong Kong** rose from 4.15% to 5.13% and **Taiwan** fell from 3.27% to 2.43%, one year each. Hong Kong's 2008 (-19.95% in June) and Taiwan's 2016 (+22.53% in June) are now cash under the entry-side rule, with 8 and 9 buyable names of 10 screened.
- The other eight markets moved by less than 0.3 points: China +0.28, Australia -0.25, the UK -0.14, India -0.08, the US +0.06, Canada +0.06, Japan +0.05 and Korea +0.01. None has a new cash year, so their moves sit in invested years; the split between the streak rule, the dedup, the tie-breaks, the price-filter fix, the price window and revised data is not determined.
- **Benchmarks.** Benchmark CAGRs are unchanged at two decimals in all 14 markets. Single benchmark years differ in five (India, Sweden, Japan, China, Korea), in offsetting groups, all between 2000 and 2007: the Sensex's 2000 year reads -29.29% where June had -31.63%, and its 2001 year -4.12% where June had -0.82%. That pattern is what a different index bar at a rebalance date gives. The price-filter fix is a possible cause, not a confirmed one.

In June, 10 of the 13 markets had a positive excess. One of them, China, was already in cash for 14 of the 25 years. Three more, Germany (+0.92), Switzerland (+1.81) and Sweden (+1.98), are now too thin to test, and their raw excess is negative.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market.

```bash
# US (default universe: NYSE + NASDAQ + AMEX)
python3 dividend-growth/backtest.py --verbose

# One market, written to the committed results file
python3 dividend-growth/backtest.py --exchange XETRA --output dividend-growth/results/returns_XETRA.json

# After any market is rerun, rebuild the combined file the charts read, then check it
python3 scripts/rebuild_comparison.py dividend-growth --keep NYSE_NASDAQ_AMEX NSE TSX LSE XETRA SIX STO HKSE JPX TAI SHZ_SHH KSC JNB
python3 scripts/rebuild_comparison.py dividend-growth --keep NYSE_NASDAQ_AMEX NSE TSX LSE XETRA SIX STO HKSE JPX TAI SHZ_SHH KSC JNB --check

# Charts from results/exchange_comparison.json
python3 dividend-growth/generate_charts.py

# The old exit rule (doesn't reproduce the June results: the listing rules, dedup, streak rule, cash rule and price-filter fix still apply)
python3 dividend-growth/backtest.py --exchange LSE --exit-method drop

# Without transaction costs
python3 dividend-growth/backtest.py --no-costs

# Current screen
python3 dividend-growth/screen.py
python3 dividend-growth/screen.py --preset india
```

The committed files were made one market at a time, each with `--exchange <codes> --output dividend-growth/results/<file>`:

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| US | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `NSE` | `returns_NSE.json` | |
| Canada | `TSX` | `returns_TSX.json` | |
| UK | `LSE` | `returns_LSE.json` | GBp lines only |
| Hong Kong | `HKSE` | `returns_HKSE.json` | |
| Japan | `JPX` | `returns_JPX.json` | |
| Australia | `ASX` | `returns_ASX.json` | |
| China | `SHZ,SHH` | `returns_SHZ_SHH.json` | |
| Sweden | `STO` | `returns_STO.json` | |
| Germany | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Taiwan | `TAI` | `returns_TAI.json` | |
| Switzerland | `SIX` | `returns_SIX.json` | Switzerland-domiciled only |
| Korea | `KSC` | `returns_KSC.json` | |
| South Africa | `JNB` | `returns_JNB.json` | |

Use `--exchange` as shown. `--preset canada` adds TSXV and `--preset taiwan` adds TWO, and the presets write other universe labels. `--global --output dividend-growth/results/exchange_comparison.json` runs the same 14 exchange sets and writes the same per-market file names, but its combined file has 14 keys (Australia included) in another order, and if one market fails it leaves that market's old file in place and records an error in the combined file. Rebuild with `--keep` afterwards. France (PAR) isn't run: its last file, from March 2026, was 25 years of cash and was removed. Each run reads the live data warehouse, so a rerun can differ slightly from the committed results.

`generate_charts.py` writes cumulative growth and year-by-year returns for each market in `exchange_comparison.json` that invested at least once, plus `3_comparison_cagr.png`. A market invested in fewer than half its years carries a "too thin to test" line on its charts and is drawn hatched, below the ranked markets, in the comparison. The comparison has no colour for a tie: every ranked market ahead of its index is drawn green as "Beat its benchmark", the US (+0.21) included. Chart files aren't committed, so run it locally.

## Data Requirements

- `dividend_calendar`: symbol, date (ex-date), adjDividend
- `financial_ratios` (FY): dividendPayoutRatio
- `cash_flow_statement` (FY): freeCashFlow
- `key_metrics` (FY): marketCap
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup)

## Known Limits

- The streak uses calendar years of ex-dates. A dividend cancelled in the spring of a year is seen only at the following July's rebalance, so the name is held one more year. An ex-date that moves across the year end reads as a missing year followed by a double year, and breaks the streak.
- The filing date used is the fiscal period end, so the 45-day lag counts from the period end. A fiscal year that ended 45 to 90 days before a rebalance can be used before the annual report was public.
- The payout, free cash flow and market cap rows are picked separately and can come from different fiscal years. There is no maximum filing age.
- The size floor and the market-cap tie-break compare the filing's market cap, in the company's reporting currency, with a threshold set in the listing currency.
- With the same ISIN on two lines, the dedup keeps the line that is buyable, then actively trading, then in the home currency, then first by symbol, not the line with the longer streak.
- `screen.py` (the current-day screen) still uses the old streak: it counts increases since the last recorded cut, skips missing years and doesn't require the run to reach the last complete year. It also lists every line on the exchange, without the listing rules or the dedup.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen (TTM data), with the old streak and no listing rules or dedup (see Known Limits) |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` (13 markets) |
| `charts/` | Output of `generate_charts.py` (not committed) |

## Academic References

- Lintner, J. (1956). "Distribution of Incomes of Corporations Among Dividends, Retained Earnings, and Taxes." *American Economic Review*, 46(2), 97-113.
- Arnott, R. & Asness, C. (2003). "Surprise! Higher Dividends = Higher Earnings Growth." *Financial Analysts Journal*, 59(1), 70-87.
- Siegel, J. (2005). *The Future for Investors.* Crown Business.
