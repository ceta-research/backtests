# FCF Yield Value Strategy

Buy the companies with the highest free cash flow yield among those that also clear three quality bars. FCF yield (free cash flow / market cap) measures the cash a business has left after capital spending, which earnings and EBITDA don't.

## Academic Basis

Gray & Vogel (2012), "Analyzing Valuation Measures: A Performance Horse Race Over the Past 40 Years", *Journal of Portfolio Management* 39(1), 112-121. They compared valuation metrics on US stocks from 1971 to 2010; FCF/TEV came second at 16.6% a year, behind EBITDA/TEV at 17.7%. Novy-Marx (2013) found that combining value with quality improves results, which is why the screen adds the ROE and margin filters.

## Signal

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| FCF Yield | > 8% and < 50% | Cheap on cash; the cap removes data artifacts |
| Return on Equity | > 10% | Earns a real return on capital |
| Interest Coverage | > 3x | Can service its debt |
| Operating Profit Margin | > 10% | Profit comes from operations |
| Market Cap | above the market's floor (table under Usage) | Liquid, investable |

**Sorting:** Highest FCF yield first. Top 30.

Every filter is tested on the company's latest fiscal-year row dated at least 45 days before the rebalance. There is no maximum filing age, so a company that stopped filing can keep qualifying on its last row.

## Parameters

- **Rebalancing:** Annual, on July 1, with entry and exit at the first close after the rebalance date. 25 periods, July 2000 to July 2025.
- **Min stocks:** 10. The portfolio holds cash for the year if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price. A cash year earns 0%. The floor is checked at entry only. A name that fails a data check at exit (a return above the 200% single-stock cap, or no exit price at all) is left out of that year's average and not replaced.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted, so on NSE, JPX, KSC, TAI/TWO and SET, where the market-cap floor is at least 10 billion local units, every holding pays the 0.1% tier.
- **Data lag:** 45 days, counted from the fiscal year end (the warehouse row's date), not from the filing date.
- **Data guards:** FCF yield capped at 50%, entry price of at least 1.00 in local units, and single-stock returns above 200% for the year left out as price artifacts.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes have their own ISINs, so a company's ordinary and preference shares can both be held.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. Saudi Arabia and Israel have no local index configured and are measured against the S&P 500 in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (table under Usage). Market-cap floors come from `MKTCAP_THRESHOLD_MAP` in the same file.

## Results (July 2000 to July 2025)

16 markets were run. 9 were invested in at least half of the 25 annual periods. 6 of those 9 beat their own index (UK, Hong Kong, US, China, Japan, Canada) and 3 trailed (Taiwan, India, Thailand). The other 7 sat in cash for more than half the periods and are too thin to test; they are listed separately below and are not results.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 11.27% | 7.85% | +3.42 | 0.399 | -50.75% | 0 | 21.9 |
| India (NSE) | Sensex | 9.52% | 12.06% | -2.53 | 0.130 | -16.86% | 12 (2000-2009, 2011, 2013) | 19.8 |
| UK (LSE) | FTSE 100 | 6.71% | 1.23% | +5.48 | 0.200 | -30.06% | 8 (2000-2007) | 18.6 |
| Hong Kong (HKSE) | Hang Seng | 6.12% | 1.64% | +4.48 | 0.150 | -21.46% | 7 (2000-2005, 2007) | 21.3 |
| Japan (JPX) | Nikkei 225 | 6.05% | 3.31% | +2.74 | 0.301 | -28.60% | 8 (2000-2004, 2006-2007, 2009) | 18.4 |
| China (SHZ + SHH) | SSE Composite | 5.68% | 2.43% | +3.25 | 0.089 | -41.10% | 5 (2000-2002, 2004, 2008) | 22.5 |
| Canada (TSX + TSXV) | TSX Composite | 5.29% | 3.95% | +1.34 | 0.164 | -27.47% | 7 (2000-2004, 2013, 2015) | 16.6 |
| Taiwan (TAI + TWO) | TAIEX | 3.81% | 4.09% | -0.27 | 0.180 | -24.44% | 7 (2000-2006) | 21.3 |
| Thailand (SET) | SET Index | 1.63% | 5.13% | -3.50 | -0.051 | -53.88% | 9 (2000-2004, 2015-2016, 2018, 2020) | 13.8 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their July rebalance and earn 0%. Avg stocks is the average number held in invested years.

- **US:** 11.27% a year against 7.85% for SPY, with a Sharpe of 0.399 against 0.352 and a max drawdown of -50.75% against -38.01%. It's the only market invested in all 25 years, and SPY includes dividends, so it's the one like-for-like comparison among the 9.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). The other 5 beats (UK +5.48, Hong Kong +4.48, China +3.25, Japan +2.74, Canada +1.34) are against price indices that leave dividends out, so each margin is overstated by roughly the index's dividend yield. Canada's +1.34 is the thinnest and may not survive a total-return comparison. See METHODOLOGY.md (Benchmark Limitations).
- **Taiwan:** trails the TAIEX by 0.27 points (3.81% against 4.09%), just outside the 0.2-point band we read as a tie, and against a price index.
- **India:** 9.52% a year, number 2 of 9 on CAGR, and 2.53 points behind the Sensex (12.06%). It was invested in 13 of 25 years, one more than the too-thin line, so read it as a borderline result.
- **Drawdowns:** measured on annual returns. The deepest are Thailand -53.88%, US -50.75%, China -41.10%.
- **Cash years:** a year is in cash when fewer than 10 names pass the screen, or fewer than 10 of those have a usable entry price. Across these 9 markets that is 63 market-years: 48 on the screen count and 15 on the entry-price check (India 2007-2009; UK 2005-2007; Hong Kong 2005, 2007; Japan 2006; China 2004; Canada 2013, 2015; Thailand 2016, 2018, 2020).
- **Thin books:** the floor is checked at entry only, so an invested year can average as few as 10 names. Years averaging 10 or 11: India 2010, 2012; UK 2016; Hong Kong 2006; Japan 2010; Canada 2005, 2007, 2012; Taiwan 2018; Thailand 2006-2008, 2012, 2014, 2019.

### Too thin to test

A market invested in fewer than half of its 25 periods has no result here. These 7 were run and are listed for the record.

| Market | Listing rule | Years invested (of 25) | Which years | Avg stocks when invested | Most names passing the screen in one year |
|--------|--------------|------------------------|-------------|--------------------------|-------------------------------------------|
| Korea (KSC) | none | 10 | 2009-2011, 2016-2017, 2019, 2021-2024 | 14.5 | 28 |
| Germany (XETRA) | Germany-domiciled only | 3 | 2022-2024 | 13.7 | 19 |
| Israel (TLV) | none | 3 | 2022-2024 | 13.3 | 23 |
| Saudi Arabia (SAU) | none | 3 | 2015-2016, 2023 | 10.3 | 12 |
| Norway (OSL) | none | 2 | 2022-2023 | 11.5 | 13 |
| Sweden (STO) | none | 2 | 2022, 2024 | 14.5 | 20 |
| Switzerland (SIX) | Switzerland-domiciled only | 0 | none | n/a | 10 |

- **Switzerland** never invested: the most Switzerland-domiciled names to pass in a year was 10 (2010), and only 8 of them had a usable entry price.
- **Norway**'s benchmark (Oslo All Share) starts in 2013, so only 12 of its 25 periods can be measured against it.
- **Saudi Arabia and Israel** have no local index configured and are measured against the S&P 500 in USD.
- The results files of these markets still carry CAGR, Sharpe and excess fields. With two or three invested years averaged with twenty-odd zeros, those fields don't describe the strategy.

## Rerun October 2026

These results were rerun with three changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias):

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 18 holdings in 9 markets exited this way (US 4, Canada 3, UK 2, China 2, Germany 2, Korea 2, Hong Kong 1, Thailand 1, Sweden 1); each results file lists them under its `ltp` key.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market. Share classes have their own ISINs and can both be held.

This screen has no selection fix of its own in the rerun. The earlier results also predate the entry-price check on the 10-stock floor: they went to cash only when fewer than 10 names passed the screen, and averaged whatever had prices otherwise. That check and the listing rules drive the moves:

- **Germany** had 5 cash years and 9.05% a year in the earlier results, on every line listed on XETRA. With Germany-domiciled companies only, fewer than 10 pass the screen in 21 of the 25 years, and in 2011 10 passed but only 7 had a usable entry price. That leaves 3 invested years: too thin to test.
- **Switzerland** had 9 cash years and 4.30% a year in the earlier results, on every line listed on SIX. With Switzerland-domiciled companies only, fewer than 10 pass the screen in 24 of the 25 years, and in 2010 10 passed but only 8 had a usable entry price. That leaves 0 invested years: too thin to test.
- **UK** fell from 9.98% to 6.71% and its lead over the FTSE 100 from +8.75 to +5.48 points. Cash years rose from 3 (2000-2002) to 8 (2000-2007): only 3 and 5 pence-quoted names passed in 2003-2004, and in 2005-2007 11, 10 and 10 passed but only 7, 8 and 8 had a usable entry price.
- **India** fell from 12.38% to 9.52% and now trails the Sensex by 2.53 points, after leading it by 0.33. No listing rule applies. In 2007-2009, 10, 11 and 10 names passed the screen but only 9, 8 and 7 had a usable entry price, so those years are now cash; the earlier results had them at +8.46%, +9.33%, +55.03%.
- **The same entry-price check elsewhere:** Japan 7.83% to 6.05% (2006, earlier +52.19%); Canada 6.03% to 5.29% (2013, 2015, earlier +27.72%, -5.00%); Hong Kong 5.31% to 6.12% (2005, 2007, earlier +9.12%, -22.28%); China 4.45% to 5.68% (2004, earlier -21.53%); Thailand 3.16% to 1.63% (2016, 2018, 2020, earlier -1.08%, +4.12%, +38.38%). A year that moves to cash can help or hurt.
- **No cash year changed:** US 11.12% to 11.27%; Taiwan 3.80% to 3.81%.
- **Already thin, now thinner:** Korea 13 to 15 cash years; Sweden 20 to 23 cash years; Norway 23 cash years both times; Saudi Arabia 20 to 22 cash years; Israel 15 to 22 cash years.
- Benchmark CAGRs are unchanged on all 16 markets.

`backtest.py` still has the experimental `--domicile-filter` flag. On XETRA and SIX it now builds the same query as the default run, and no committed result uses it. No as-listed result for XETRA, SIX or LSE is committed either.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market, as every preset below does.

Each committed results file is one market run on its own:

```bash
# One market, written to the committed results file
python3 fcf-yield/backtest.py --preset uk --output fcf-yield/results/value-04-fcf-yield_uk.json

# All 16, one market at a time
for m in us uk japan canada hongkong china switzerland taiwan thailand india germany korea sweden norway saudi israel; do
  python3 fcf-yield/backtest.py --preset "$m" --output "fcf-yield/results/value-04-fcf-yield_$m.json"
done

# The comparison file the charts read: 10 markets, each node a copy of its per-market file
python3 scripts/rebuild_comparison.py fcf-yield --keep US_MAJOR LSE JPX Canada HKSE China SIX Taiwan SET NSE
python3 scripts/rebuild_comparison.py fcf-yield --keep US_MAJOR LSE JPX Canada HKSE China SIX Taiwan SET NSE --check

# Charts from results/exchange_comparison.json
python3 fcf-yield/generate_charts.py

# The old exit rule (doesn't reproduce old results: the listing rules, dedup and entry-price check still apply)
python3 fcf-yield/backtest.py --preset uk --exit-method drop

# Current screen
python3 fcf-yield/screen.py
python3 fcf-yield/screen.py --preset india
```

Don't use `--global` to reproduce the files. It runs the same 16 presets in one loop but writes a single combined file and no per-market files, and one market's error stops the loop before anything is written.

| Market | `--preset` | Exchanges | Market cap floor | Risk-free rate | Benchmark | Listing rule | Output file |
|--------|------------|-----------|------------------|----------------|-----------|--------------|-------------|
| US | `us` | NYSE, NASDAQ, AMEX | USD 1B | 2.0% | S&P 500 (SPY) | none | `value-04-fcf-yield_us.json` |
| UK | `uk` | LSE | GBP 500M | 3.5% | FTSE 100 | GBp lines only | `value-04-fcf-yield_uk.json` |
| Japan | `japan` | JPX | JPY 100B | 0.1% | Nikkei 225 | none | `value-04-fcf-yield_japan.json` |
| Canada | `canada` | TSX, TSXV | CAD 500M | 2.5% | TSX Composite | none | `value-04-fcf-yield_canada.json` |
| Hong Kong | `hongkong` | HKSE | HKD 2B | 3.0% | Hang Seng | none | `value-04-fcf-yield_hongkong.json` |
| China | `china` | SHZ, SHH | CNY 2B | 2.5% | SSE Composite | none | `value-04-fcf-yield_china.json` |
| Switzerland | `switzerland` | SIX | CHF 500M | 0.5% | SMI | Switzerland-domiciled only | `value-04-fcf-yield_switzerland.json` |
| Taiwan | `taiwan` | TAI, TWO | TWD 10B | 1.0% | TAIEX | none | `value-04-fcf-yield_taiwan.json` |
| Thailand | `thailand` | SET | THB 10B | 2.5% | SET Index | none | `value-04-fcf-yield_thailand.json` |
| India | `india` | NSE | INR 20B | 6.5% | Sensex | none | `value-04-fcf-yield_india.json` |
| Germany | `germany` | XETRA | EUR 500M | 2.0% | DAX | Germany-domiciled only | `value-04-fcf-yield_germany.json` |
| Korea | `korea` | KSC | KRW 500B | 3.0% | KOSPI | none | `value-04-fcf-yield_korea.json` |
| Sweden | `sweden` | STO | SEK 5B | 2.0% | OMX Stockholm 30 | none | `value-04-fcf-yield_sweden.json` |
| Norway | `norway` | OSL | NOK 5B | 3.0% | Oslo All Share | none | `value-04-fcf-yield_norway.json` |
| Saudi Arabia | `saudi` | SAU | SAR 1B | 3.5% | S&P 500 (SPY, in USD) | none | `value-04-fcf-yield_saudi.json` |
| Israel | `israel` | TLV | ILS 1B | 3.0% | S&P 500 (SPY, in USD) | none | `value-04-fcf-yield_israel.json` |

`exchange_comparison.json` holds the 9 markets of the results table plus Switzerland, which the comparison charts name as not ranked. Germany, Korea, Sweden, Norway, Saudi Arabia and Israel have per-market files only. The market-cap floor is compared with the filing's market cap in the company's reporting currency.

Not run: Brazil (SAO) and Australia (ASX) for split-adjustment errors in `adjClose`, South Africa (JNB) for a thin universe, France (PAR) for a gap in annual filings, and Singapore (SES) and Mexico (MEX), which were never part of this study.

## Data Requirements

- `key_metrics` (FY): freeCashFlowYield, returnOnEquity, marketCap
- `financial_ratios` (FY): interestCoverageRatio, operatingProfitMargin
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen (TTM data). Lists every line on the exchange, without the listing rules or dedup |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `value-04-fcf-yield_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py` |

See [METHODOLOGY.md](../METHODOLOGY.md) for the shared methodology.
