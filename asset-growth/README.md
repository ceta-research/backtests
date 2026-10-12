# Asset Growth Anomaly

Companies that grow their assets slowly tend to outperform those expanding aggressively. This strategy screens for quality companies with low total asset growth.

## Academic Basis

Cooper, Gulen & Schill (2008), "Asset Growth and the Cross-Section of Stock Returns", *Journal of Finance* 63(4), 1609-1651. Found that firms in the lowest asset growth decile outperformed the highest decile by ~20% annually over 1968-2003.

## Signal

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| Asset Growth (YoY) | -20% to +10% | Low growth, not distressed |
| Return on Equity | > 8% | Profitable |
| Return on Assets | > 5% | Capital efficient |
| Operating Profit Margin | > 10% | Operationally strong |
| Market Cap | > $500M in the US; elsewhere half the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`), in local currency (₹10B on NSE, ¥50B on JPX, €250M on XETRA) | Liquid, investable |

**Sorting:** Lowest asset growth first (ascending). Top 30.

**Asset Growth** = (Total Assets current FY - Total Assets prior FY) / Total Assets prior FY

## Parameters

- **Rebalancing:** Annual, on July 1, executed at the next available close. 25 periods, July 2000 to July 2025.
- **Min stocks:** 10 (holds cash if fewer qualify, or if fewer than 10 have a usable entry price). A cash year earns 0%. The floor is checked at entry only. A name that fails a data check at exit (a return above the 200% single-stock cap, or no exit price at all) is left out of that year's average and not replaced, so an invested year can hold fewer than 10: Brazil's 2007 book had 10 buyable names and averaged 9.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit (0.2% to 1.0% round trip). The tier thresholds are in US dollars and market caps aren't converted, so on NSE, JPX and KSC, where the market-cap floor is at least 10 billion local units (₹10B, ¥50B, ₩250B), every holding pays the 0.1% tier.
- **Data lag:** 45 days (point-in-time)
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes have their own ISINs, so a company's ordinary and preference shares can both be held: Germany's book holds both lines of at least one of Henkel (HEN.DE, HEN3.DE), Fuchs (FPE.DE, FPE3.DE), Sartorius, Biotest or BMW in 17 of its 18 invested years, and Brazil's holds COCE5.SA and COCE6.SA together in five.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. Saudi Arabia has no local index configured and is measured against the S&P 500 in USD. China (SHZ + SHH) is measured against the SSE Composite.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 10.5% Brazil).

## Results (July 2000 to July 2025)

The strategy beat its own market's index in 12 of 15 markets. Germany, Australia and Thailand trailed. Saudi Arabia is left out of the count because its benchmark is the S&P 500 in USD; it trailed that by 5.19pp.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| India (NSE) | Sensex | 13.42% | 12.06% | +1.37 | 0.286 | -20.43% | 5 (2000-2004) | 22.4 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 12.97% | 7.85% | +5.12 | 0.691 | -29.07% | 0 | 25.9 |
| UK (LSE) | FTSE 100 | 9.38% | 1.23% | +8.15 | 0.333 | -21.88% | 1 (2001) | 20.6 |
| Brazil (SAO) | Bovespa | 8.73% | 8.70% | +0.03 | -0.124 | -15.78% | 7 (2000-2006) | 13.9 |
| Canada (TSX) | TSX Composite | 8.36% | 3.95% | +4.41 | 0.375 | -31.51% | 3 (2000, 2002-2003) | 24.0 |
| Japan (JPX) | Nikkei 225 | 7.34% | 3.31% | +4.03 | 0.389 | -27.17% | 5 (2000-2004) | 28.2 |
| Hong Kong (HKSE) | Hang Seng | 6.08% | 1.64% | +4.44 | 0.165 | -26.63% | 2 (2000, 2002) | 19.3 |
| Sweden (STO) | OMX Stockholm 30 | 5.88% | 2.55% | +3.32 | 0.241 | -36.52% | 7 (2000-2004, 2006, 2009) | 20.2 |
| Taiwan (TAI) | TAIEX | 5.72% | 4.09% | +1.64 | 0.404 | -10.94% | 8 (2000-2007) | 28.4 |
| Korea (KSC) | KOSPI | 5.57% | 5.35% | +0.21 | 0.193 | -22.01% | 10 (2000-2009) | 23.8 |
| Switzerland (SIX) | SMI | 5.51% | 1.74% | +3.77 | 0.314 | -35.82% | 5 (2000-2004) | 20.8 |
| China (SHZ + SHH) | SSE Composite | 5.25% | 2.43% | +2.82 | 0.084 | -64.17% | 0 | 19.9 |
| Thailand (SET) | SET Index | 3.49% | 5.13% | -1.64 | 0.059 | -32.71% | 5 (2000-2004) | 22.1 |
| Germany (XETRA) | DAX | 2.69% | 5.04% | -2.35 | 0.039 | -48.37% | 7 (2000-2006) | 20.7 |
| Saudi Arabia (SAU) | S&P 500 (USD) | 2.67% | 7.85% | -5.19 | -0.053 | -38.96% | 9 (2000-2008) | 23.1 |
| Australia (ASX) | ASX 200 | 2.17% | 3.89% | -1.72 | -0.101 | -44.51% | 7 (2000-2006) | 21.8 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their July rebalance.

- **US:** 12.97% a year against 7.85% for SPY, with a Sharpe of 0.691 against 0.352, a max drawdown of -29.07% against -38.01%, and no cash years.
- **Thin leads:** Brazil beats the Bovespa by 0.03pp with a lower Sharpe (-0.124 against -0.075), and Korea beats the KOSPI by 0.21pp, invested only from the 2010 rebalance.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). Four markets are measured against a series that does too: the US and Saudi Arabia against SPY, Germany against the DAX, Brazil against the Bovespa. The other twelve are measured against price indices, so their excess is overstated by roughly the index's dividend yield. See METHODOLOGY.md (Benchmark Limitations).
- **Cash years:** a year is in cash when fewer than 10 names pass the screen, or fewer than 10 of those have a usable entry price. Every cash year is a 2009 or earlier rebalance. The US and China were invested in all 25 years.
- **Sharpe:** Brazil's is negative at an 8.73% CAGR because its risk-free rate is 10.5%.

## Rerun October 2026

These results were rerun with three changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias):

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 26 holdings in 11 markets exited this way; each results file lists them under its `ltp` key.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market. Share classes have their own ISINs and can both be held, so Germany's ordinary and preference pairs (HEN.DE and HEN3.DE, for example) aren't a dedup failure.

The listing rules drive the large moves:

- **Germany** fell from 6.76% to 2.69% and now trails the DAX by 2.35pp, after leading it by 1.72pp. It's the one market whose verdict changed (13 of 15 beat before, 12 now). Cash years rose from 4 (2000-2003) to 7 (2000-2006): fewer than 10 Germany-domiciled companies passed the screen from 2000 to 2005, and in 2006 only 8 of the 11 that passed had a usable entry price.
- **UK** rose from 6.04% to 9.38%. Before, 12 of 25 rebalances sat in cash: the screen passed 30 names, but only 6 to 9 of them had a usable entry price. With pence lines only, that happens once (2001).
- **Switzerland** moved from 5.64% to 5.51%.
- The other 13 markets moved by -0.21pp to +0.09pp. Benchmark returns are unchanged.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market, as in the table below. A mixed leg such as `--exchange XETRA,LSE` or `--exchange LSE,NYSE` gets no listing rule.

```bash
# US (default universe: NYSE + NASDAQ + AMEX)
python3 asset-growth/backtest.py --verbose

# One market, written to the committed results file
python3 asset-growth/backtest.py --exchange XETRA --output asset-growth/results/returns_XETRA.json

# All markets in one loop, plus exchange_comparison.json
python3 asset-growth/backtest.py --global --output asset-growth/results/exchange_comparison.json --verbose

# The old exit rule (doesn't reproduce old results: the listing rules and dedup still apply)
python3 asset-growth/backtest.py --exchange LSE --exit-method drop

# Charts from results/exchange_comparison.json
python3 asset-growth/generate_charts.py

# Current screen
python3 asset-growth/screen.py
python3 asset-growth/screen.py --preset india
```

Per-market runs that reproduce the committed files:

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| US | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `NSE` | `returns_NSE.json` | |
| UK | `LSE` | `returns_LSE.json` | GBp lines only |
| Brazil | `SAO` | `returns_SAO.json` | |
| Canada | `TSX` | `returns_TSX.json` | |
| Japan | `JPX` | `returns_JPX.json` | |
| Hong Kong | `HKSE` | `returns_HKSE.json` | |
| Sweden | `STO` | `returns_STO.json` | |
| Taiwan | `TAI` | `returns_TAI.json` | |
| Korea | `KSC` | `returns_KSC.json` | |
| Switzerland | `SIX` | `returns_SIX.json` | Switzerland-domiciled only |
| China | `SHZ,SHH` | `returns_SHZ_SHH.json` | |
| Thailand | `SET` | `returns_SET.json` | |
| Germany | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Saudi Arabia | `SAU` | `returns_SAU.json` | |
| Australia | `ASX` | `returns_ASX.json` | |

Use `--exchange` as shown. `--preset canada` adds TSXV and `--preset taiwan` adds TWO, so neither reproduces the TSX or TAI file. `--global` also tries Singapore (SGX); no Singapore results are committed.

## Data Requirements

- `balance_sheet` (FY): totalAssets (two consecutive years)
- `key_metrics` (FY): returnOnEquity, returnOnAssets, marketCap
- `financial_ratios` (FY): operatingProfitMargin
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen (TTM data). Lists every line on the exchange, without the listing rules or dedup. It also excludes the Asset Management, Shell Companies and Closed-End Fund industries, which the backtest doesn't |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py` |
