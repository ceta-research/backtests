# Graham Number

Buy stocks trading below their Graham number, Benjamin Graham's fair-value estimate from earnings and book value. Graham's rule for the defensive investor was a P/E of at most 15 and a P/B of at most 1.5, or a product of at most 22.5. The Graham number puts that in one figure: the square root of 22.5 x EPS x book value per share. A price below it is the same test as P/E x P/B below 22.5.

## Strategy

**Signal:** price < Graham number
**Graham number = sqrt(22.5 x EPS x book value per share)**
**Data source:** `key_metrics.grahamNumber` (FMP, pre-computed per share, fiscal-year rows). It is empty when EPS or book value is negative.

## How the screen works

On each January rebalance date, for every line FMP lists on the market's exchanges:

1. Take the company's latest fiscal-year row whose period ended at least 45 days before the rebalance date and no more than 540 days before it. A company with no such row drops out for that year.
2. Keep it only if that row has a positive Graham number and a market cap above the market's floor (table below).
3. Restate the Graham number in the unit the price is quoted in: x100 for lines quoted in pence or South African cents, and the newest USD exchange rate at most 7 days old when the reporting and quote currencies differ. A line with no currency, no fresh rate, or a depositary receipt quoted in a currency other than its reporting currency is excluded. In the US this excludes 2,762 receipt lines, summed over the 25 rebalances.
4. Require the price (the first close on or after the rebalance date, within 10 days) to be below the Graham number.
5. Keep one line per ISIN, so a security listed under two tickers counts once.
6. Rank by price / Graham number, cheapest first, and take up to 30 names at equal weight. If fewer than 10 pass, or fewer than 10 of those have an entry price of at least 1.0 (local units), hold cash for the year.

## Parameters

- **Rebalancing:** Annual, on January 1, executed at the next available close. 25 periods, January 2000 to January 2025.
- **Min stocks:** 10 (holds cash if fewer qualify, or if fewer than 10 have a usable entry price). A cash year earns 0%. The floor is checked at entry only. A name whose return over the year is above the 200% single-stock cap is left out of that year's average and not replaced.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted.
- **Data lag:** 45 days from the fiscal period end. Many companies publish annual results later than that, so a row can be used before it was public.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. France and South Africa have no local index configured and are measured against the S&P 500 in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py`: US 2.0%, India 6.5%, Germany 2.0%, Canada 2.5%, Sweden 2.0%, Brazil 10.5%, Korea 3.0%, Hong Kong 3.0%, Taiwan 1.0%, Switzerland 0.5%, Japan 0.1%, UK 3.5%.

## Results (January 2000 to January 2025)

Each market is measured in its own currency against its local index. These 12 markets are the ones in `results/exchange_comparison.json`, which the charts read.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| Japan (JPX) | Nikkei 225 | 11.11% | 2.95% | +8.16 | 0.537 | -37.64% | 0 | 29.0 |
| UK (LSE) | FTSE 100 | 8.09% | 0.86% | +7.23 | 0.253 | -38.15% | 0 | 28.5 |
| Sweden (STO) | OMX Stockholm 30 | 9.35% | 2.95% | +6.40 | 0.304 | -42.83% | 3 (2000-2001, 2003) | 28.7 |
| Canada (TSX + TSXV) | TSX Composite | 10.74% | 4.44% | +6.30 | 0.444 | -24.21% | 0 | 28.4 |
| Taiwan (TAI + TWO) | TAIEX | 9.39% | 3.91% | +5.48 | 0.310 | -35.28% | 6 (2000-2005) | 28.8 |
| Hong Kong (HKSE) | Hang Seng | 5.33% | 0.49% | +4.85 | 0.072 | -58.39% | 0 | 19.2 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 11.80% | 7.64% | +4.16 | 0.470 | -31.28% | 0 | 29.2 |
| Brazil (SAO) | Bovespa | 12.60% | 8.44% | +4.16 | 0.078 | -43.52% | 6 (2000-2005) | 26.4 |
| Switzerland (SIX) | SMI | 5.88% | 1.90% | +3.99 | 0.335 | -32.80% | 3 (2000-2001, 2003) | 26.5 |
| Korea (KSC) | KOSPI | 6.49% | 3.32% | +3.17 | 0.154 | -45.26% | 6 (2000-2005) | 28.3 |
| Germany (XETRA) | DAX | 6.49% | 4.45% | +2.04 | 0.197 | -42.84% | 1 (2000) | 27.8 |
| India (NSE) | Sensex | 9.93% | 11.40% | -1.48 | 0.097 | -55.11% | 5 (2000-2004) | 27.1 |

Excess is CAGR minus benchmark CAGR, in percentage points. Returns are in local currency, after costs. Avg stocks is the average number held in invested years.

- **Against the index:** 11 of the 12 books finished ahead of their own index; India trailed the Sensex by 1.48 points. Read that count with the two limits below before treating it as 11 wins for the screen.
- **Dividends:** The portfolios' returns include dividends. Of the 12 benchmarks SPY, the DAX and the Bovespa do; the other nine are price indices, so each of those nine margins is overstated by roughly the index's dividend yield. Details: [METHODOLOGY.md](../METHODOLOGY.md#benchmark-limitations).
- **What the books hold:** The books count lines, not companies, and the cheapest lines by price / Graham number are often not ordinary shares. See "Lines that are not ordinary shares" below. This matters most in the US, Canada and the UK.
- **Sharpe:** Brazil's is 0.078 at a 12.60% CAGR because its risk-free rate is 10.5%. Compare a Sharpe with the index's in the same market, not across markets.

### Other markets

These five have results files but are not in `results/exchange_comparison.json`.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks | Status |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|--------|
| Australia (ASX) | ASX 200 | 7.08% | 4.00% | +3.08 | 0.217 | -39.67% | 0 | 21.6 | Not in the comparison file |
| Norway (OSL) | Oslo All Share | 13.98% | 9.84% | +4.15 | 0.728 | -16.06% | 8 (2000-2006, 2010) | 16.0 | Window truncated: the index starts in 2014, so the figures cover 11 of 25 years |
| France (PAR) | S&P 500 (SPY), in USD | 0.00% | 7.64% | -7.64 | n/a | 0.00% | 25 (2000-2024) | 0.0 | Too thin to test: cash in 25 of 25 years |
| South Africa (JNB) | S&P 500 (SPY), in USD | 6.29% | 7.64% | -1.36 | -0.153 | -29.35% | 7 (2000-2006) | 20.2 | No local index: a local-currency book against a dollar index |
| Singapore (SES) | STI | -0.52% | 1.64% | -2.16 | -0.184 | -53.15% | 9 (2000-2004, 2009, 2012, 2019, 2023) | 13.9 | Not in the comparison file |

France is too thin to test: no line passed the screen in any year, so its 0.00% is 25 years of cash and not a result. Norway's CAGR, Sharpe and drawdown cover 2014 to 2024 only, and none of its cash years falls in that window. South Africa's excess is a rand book against a dollar index and says nothing about the screen. Singapore was invested in 16 of 25 years with 13.9 names on average.

### Cash years

A cash year is one where fewer than 10 names passed the screen, or 10 or more passed and fewer than 10 of them had a usable entry price. Years are the January each holding period starts.

| Market | Cash years | Why |
|--------|------------|-----|
| Japan (JPX) | none | |
| UK (LSE) | none | |
| Sweden (STO) | 2000-2001, 2003 | 2000-2001, 2003: 4 to 9 passed the screen |
| Canada (TSX + TSXV) | none | |
| Taiwan (TAI + TWO) | 2000-2005 | 2000-2005: 0 to 4 passed the screen |
| Hong Kong (HKSE) | none | |
| US (NYSE + NASDAQ + AMEX) | none | |
| Brazil (SAO) | 2000-2005 | 2000-2004: 2 to 8 passed the screen; 2005: 8 of 11 screened had an entry price |
| Switzerland (SIX) | 2000-2001, 2003 | 2000-2001, 2003: 2 to 9 passed the screen |
| Korea (KSC) | 2000-2005 | 2000-2005: 1 to 9 passed the screen |
| Germany (XETRA) | 2000 | 2000: 6 passed the screen |
| India (NSE) | 2000-2004 | 2000-2004: 1 to 4 passed the screen |
| Australia (ASX) | none | |
| Norway (OSL) | 2000-2006, 2010 | 2000-2006, 2010: 1 to 6 passed the screen |
| South Africa (JNB) | 2000-2006 | 2000-2006: 1 to 8 passed the screen |
| Singapore (SES) | 2000-2004, 2009, 2012, 2019, 2023 | 2000-2002: 1 to 7 passed the screen; 2003: 5 of 11 screened had an entry price; 2004: 6 of 14 screened had an entry price; 2009: 7 of 30 screened had an entry price; 2012: 8 of 30 screened had an entry price; 2019: 9 of 30 screened had an entry price; 2023: 9 of 30 screened had an entry price |

Some invested years sit on the 10-name line: Japan 2000, Germany 2001, Switzerland 2002, Norway 2007, Hong Kong 2009, South Africa 2010 and Singapore 2024 each held exactly 10. In Hong Kong 2009 and Singapore 2024 that was 10 names with an entry price out of 30 screened. In Hong Kong and Singapore the 1.0 entry floor is a real price filter: of the names screened, 10 to 25 had a usable entry price in Hong Kong and 10 to 19 in Singapore in invested years.

### Lines that are not ordinary shares

Each results file records the ten cheapest names of every invested year (`period_data[].holdings`, in rank order). A preference line, a fund share class or a second share class can carry the company's Graham number against its own, lower price, and then it has the lowest price / Graham number on the market. The same-ISIN rule doesn't remove it, because it has its own ISIN. Counts over the ten recorded names, by ticker pattern:

| Market | Pattern in the ten recorded names | Years present (of invested years) | Most in one year's ten |
|--------|-----------------------------------|-----------------------------------|------------------------|
| US | Five-letter ticker ending in X, the mutual-fund format (JTSSX, SMTAX, PRGTX) | 11 of 25 (2014-2024) | 7 |
| Canada | Preferred-share series (BBD-PB.TO, IFC-PA.TO, RY-PZ.TO) | 18 of 25 (2007-2024) | 9 |
| UK | Aviva and BP preference lines (AV-A.L, AV-B.L, BP-A.L, BP-B.L) | 19 of 25 | 4 |
| Korea | Code not ending in 0, the preferred-share numbering (003545.KS, 005389.KS) | 19 of 19 | 10 |
| Sweden | Two share classes of one company (SCA-A.ST with SCA-B.ST) | 21 of 22 | 4 pairs |
| Brazil | Two or more lines of one company (TAEE3.SA with TAEE4.SA) | 10 of 19 | 3 companies |
| Germany | Ordinary and preference line of one company (VOW.DE with VOW3.DE) | 17 of 24 | 2 pairs |

The files record ten of up to 30 names a year and no per-name return, so how much of each CAGR these lines earn isn't measured. Until a run excludes them, the US, Canada and UK figures describe a mixed book of shares, funds and preference lines, not a stock screen. Korea's preferred lines are equity of the same companies, but they fill 123 of the 190 recorded slots, so the ten cheapest names each year are mostly preferred shares. The general limit is in DATA_QUALITY_ISSUES.md (lines of one company that carry their own ISINs).

## Rerun October 2026

These results were rerun with five changes. The first three are the campaign rules described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias); the last two are this topic's own.

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 26 holdings in 8 of the 12 markets exited this way; each results file lists them under its `ltp` key. On identical selection the rule moves CAGR by -0.08 to +0.12 points.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market.
- **Graham number in the price's unit.** The screen used to compare the price with the Graham number in whatever currency each was reported in, so depositary receipts of won and yen reporters ranked first in the US on raw figures, and no pence-quoted UK line or cent-quoted South African line could pass. Step 3 above replaces that.
- **Latest filing first.** The screen used to load only fiscal-year rows with a positive Graham number and take the newest of those, so a company whose earnings or book value turned negative kept qualifying on its last good year, however old. It now takes the latest row first, tests it, and ignores rows more than 540 days old.

The earlier results also predate the cash rule on entry prices: a year went to cash only when fewer than 10 names passed the screen, and otherwise averaged whatever was priced.

| Market | CAGR before | CAGR now | Change (pp) | Excess before | Excess now | Cash years before | Cash years now |
|--------|-------------|----------|-------------|---------------|------------|-------------------|----------------|
| Japan (JPX) | 11.63% | 11.11% | -0.52 | +8.68 | +8.16 | 0 | 0 |
| UK (LSE) | 4.54% | 8.09% | +3.55 | +3.68 | +7.23 | 5 | 0 |
| Sweden (STO) | 13.04% | 9.35% | -3.69 | +10.08 | +6.40 | 3 | 3 |
| Canada (TSX + TSXV) | 10.89% | 10.74% | -0.15 | +6.46 | +6.30 | 0 | 0 |
| Taiwan (TAI + TWO) | 8.56% | 9.39% | +0.83 | +4.65 | +5.48 | 6 | 6 |
| Hong Kong (HKSE) | 0.58% | 5.33% | +4.75 | +0.09 | +4.85 | 0 | 0 |
| US (NYSE + NASDAQ + AMEX) | 7.01% | 11.80% | +4.79 | -0.63 | +4.16 | 0 | 0 |
| Brazil (SAO) | 14.04% | 12.60% | -1.44 | +5.60 | +4.16 | 4 | 6 |
| Switzerland (SIX) | 6.55% | 5.88% | -0.67 | +4.66 | +3.99 | 2 | 3 |
| Korea (KSC) | 5.59% | 6.49% | +0.90 | +2.26 | +3.17 | 6 | 6 |
| Germany (XETRA) | 8.21% | 6.49% | -1.72 | +3.77 | +2.04 | 0 | 1 |
| India (NSE) | 8.25% | 9.93% | +1.68 | -3.15 | -1.48 | 5 | 5 |

The two selection fixes drive the large moves, and no run isolates one change from another:

- **US** rose from 7.01% to 11.80% and went from trailing SPY to leading it. The currency rule replaced the book: at five sampled rebalances 23 to 27 of the earlier top 30 were foreign-currency reporters, which it now converts or excludes. The new book is the one with fund-format tickers among its cheapest names from 2014.
- **UK** rose from 4.54% to 8.09%. Before, no pence line could pass, so the book was made of the non-pence lines the listing rule now removes, and five years (2000-2004) sat in cash. Now every line is pence-quoted and no year is in cash.
- **Hong Kong** rose from 0.58% to 5.33% and went from level with the Hang Seng (+0.09) to ahead of it. The earlier -80.80% drawdown, a run of losses from 2018 to 2023, is now -58.39%, the single year 2008.
- **Sweden** fell from 13.04% to 9.35% on two years. 2002 was a cash year and is now invested with 11 names (-22.56%); 2003 was invested (+53.30%) and is now a cash year with 8 names passing.
- **Brazil** has two more cash years, 2003 (8 names passed) and 2005 (8 of 11 screened had an entry price). **Germany** and **Switzerland** each have one more, 2000 and 2003, with 6 and 9 home-domiciled names passing.
- **South Africa** went from 25 cash years to 7 once cent prices were compared with a Graham number in cents. **Singapore** went from 2 cash years to 9; six of them are years where 11 to 30 names passed the screen and only 5 to 9 had an entry price of at least 1.0.
- Benchmark CAGRs are unchanged in all 12 markets.

## Market Cap Floors

From `MKTCAP_THRESHOLD_MAP` in `cli_utils.py`. The floor compares the filing's market cap, in the company's reporting currency, with a threshold set in the listing currency.

| Market | Floor (local currency) |
|--------|------------------------|
| NYSE / NASDAQ / AMEX | $1B |
| TSX / TSXV (Canada) | C$500M |
| LSE (UK) | £500M |
| XETRA (Germany), PAR (France) | €500M |
| SIX (Switzerland) | CHF 500M |
| STO (Sweden), OSL (Norway) | SEK 5B, NOK 5B |
| NSE (India) | ₹20B |
| HKSE (Hong Kong) | HK$2B |
| JPX (Japan) | ¥100B |
| KSC (Korea) | ₩500B |
| TAI / TWO (Taiwan) | NT$10B |
| SAO (Brazil) | R$1B |
| ASX (Australia), SES (Singapore) | A$500M, S$500M |
| JNB (South Africa) | R10B |

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market, as below.

```bash
# One market, written to the committed results file
python3 graham-number/backtest.py --preset us --output graham-number/results/us_major_results.json
python3 graham-number/backtest.py --exchange HKSE --output graham-number/results/hkse_results.json

# The 12-market comparison file the charts read, built from the per-market files
python3 scripts/rebuild_comparison.py graham-number --keep US_MAJOR India Germany Canada Sweden Brazil Korea HKSE Taiwan Switzerland Japan UK
python3 scripts/rebuild_comparison.py graham-number --keep US_MAJOR India Germany Canada Sweden Brazil Korea HKSE Taiwan Switzerland Japan UK --check

# Charts from results/exchange_comparison.json
python3 graham-number/generate_charts.py

# The old exit rule (doesn't reproduce old results: the other four changes still apply)
python3 graham-number/backtest.py --preset uk --exit-method drop

# Current screen
python3 graham-number/screen.py
python3 graham-number/screen.py --exchange NSE
```

The committed files were made one market at a time. Each row is `python3 graham-number/backtest.py <arguments> --output graham-number/results/<output file>`:

| Market | Arguments | Exchanges | Output file | Listing rule |
|--------|-----------|-----------|-------------|--------------|
| US | `--preset us` | NYSE, NASDAQ, AMEX | `us_major_results.json` |  |
| India | `--preset india` | NSE | `india_results.json` |  |
| Germany | `--preset germany` | XETRA | `germany_results.json` | Germany-domiciled only |
| Canada | `--preset canada` | TSX, TSXV | `canada_results.json` |  |
| Sweden | `--preset sweden` | STO | `sweden_results.json` |  |
| Brazil | `--preset brazil` | SAO | `brazil_results.json` |  |
| Korea | `--preset korea` | KSC | `korea_results.json` |  |
| Australia | `--preset australia` | ASX | `australia_results.json` |  |
| Hong Kong | `--exchange HKSE` | HKSE | `hkse_results.json` |  |
| Taiwan | `--preset taiwan` | TAI, TWO | `taiwan_results.json` |  |
| Switzerland | `--preset switzerland` | SIX | `switzerland_results.json` | Switzerland-domiciled only |
| Norway | `--preset norway` | OSL | `norway_results.json` |  |
| UK | `--preset uk` | LSE | `uk_results.json` | GBp lines only |
| France | `--preset france` | PAR | `france_results.json` |  |
| Japan | `--preset japan` | JPX | `japan_results.json` |  |
| South Africa | `--preset southafrica` | JNB | `southafrica_results.json` |  |
| Singapore | `--preset singapore` | SES | `singapore_results.json` |  |

`graham-number/run_all_exchanges.py` runs the same 17 markets and writes the same per-market files, but it also replaces `exchange_comparison.json` with all 17 markets. Run the two `rebuild_comparison.py` lines after it to get the 12-market file back. `backtest.py --global` pools every exchange into a single 30-stock portfolio; it is not what the tables above report.

## Data Requirements

- `key_metrics` (FY): grahamNumber, marketCap, reportedCurrency
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, currency and isAdr (Graham number unit, LSE rule), country (XETRA and SIX rule), isin and isActivelyTrading (dedup)
- `forex_prices_daily`: USD exchange rates for lines that report in one currency and trade in another

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025), one market per run |
| `run_all_exchanges.py` | Runs the 17 markets in turn and writes a 17-market `exchange_comparison.json` |
| `screen.py` | Current stock screen on trailing figures (P/E x P/B below 22.5). No currency restatement, listing rules or dedup |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `<market>_results.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py` |

## Academic Reference

Benjamin Graham, *The Intelligent Investor* (1949, revised 1973), chapter 14: for the defensive investor, a price of at most 15 times average earnings and at most 1.5 times book value, or a product of the two multipliers of at most 22.5.

## Data Source

All data via [Ceta Research](https://cetaresearch.com) (FMP financial data warehouse): fiscal-year key metrics, dividend-adjusted end-of-day prices and daily exchange rates. See [METHODOLOGY.md](../METHODOLOGY.md).
