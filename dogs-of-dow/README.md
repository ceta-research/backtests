# Dogs of the Dow

Buy the 10 highest-yielding Dow stocks every January, hold them for a year, repeat. Outside the US there is no Dow, so the same rule runs on each exchange's 30 largest lines: rank them by dividend yield and hold the top 10 ("high-yield blue chips").

## Academic Basis

Michael O'Higgins, *Beating the Dow* (1991). The idea is mean reversion among large companies: a high yield marks a share price that has fallen relative to the dividend, and large companies tend to recover.

## What the screen selects

On each January rebalance date:

**US (the Dow leg)**

1. Start from the Dow Jones Industrial Average members in `dowjones_constituent`. The table holds today's members, and they are used for every year (see Known limits).
2. Take each member's latest fiscal-year ratios row whose fiscal year ended at least 45 days before the rebalance date and at most 540 days before it. A member with no such row, or whose row shows no dividend yield above zero, is out for the year.
3. Keep one line per ISIN.
4. Hold the 10 highest yields at equal weight.

**Every other market (high-yield blue chips)**

1. Start from every line listed on the exchange, after the listing rule on XETRA, SIX and LSE (see Parameters).
2. Size each line as the smaller of two figures, both in the exchange's currency: its latest fiscal-year market cap restated from its reporting currency, and its entry-bar close times its latest fiscal-year weighted average share count (times 0.01 on pence and cent quotes). The exchange rate is the newest USD rate on or before the entry bar, at most 7 days old. If one figure is missing the other is used.
3. A line can take a blue-chip slot only if it has a usable entry price (an adjusted close of at least 1.0 in the 10 days after the rebalance date) and its size is at or above the market's floor (table under Usage) and at most 15,000 times that floor. The ceiling is a data-error limit, not a size preference.
4. Keep one line per ISIN and take the 30 largest.
5. Of those 30, keep the lines whose latest fiscal-year ratios row (same 45-day lag and 540-day limit) shows a dividend yield above zero, and hold the 10 highest yields at equal weight.

If fewer than 5 names pass, the portfolio holds cash for the year.

## Parameters

- **Rebalancing:** Annual, on January 1, executed at the next available close. 25 periods, January 2000 to January 2025.
- **Min stocks:** 5. The portfolio holds cash if fewer than 5 names pass the screen, or if fewer than 5 of them have a usable entry price. A cash year earns 0%. In these results every cash year comes from the screen count, because outside the US a line needs an entry price to be screened at all.
- **Max stocks:** 10
- **Weighting:** Equal weight
- **Return cap:** A holding whose one-year return is above 200% is treated as a price error. It is left out of that year's average and not replaced, so an invested year can average fewer names than it bought (see Trimmed and short books).
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit (0.2% to 1.0% round trip). The tier thresholds are in US dollars and sizes aren't converted, so on the seven markets whose floor is 10 billion local units or more (BSE, NSE, JPX, KSC, SET, TAI, JNB) every holding pays the 0.1% tier.
- **Data lag:** 45 days after the fiscal year end. A fiscal-year row more than 540 days old is not used.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes, preferred shares and Thai `-R` lines have their own ISINs and are not merged (see Lines are not companies).
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. Shenzhen, South Africa and Saudi Arabia have no local index in the price data and are measured against the S&P 500 in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 9.0% South Africa).

## Results (January 2000 to January 2025)

The strategy finished ahead of its own market's index in all 14 markets that have one. Two of those comparisons are like for like, because the index includes dividends as the portfolio does: the US, +2.54pp against SPY, and Germany, +1.64pp against the DAX. The other 12 indices are price indices, so those margins are overstated by roughly the index's dividend yield, and the thinnest of them (India BSE +1.10pp, Shanghai +1.19pp, Taiwan +1.87pp, India NSE +2.39pp) may not survive a total-return comparison. Shenzhen, South Africa and Saudi Arabia are measured against the S&P 500 in USD. All three trail it, and they are left out of the count.

No market is too thin to test. The least invested, Saudi Arabia, holds stocks in 17 of its 25 years.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg lines |
|--------|-----------|------|----------------|-------------|--------|--------|------------|-----------|
| India (NSE) | Sensex | 13.80% | 11.40% | +2.39 | 0.266 | -44.41% | 2 (2000-2001) | 9.3 |
| India (BSE) | Sensex | 12.50% | 11.40% | +1.10 | 0.242 | -43.15% | 4 (2000-2003) | 9.7 |
| US (Dow 30) | S&P 500 (SPY) | 10.19% | 7.64% | +2.54 | 0.698 | -19.48% | 0 | 10.0 |
| Sweden (STO) | OMX Stockholm 30 | 9.26% | 2.95% | +6.31 | 0.342 | -39.47% | 0 | 9.9 |
| Canada (TSX + TSXV) | TSX Composite | 8.70% | 4.44% | +4.27 | 0.385 | -24.52% | 0 | 10.0 |
| Hong Kong (HKSE) | Hang Seng | 8.52% | 0.49% | +8.03 | 0.232 | -43.26% | 0 | 10.0 |
| Switzerland (SIX) | SMI | 7.96% | 1.90% | +6.06 | 0.395 | -39.15% | 0 | 9.9 |
| UK (LSE) | FTSE 100 | 7.62% | 0.86% | +6.76 | 0.268 | -21.10% | 0 | 10.0 |
| Thailand (SET) | SET Index | 7.60% | 4.16% | +3.44 | 0.208 | -38.73% | 4 (2000-2003) | 10.0 |
| Korea (KSC) | KOSPI | 7.03% | 3.32% | +3.71 | 0.187 | -44.17% | 3 (2000-2002) | 9.6 |
| South Africa (JNB) | S&P 500 (USD) | 6.52% | 7.64% | -1.12 | -0.118 | -23.24% | 3 (2000-2002) | 9.4 |
| Japan (JPX) | Nikkei 225 | 6.35% | 2.95% | +3.40 | 0.295 | -62.45% | 0 | 10.0 |
| Germany (XETRA) | DAX | 6.09% | 4.45% | +1.64 | 0.188 | -44.69% | 0 | 10.0 |
| Taiwan (TAI) | TAIEX | 5.78% | 3.91% | +1.87 | 0.280 | -31.18% | 6 (2000-2005) | 9.9 |
| China, Shenzhen (SHZ) | S&P 500 (USD) | 5.21% | 7.64% | -2.44 | 0.062 | -64.80% | 0 | 9.8 |
| China, Shanghai (SHH) | SSE Composite | 4.73% | 3.54% | +1.19 | 0.054 | -64.90% | 0 | 9.8 |
| Saudi Arabia (SAU) | S&P 500 (USD) | 2.14% | 7.64% | -5.50 | -0.081 | -45.38% | 8 (2000-2007) | 10.0 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their January rebalance. Avg lines is the average number of lines held in invested years; two share lines of one company count as two. Max drawdown is measured on January-to-January values, so the drawdown inside a year is deeper.

- **US:** 10.19% a year against 7.64% for SPY, with a Sharpe of 0.698 against 0.322, a max drawdown of -19.48% against -34.90%, and no cash years.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). SPY and the DAX do too. The other 12 local indices are price indices, so the excess against them is overstated by roughly the index's dividend yield. See METHODOLOGY.md (Benchmark Limitations).
- **S&P 500 in USD:** for Shenzhen, South Africa and Saudi Arabia the excess compares a local-currency return with a US dollar index. It says nothing about the local market.
- **Cash years:** 30 market-years in seven markets, all at the start of the window, when few lines have a fiscal-year filing, a size above the floor and a price. The other ten markets were invested in all 25 years.
- **Drawdowns:** three markets lost more than 60% from a January peak: Shenzhen -64.80% (the single year 2008), Shanghai -64.90% (January 2008 to January 2014) and Japan -62.45% (January 2007 to January 2012).
- **Sharpe:** South Africa's is negative at a 6.52% CAGR because its risk-free rate is 9.0%.

### Lines are not companies

The dedup merges lines with the same ISIN. Share classes, preferred shares and Thai `-R` lines carry their own ISINs, so one company can fill several of the 10 slots. Counting by ticker pattern (a hyphen suffix dropped; in Korea the first five characters; on XETRA a trailing digit dropped), these slots are a second or later line of a company already in that year's book:

| Market | Slots filled | Second or later lines | Years affected | Largest case |
|--------|--------------|-----------------------|----------------|--------------|
| Thailand (SET) | 210 | 81 (39%) | 21 of 21 | 2009: five pairs, so 10 lines are 5 companies |
| Canada (TSX + TSXV) | 249 | 63 (25%) | 22 of 25 | 2022: ENB.TO with four Enbridge preferred lines, TRP.TO with one |
| Sweden (STO) | 248 | 61 (25%) | 25 of 25 | 2005: five A/B or A/C pairs, so 10 lines are 5 companies |
| Korea (KSC) | 211 | 35 (17%) | 16 of 22 | 2022: 005380.KS with three preferred lines, 005930.KS with one |
| Germany (XETRA) | 249 | 17 (7%) | 14 of 25 | 2004: HEN.DE with HEN3.DE, VOW.DE with VOW3.DE |
| UK (LSE) | 250 | 13 (5%) | 7 of 25 | 2010: BP.L with BP-A.L and BP-B.L |

The count is a lower bound: the pattern misses pairs such as GACA.L and GACB.L, BAS.DE and BASA.DE (held together in 2014-2021), RO.SW and ROG.SW, UHR.SW and UHRN.SW, and FFA.JO and FFB.JO. Three further points:

- **Canada:** 96 of the 249 slots (39%) are preferred-share lines (tickers with `-P`), and at least one is held in every year. Preferred shares pay a fixed coupon and move less than ordinary shares, so Canada's figures describe a book that is partly preferred shares.
- **UK:** 52 of the 250 slots (21%) are four preference-share lines: GACA.L and GACB.L (General Accident preference shares, 18 years each) and BP-A.L and BP-B.L (BP preference shares, 8 years each). At least one is held in every year from 2005 to 2024, and they fill 4 of the 10 slots in 2010, 2011, 2016, 2017, 2019 and 2022. The UK's drawdown, volatility and win rate describe a book that is partly fixed-coupon preference shares.
- **Hong Kong:** 10 slots are lines numbered 43xx.HK (4333.HK, 4335.HK, 4338.HK), the Hong Kong lines of US companies.

### Trimmed and short books

- **Return cap:** the 200% cap removed 25 names in 12 market-years: India (BSE) 2007 (1 of 10) and 2009 (2), India (NSE) 2007 (1) and 2009 (2), Hong Kong 2018 (1), Shenzhen 2007 (4), Shanghai 2006 (1) and 2007 (3), Canada 2000 (1), Taiwan 2023 (1), South Africa 2005 (6) and 2011 (2). The cap works on the upside only, so if a removed return was real the year is understated. South Africa 2005 averages 4 names and Shenzhen 2007 averages 6.
- **Short screens:** in 11 invested market-years fewer than 10 names passed: India (BSE) 2004 (6), India (NSE) 2002, 2003 and 2004 (6 each), Germany 2000 (9), Korea 2003 and 2004 (6 each) and 2005 (9), Sweden 2000 (8), Switzerland 2000 (8), South Africa 2003 (5). Every other invested year screens 10.

## Rerun October 2026

These results were rerun with the campaign's three changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias), and two fixes to this topic's screen (METHODOLOGY.md, Filing and Selection Rules):

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 7 exits in 6 markets were priced this way; each results file lists them under its `ltp` key. On the same selection the exit rule changes CAGR by -0.12pp (UK) to +0.46pp (Shenzhen).
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market.
- **Yield on the latest filing.** The latest fiscal-year ratios row is picked first and then tested for a yield above zero, and a row more than 540 days old is not used. Before, a company that cut its dividend kept ranking on its last paying filing.
- **Size outside the US.** The 30 blue chips are sized as the smaller of the reported market cap restated in the exchange's currency and entry price times share count, a line needs an entry price to take a slot, and a size above 15,000 times the floor is dropped as a data error. Before, the cut ranked raw reported market caps, in mixed currencies, with corrupt values and unpriced lines taking slots. Each non-US results file records what the rule dropped under `size_excluded`: 29,990 line-years with no entry price across the 16 markets, and 73 over the ceiling (71 of them in Japan).

The earlier results, from April 2026, also predate the entry-price check in the cash rule: they counted every screened line toward the floor of 5 and averaged whichever lines had prices.

What moved:

- **Books are full now.** Average lines held rose in all 16 non-US markets, from between 3.6 and 9.8 to between 9.3 and 10.0: Switzerland 3.6 to 9.9, UK 5.0 to 10.0, Germany 6.1 to 10.0, Canada 7.0 to 10.0.
- **UK** rose from 2.01% to 7.62%, and its max drawdown went from -74.32% to -21.10%, with a book that is 21% preference-share lines.
- **South Africa** fell from 10.29% to 6.52% and now trails the S&P 500 (USD) by 1.12pp, after leading it by 2.65pp.
- **Germany** rose from 3.15% to 6.09% and now leads the DAX by 1.64pp, after trailing it by 1.29pp.
- **Canada** rose from 5.97% to 8.70%, with a book that is 39% preferred-share lines.
- **India (BSE)** fell from 14.60% to 12.50%. 2001, 2002 and 2003 returned -42.44%, +28.68% and +116.32% before and are cash years now, because 2, 3 and 4 names pass the screen. Those three years account for the whole fall.
- **India (NSE)** rose from 12.46% to 13.80%: 2001, -28.24% before, is a cash year now. **Taiwan** fell from 6.99% to 5.78%: 2005, +32.48% before, is a cash year now.
- **Shanghai** moved from level with the SSE Composite (-0.10pp) to 1.19pp ahead of it.
- **Japan** moved from 5.77% to 6.35%, and its max drawdown deepened from -51.18% to -62.45%.
- **Other markets:** Thailand 5.77% to 7.60%, Hong Kong 6.87% to 8.52%, Korea 8.67% to 7.03%, Switzerland 6.48% to 7.96%, Shenzhen 3.93% to 5.21%, Saudi Arabia 1.63% to 2.14%, Sweden 9.74% to 9.26%, US 10.22% to 10.19%. Benchmark CAGRs are unchanged.

The earlier results files record no holdings, so the files can't say how much of each move comes from the listing rules, the dedup, the size rule, the yield fix or six months of data revisions. The exit rule's share is known from a run that computes both books, and it is the -0.12pp to +0.46pp above.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market.

The committed results were produced one market at a time:

```bash
# The three markets run from a preset
python3 dogs-of-dow/backtest.py --preset us --output dogs-of-dow/results/us.json
python3 dogs-of-dow/backtest.py --preset sweden --output dogs-of-dow/results/sweden.json
python3 dogs-of-dow/backtest.py --preset canada --output dogs-of-dow/results/returns_TSX.json

# The other fourteen
for ex in BSE NSE XETRA HKSE SHZ SHH KSC SET TAI SIX JPX LSE JNB SAU; do
  python3 dogs-of-dow/backtest.py --exchange $ex --output dogs-of-dow/results/returns_$ex.json
done

# Rebuild the combined file the charts read from the 17 per-market files, then check it
python3 scripts/rebuild_comparison.py dogs-of-dow
python3 scripts/rebuild_comparison.py dogs-of-dow --check

# Charts from results/exchange_comparison.json
python3 dogs-of-dow/generate_charts.py
```

Other commands:

```bash
# One market with per-year detail printed
python3 dogs-of-dow/backtest.py --exchange JPX --verbose

# The old exit rule (doesn't reproduce old results: the listing rules, dedup and screen fixes still apply)
python3 dogs-of-dow/backtest.py --exchange LSE --exit-method drop

# Current screen
python3 dogs-of-dow/screen.py
python3 dogs-of-dow/screen.py --preset india
```

| Market | Arguments | Output file | Listing rule | Size floor |
|--------|-----------|-------------|--------------|------------|
| US (Dow 30) | `--preset us` | `us.json` | | none (Dow members) |
| India (BSE) | `--exchange BSE` | `returns_BSE.json` | | ₹20B |
| India (NSE) | `--exchange NSE` | `returns_NSE.json` | | ₹20B |
| Germany (XETRA) | `--exchange XETRA` | `returns_XETRA.json` | Germany-domiciled only | €500M |
| Hong Kong (HKSE) | `--exchange HKSE` | `returns_HKSE.json` | | HK$2B |
| China, Shenzhen (SHZ) | `--exchange SHZ` | `returns_SHZ.json` | | ¥2B |
| China, Shanghai (SHH) | `--exchange SHH` | `returns_SHH.json` | | ¥2B |
| Korea (KSC) | `--exchange KSC` | `returns_KSC.json` | | ₩500B |
| Canada (TSX + TSXV) | `--preset canada` | `returns_TSX.json` | | C$500M |
| Thailand (SET) | `--exchange SET` | `returns_SET.json` | | ฿10B |
| Taiwan (TAI) | `--exchange TAI` | `returns_TAI.json` | | NT$10B |
| Sweden (STO) | `--preset sweden` | `sweden.json` | | SEK 5B |
| Switzerland (SIX) | `--exchange SIX` | `returns_SIX.json` | Switzerland-domiciled only | CHF 500M |
| Japan (JPX) | `--exchange JPX` | `returns_JPX.json` | | ¥100B |
| UK (LSE) | `--exchange LSE` | `returns_LSE.json` | GBp lines only | £500M |
| South Africa (JNB) | `--exchange JNB` | `returns_JNB.json` | | R10B |
| Saudi Arabia (SAU) | `--exchange SAU` | `returns_SAU.json` | | SAR 1B |

Use the arguments as shown. `--preset india` runs NSE and `--preset taiwan` adds TWO, so neither reproduces the BSE or TAI file. `--exchange TSX` leaves out TSXV and doesn't reproduce `returns_TSX.json`.

`run_all_exchanges.py` runs the same 17 markets in one loop but writes only a combined file, with no per-market files, no per-year records and extra keys on each market (`status`, `exchanges`, `elapsed_seconds`). `rebuild_comparison.py --check` rejects that file. `backtest.py --global` pools every exchange into a single 10-stock portfolio; it is not what the table above reports.

## Known limits

- **The Dow leg uses today's members in every year.** A company that joined the index later is ranked in years before it joined, and a company that was removed is never ranked. SHW, for example, is held from 2000 to 2005. This is survivorship bias; see METHODOLOGY.md (Limitations: Survivorship Bias).
- **One line per ISIN isn't one line per company** (Lines are not companies, above).
- **Missing price history.** A line needs an entry price to take a blue-chip slot. Some large companies have no price history in the data before 2026, and their slots go to smaller names. See DATA_QUALITY_ISSUES.md.
- **South Africa's prices are unreliable in the mid-2000s.** The return cap removed 6 of the 10 names bought in 2005. Treat that market's early years with caution.
- **Fiscal-year rows are used 45 days after the fiscal year end.** Many companies publish annual results later than that, so a row can be used before it was public.
- **`screen.py`** shows today's screen on trailing figures. It ranks raw market caps and applies none of the listing rules, the dedup or the size rule.

## Data Requirements

- `dowjones_constituent`: Dow members (US leg)
- `financial_ratios` (FY): dividendYield
- `key_metrics` (FY): marketCap, reportedCurrency
- `income_statement` (FY): weightedAverageShsOut (size, non-US legs)
- `forex_prices_daily`: USD exchange rates (size, non-US legs)
- `stock_eod`: adjClose and close after each rebalance date, and the last traded price for exits
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, size, dedup tie-break), isin and isActivelyTrading (dedup)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025), one market per run |
| `run_all_exchanges.py` | All 17 markets in one loop; writes a combined file only (see Usage) |
| `screen.py` | Current screen on trailing figures, without the backtest's listing rules, dedup or size rule |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | One file per market (table under Usage) and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py` |

*Data: Ceta Research (FMP financial data warehouse).*
