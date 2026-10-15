# 52-Week High Proximity

Buys the 30 stocks trading closest to their 52-week high, every quarter, at equal weight. George & Hwang (2004) found that nearness to the 52-week high predicts returns and explains much of what is called price momentum. The proposed reason is anchoring: investors treat the old high as a ceiling and are slow to price good news as a stock approaches it.

## Academic Basis

- George, T. & Hwang, C. (2004). "The 52-Week High and Momentum Investing." *Journal of Finance*, 59(5), 2145-2176.
- Li, J. & Yu, J. (2012). "Investor Attention, Psychological Anchors, and Stock Return Predictability." *Journal of Financial Economics*, 104(2), 401-419.
- Jegadeesh, N. & Titman, S. (1993). "Returns to Buying Winners and Selling Losers." *Journal of Finance*, 48(1), 65-91.

## Signal

```
proximity_ratio = close / MAX(high over the 252 bars ending at the signal bar)
```

- **Signal bar:** the latest bar on or up to 10 days before the rebalance date with volume above 0. A line with no traded bar in those 10 days is not ranked. The 52-week high is the highest `high` over the 252 bars ending at the signal bar, untraded (zero or NULL volume) bars included, and a line needs at least 100 of them.
- **Price basis:** `close` and `high` are both split-adjusted and neither is dividend-adjusted, so the ratio compares like with like. A ratio of 1.0 means the stock closed at its 52-week high; 0.95 means 5% below it.
- **Ranking:** highest ratio first. Exact ties at the cut (names sitting on their high, such as Chinese limit-up closes) go to the larger market cap, then to the symbol.

On each rebalance date, for every line on the market's exchanges:

1. Keep it if the market cap on its latest `key_metrics` fiscal-year row dated at least 45 days before the rebalance date is above the market's floor. The row's date is the fiscal year end, not the date the figures were published.
2. Compute the ratio as above.
3. Keep one line per ISIN.
4. Take the 30 highest ratios and hold them at equal weight for the quarter.

## Parameters

- **Rebalancing:** quarterly, on 1 January, April, July and October, executed at the next available close. 103 quarters, January 2000 to July 2025; the last exit is 1 October 2025.
- **Min stocks:** 10. The quarter is held in cash if fewer than 10 names pass the screen, or if fewer than 10 of them pass the entry test: a price of at least 1.00 in local units in the 10-day entry window. The entry test reads `adjClose`, which is back-adjusted for every later dividend, not the `close` the ratio uses. In an invested quarter, a name that fails the entry test is dropped from the book and its slot isn't refilled from lower ranks, so a book can hold fewer than 30 names. A cash quarter earns 0%. The floor is checked at entry only.
- **Max stocks:** 30, equal weight.
- **Size floor:** the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`), in local currency: $1B in the US, ₹20B on NSE, ¥100B on JPX, ₩500B on KSC, NT$10B on TAI, ฿10B on SET, €500M on XETRA and MIL, £500M on the LSE.
- **Return cap:** a single holding's quarterly return above 200% is treated as a data error and left out of that quarter's average.
- **Transaction costs:** size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted.
- **Exits:** a holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes, preference shares and Thai NVDR lines have their own ISINs, so two lines of one company can both be held (see Known limits).
- **Benchmark:** each market's own index, in local currency. The US is measured against SPY. South Africa, Italy and Malaysia have no local index in the data and are measured against the S&P 500 (SPY) in USD. China (SHZ + SHH) is measured against the SSE Composite, Norway against the Oslo All Share (`^OSEAX`).
- **Risk-free rate (Sharpe):** fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 0.5% Switzerland, 3.5% UK).

## Results (January 2000 to October 2025)

Of the 15 markets measured against their own index, the screen beat it in 11: Taiwan (+7.61pp a year), Hong Kong (+6.81), Korea (+4.84), Sweden (+4.80), Switzerland (+4.39), Thailand (+4.29), Norway (+3.60, measured 2013-2025), India (+3.00), the UK (+2.83), Canada (+2.16) and Singapore (+1.48). Germany ties the DAX (0.11pp behind, under the 0.2pp line). The US (-3.45pp against SPY), China (-0.93) and Japan (-0.84) trail. Every market was invested in at least half of its quarters, so none is too thin to test.

Three benchmarks include dividends, as the portfolio's returns do: SPY (US), the DAX (Germany) and the Oslo All Share GR (Norway). Norway's +3.60pp is like for like; Germany's tie and the US shortfall are too. The other twelve local indices are price indices, so those margins are overstated by roughly the index's dividend yield. India's +3.00, the UK's +2.83, Canada's +2.16 and Singapore's +1.48 are small enough that they may not survive a total-return comparison, and Japan and China would trail by more.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash quarters | Avg names |
|--------|-----------|------|----------------|-------------|--------|--------|---------------|-----------|
| Norway (OSL), 2013-2025 | Oslo All Share (`^OSEAX`) | 14.51% | 10.91% | +3.60 | 0.829 | -22.93% | 0 of 50 measured | 28.3 |
| India (NSE) | Sensex | 14.12% | 11.12% | +3.00 | 0.336 | -57.08% | 17 (2000-01 to 2004-01) | 29.8 |
| Taiwan (TAI) | TAIEX | 11.99% | 4.38% | +7.61 | 0.670 | -47.69% | 21 (2000-01 to 2005-01) | 30.0 |
| South Africa (JNB) | S&P 500 in USD | 10.00% | 8.02% | +1.99 | 0.067 | -36.62% | 18 (2000-01 to 2004-04) | 26.7 |
| Korea (KSC) | KOSPI | 9.64% | 4.81% | +4.84 | 0.354 | -42.92% | 20 (2000-01 to 2004-10) | 28.8 |
| Hong Kong (HKSE) | Hang Seng | 8.58% | 1.77% | +6.81 | 0.267 | -52.83% | 2 (2000-01, 2000-04) | 27.6 |
| Malaysia (KLS) | S&P 500 in USD | 8.50% | 8.02% | +0.48 | 0.529 | -34.26% | 24 (2000-01 to 2005-10) | 26.1 |
| Thailand (SET) | SET Index | 8.05% | 3.76% | +4.29 | 0.326 | -44.90% | 17 (2000-01 to 2004-01) | 29.3 |
| Sweden (STO) | OMX Stockholm 30 | 7.98% | 3.17% | +4.80 | 0.284 | -52.29% | 2 (2000-01, 2000-04) | 28.2 |
| Canada (TSX) | TSX Composite | 7.24% | 5.08% | +2.16 | 0.367 | -34.94% | 0 | 29.8 |
| Italy (MIL) | S&P 500 in USD | 6.51% | 8.02% | -1.51 | 0.196 | -58.72% | 17 (2000-01 to 2004-01) | 27.6 |
| Switzerland (SIX) | SMI | 6.50% | 2.10% | +4.39 | 0.354 | -48.70% | 1 (2000-01) | 27.9 |
| Germany (XETRA) | DAX | 5.01% | 5.12% | -0.11 | 0.154 | -47.94% | 0 | 28.6 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 4.57% | 8.02% | -3.45 | 0.173 | -53.96% | 0 | 29.7 |
| UK (LSE) | FTSE 100 | 4.19% | 1.36% | +2.83 | 0.050 | -39.02% | 0 | 29.9 |
| Singapore (SES) | STI | 3.65% | 2.17% | +1.48 | 0.074 | -52.89% | 14 (2000-01 to 2002-04, 2003-04 to 2004-01) | 17.6 |
| China (SHZ + SHH) | SSE Composite | 3.26% | 4.19% | -0.93 | 0.025 | -72.47% | 0 | 29.3 |
| Japan (JPX) | Nikkei 225 | 2.56% | 3.40% | -0.84 | 0.157 | -59.03% | 2 (2000-01, 2000-04) | 30.0 |

Returns are in local currency, after costs. Cash quarters are counted over the 103 rebalances and labelled by their rebalance month. Avg names is the average number of lines held in invested quarters; two lines of one company count as two. A figure below 30 means fewer than 30 names passed the screen, or the entry test (see Parameters) dropped some of them. The entry test dropped 40.2% of screened names in Singapore (17.6 held of 29.4 screened), 12.4% in Malaysia, 8.0% in Hong Kong, 7.1% in Italy and 2.3% in China. Norway's 28.3 is over its 50 measured quarters; the file's `avg_stocks_when_invested`, 24.4, averages all 74 invested rebalances of the run, including the 24 before its window.

- **US:** 4.57% a year against 8.02% for SPY, with a Sharpe of 0.173 against 0.361 and a maximum drawdown of -53.96% (2008-01-01 to 2009-07-01) against -43.86%. It was invested in all 103 quarters and beat SPY in 43.69% of them.
- **Norway** is measured from April 2013, when `^OSEAX` starts: 50 quarters, all invested. Its 29 cash quarters (2000-01 to 2006-01 and 2009-04 to 2010-01) fall before that and aren't in its figures. Its CAGR covers 12.5 years, not 25.8, so it doesn't rank against the other rows.
- **South Africa, Italy, Malaysia** set a local-currency return against a USD index, so their excess mixes in the currency move and isn't a verdict either way.
- **Cash quarters** are early: apart from Norway's, which fall before its measured window, the last one in any market is 2005-10 (Malaysia). In every market but three, all of them are quarters when fewer than 10 names passed the screen. Italy's 2003-04 to 2004-01 (8 or 9 of 10 to 12 names buyable), Malaysia's 2005-10 (8 of 12) and Singapore's 2001-04 to 2002-04 and 2003-04 to 2004-01 (5 to 9 of 11 to 21) are quarters when 10 or more passed but fewer than 10 passed the entry test (see Parameters).
- **Singapore** holds 17.6 names on average in its 89 invested quarters, though 29.4 pass the screen. The entry test drops 1,053 of its 2,620 screened names (40.2%), at 35% to 48% in each five-year stretch, and the dropped slots aren't refilled (2024-10: 30 screened, 16 held). The test reads the dividend-adjusted price, so how many of those names had a printed close under S$1 isn't determined from these files.
- **China's drawdown**, -72.47%, runs from October 2007 to January 2025: the book did not regain its October 2007 value for 69 quarters. The SSE Composite's own maximum is -66.96%.
- **Sharpe:** compare it with the index's Sharpe in the same market, not across markets, because the risk-free rate differs (6.5% in India, 0.1% in Japan).

## Rerun October 2026

These results replace a run from June 2026. The whole topic was rerun with the campaign's changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias), and four changes to this screen:

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the quarter. 176 holdings in 16 markets exited this way (62 in the US); 8 had no bar to exit at. Each results file lists them under its `ltp` key. On identical selection the exit rule moves CAGR by -0.21pp (Japan) to +0.05pp (US).
- **Listing rules and same-ISIN dedup**, as under Parameters. The dedup runs before the top-30 cut, so a removed twin frees its slot.
- **Entry-side cash rule.** A quarter is cash when fewer than 10 screened names pass the entry test (an entry `adjClose` of at least 1.00, see Parameters). The June run decided cash on the screen count alone.
- **Price basis of the ratio.** The June run divided `adjClose` by the 52-week high. `adjClose` is back-adjusted for every later dividend, so a company scored lower the more it paid out after the rebalance date: the ranking used future dividends and shut dividend payers out. The ratio is now `close` over the high, both on the same basis. Before the rerun this was measured to change 45.4% of book slots across the 18 markets, from 4.9% in Norway to 76.8% in the US.
- **Signal bar.** The June run took the latest bar up to 5 days after the rebalance date, while entry is the first close after it, so the first days of a rise counted towards both the ranking and the return. The signal bar is now the last traded bar in the 10 days up to the rebalance date. Lines that didn't trade (fund share classes, suspended stocks) had sat exactly on their high and won ties; the traded-bar rule changes 4.2% of holdings.
- **Ties** at the cut go to the larger market cap, then the symbol.

The selection changes drive the large moves, and no run separates them. The US fell from 12.52% to 4.57% (from 4.51pp ahead of SPY to 3.45pp behind), Japan from 8.43% to 2.56% (from 5.03pp ahead of the Nikkei to 0.84pp behind), India from 18.96% to 14.12%, Germany from 8.23% to 5.01% (from 3.11pp ahead of the DAX to a tie), Korea from 12.07% to 9.64% and Thailand from 10.47% to 8.05%. China rose from -2.25% to 3.26%, Taiwan from 9.18% to 11.99%, Switzerland from 4.47% to 6.50% and Malaysia from 6.42% to 8.50%. Cash quarters rose by 2 in Korea and 3 in Thailand, and by 4 in Italy, 9 in Singapore and 1 in Malaysia (all from the entry-side rule); Switzerland's fell by 2. Benchmark returns are unchanged in all 18 markets.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market.

The committed files were made one market at a time, each with an explicit `--output`:

```bash
# One market, written to the committed results file
python3 52-week-high/backtest.py --exchange XETRA --output 52-week-high/results/returns_XETRA.json
python3 52-week-high/backtest.py --preset us --output 52-week-high/results/returns_NYSE_NASDAQ_AMEX.json

# After all 18: rebuild the comparison file the charts read, then check it against the per-market files
python3 scripts/rebuild_comparison.py 52-week-high
python3 scripts/rebuild_comparison.py 52-week-high --check

# Charts from results/exchange_comparison.json, written to 52-week-high/charts/
python3 52-week-high/generate_charts.py

# The old exit rule (doesn't reproduce the June results: the other changes above still apply)
python3 52-week-high/backtest.py --exchange LSE --exit-method drop

# Current screen (see Known limits)
python3 52-week-high/screen.py --preset us
```

| Market | Arguments | Output file | Listing rule |
|--------|-----------|-------------|--------------|
| US | `--preset us` | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `--exchange NSE` | `returns_NSE.json` | |
| UK | `--exchange LSE` | `returns_LSE.json` | GBp lines only |
| Germany | `--exchange XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Japan | `--exchange JPX` | `returns_JPX.json` | |
| China | `--exchange SHZ,SHH` | `returns_SHZ_SHH.json` | |
| Hong Kong | `--exchange HKSE` | `returns_HKSE.json` | |
| Korea | `--exchange KSC` | `returns_KSC.json` | |
| Taiwan | `--exchange TAI` | `returns_TAI.json` | |
| Canada | `--exchange TSX` | `returns_TSX.json` | |
| Switzerland | `--exchange SIX` | `returns_SIX.json` | Switzerland-domiciled only |
| Sweden | `--exchange STO` | `returns_STO.json` | |
| Thailand | `--exchange SET` | `returns_SET.json` | |
| South Africa | `--exchange JNB` | `returns_JNB.json` | |
| Norway | `--exchange OSL` | `returns_OSL.json` | |
| Italy | `--exchange MIL` | `returns_MIL.json` | |
| Malaysia | `--exchange KLS` | `returns_KLS.json` | |
| Singapore | `--exchange SES` | `returns_SES.json` | |

The arguments set the universe name written into the file: `--preset us` writes `US_MAJOR`, as the committed file has. `--preset canada` adds TSXV and `--preset taiwan` adds TWO, so neither reproduces the TSX or TAI file.

`--global` loops over the same 18 exchange sets and writes `returns_<exchanges>.json` next to the `--output` path, plus a comparison file. It isn't how these files were made: its US file carries the universe name `NYSE_NASDAQ_AMEX`, and a market that fails inside the loop is recorded as an error entry while its old file stays on disk. Run the `--check` above after any rerun.

## Known limits

- **Survivorship.** The price data holds almost no company that stopped trading before 2015 (US) or 2020 (other markets), so the earlier years are measured on companies that survived at least that long. Last-traded-price exits can only capture companies that stopped trading after those dates. See METHODOLOGY.md (Limitations: Survivorship Bias).
- **Not only ordinary shares.** The screen has no instrument filter. Income lines (funds, preferred shares, notes, REITs) sit near their 52-week high much of the time, and Thai NVDR lines rank beside their ordinary lines. Each period record lists the first 10 names held, in rank order. Among those, by ticker pattern and name (not by a fund flag in the data), the share of slots held by funds, ETFs, preferred or note lines, warrants and NVDRs is: Thailand 27.4% (NVDR lines 216 of 860, infrastructure and property funds 20), Korea 20.7% (preferred lines, 172 of 830), the UK 20.4% (investment trusts 182 of 1,030, preference shares 28), Canada 19.4% (preferred lines 185 of 1,030, closed-end funds and bullion trusts 15), the US 9.1% (closed-end funds 46 of 1,030, notes and debentures 26, preferred lines 19, warrants 3) and Germany 8.9% (preference shares, 92 of 1,030). REIT and trust units, not counted there, are 16.4% of Singapore's recorded slots. The effect of these lines on the results isn't measured.
- **Two lines of one company.** One line per ISIN isn't one line per company. Among the recorded names, at least one company has two lines in the book in 81 quarters in Sweden (A and B shares), 73 in Thailand (ordinary and NVDR), 50 in Korea (ordinary and preferred, or two preferred series), 40 in Canada, 37 in Germany (HEN.DE with HEN3.DE in 20 of them) and 28 in Switzerland (Roche's RO.SW with ROG.SW in 24, Swatch's UHR.SW with UHRN.SW in 16). Avg names counts lines.
- **Size floor.** The floor compares `key_metrics` market cap, in the company's reporting currency, with a floor set in the listing currency, and takes the latest filing at least 45 days old with no upper age limit.
- **Entry floor.** The 1.00 entry test reads `adjClose`, which is back-adjusted for every dividend paid after the rebalance date, so it sits below the printed close, by more the earlier the quarter and the higher the payout. A name whose printed close was above 1.00 fails the test when later dividends push its adjusted price under 1.00. That is the same dependence on later dividends the rerun removed from the ratio. Here it decides which screened names are held and, in 14 quarters (Italy 4, Malaysia 1, Singapore 9), whether the book is invested at all. In invested quarters the test drops 40.2% of screened names in Singapore, 12.4% in Malaysia, 8.0% in Hong Kong, 7.1% in Italy, 2.3% in China and 1.1% or less in the other 13 markets (none in Japan, Korea, Taiwan or Germany). These counts include the rare name with no entry bar in the window. How many dropped names had a printed close above 1.00, and what that does to the results, isn't measured.
- **Charts.** The CAGR and drawdown comparison charts leave Norway out, because its CAGR and drawdown cover 2013-2025 only, and name it in a footnote. Its own growth chart is titled "measured 2013-2025", and its "Cash" footer counts the 50 measured quarters (0%).
- **`screen.py`** (the current-day screen) still ranks on `adjClose / MAX(high)`, with no traded-bar rule, no listing rules and no dedup. The backtest doesn't use it.

## Data Requirements

- `stock_eod`: close, high and volume for the signal; adjClose for the 1.00 entry test, for entry and exit returns, and for the last traded price at exit
- `key_metrics` (FY): marketCap
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup tie-break)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen (see Known limits) |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py`: growth and yearly returns for each market, plus CAGR and maximum drawdown across markets. Chart files aren't committed, so run it locally |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
