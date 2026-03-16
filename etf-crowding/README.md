# ETF Anti-Crowding Strategy

Buy quality stocks with the **lowest ETF ownership**. Tests whether under-owned stocks outperform crowded names, based on academic research showing ETF ownership amplifies volatility and reduces price informativeness.

## Signal

1. **ETF crowding score**: Count distinct ETFs holding each stock (from `etf_holder`)
2. **Quality filters** (point-in-time FY data):
   - ROE > 10%
   - P/E between 0 and 40
   - Market cap > exchange-specific threshold
3. **Selection**: Bottom 30 stocks by ETF count among qualifying stocks (equal weight)
4. **Minimum**: Hold cash if fewer than 10 stocks qualify

## Parameters

| Parameter | Value |
|-----------|-------|
| Rebalancing | Annual (July) |
| Period | 2005-2025 |
| Min ETF count | 5 (filter invisible stocks) |
| Max portfolio size | 30 |
| Min portfolio size | 10 (cash otherwise) |
| Transaction costs | Size-tiered (0.1-0.5% one-way) |
| Benchmark | SPY (S&P 500) |

## Data Caveat

The `etf_holder` table contains **current snapshot data only** (no historical holdings). Crowding classifications are applied retrospectively across all backtest periods. This introduces look-ahead bias in the crowding signal. The quality filters (ROE, P/E, market cap) use point-in-time FY data and are free of look-ahead bias.

Results should be interpreted as: "How would a portfolio of currently under-owned quality stocks have performed historically?" This is an empirical study, not a fully point-in-time tradeable strategy.

## Academic References

- Ben-David, Franzoni & Moussawi (2018). "Do ETFs Increase Volatility?" *Journal of Finance*, 73(6), 2471-2535. (+16% volatility per SD of ETF ownership)
- Israeli, Lee & Sridharan (2017). "Is There a Dark Side to Exchange Traded Funds?" *Review of Accounting Studies*, 22(3). (Reduced price informativeness)
- Da & Shive (2018). "Exchange Traded Funds and Asset Return Correlations." *European Financial Review*, 22(6). (Increased pairwise correlations)

## Usage

```bash
# US stocks (NYSE + NASDAQ)
python3 etf-crowding/backtest.py

# India
python3 etf-crowding/backtest.py --preset india

# All exchanges
python3 etf-crowding/backtest.py --global --output results/exchange_comparison.json --verbose

# Without transaction costs
python3 etf-crowding/backtest.py --no-costs
```

## Exchange Coverage

AMEX excluded due to only 2.8% of ETF-held stocks having FY financial data. All other major exchanges included (NYSE, NASDAQ, BSE, NSE, JPX, LSE, XETRA, HKSE, KSC, TAI, TSX, ASX, STO, SIX, SAO, SET, SES, JNB, OSL, SHZ, SHH).

## Files

```
etf-crowding/
├── backtest.py          # Main backtest script
├── README.md            # This file
└── results/             # Generated results (JSON/CSV)
```
