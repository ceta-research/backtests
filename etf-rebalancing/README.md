# ETF Rebalancing Drag

Buy quality stocks with the lowest fraction of their market cap owned by ETFs, avoiding the hidden reconstitution drag that costs index investors 20-28 basis points annually.

## Signal

**ETF Ownership Ratio** = Total ETF market value in stock / Stock market cap

Lower ratio = less of the stock's float is controlled by passive/ETF money = less forced trading during index reconstitution events.

### Filters
- ROE > 10% (profitable, quality business)
- P/E between 0 and 40 (exclude loss-makers and extreme growth)
- Market cap above exchange-specific threshold
- Held by at least 1 ETF (has some institutional visibility)
- Bottom 30 by ownership ratio (least rebalancing exposure)

### Distinction from ETF Anti-Crowding (etf-02)

| | etf-02 Anti-Crowding | etf-04 Rebalancing Drag |
|---|---|---|
| Signal | Raw ETF count (fewest ETFs) | ETF ownership ratio (lowest % of market cap) |
| Measures | Breadth of ETF coverage | Dollar magnitude of passive exposure |
| Example | Stock held by 3 ETFs = low count | Stock where ETFs own 0.1% of market cap = low ratio |

A stock could be held by 50 ETFs with tiny positions (low ratio, high count) or 3 ETFs with large positions (high ratio, low count). The ownership ratio captures the actual rebalancing pressure more accurately.

## Parameters

- Rebalancing: Annual (July)
- Period: 2005-2025
- Weighting: Equal weight
- Min stocks: 10 (cash if fewer qualify)
- Max stocks: 30
- Transaction costs: Size-tiered (0.1-0.5% one-way)

## Academic Basis

- Petajisto (2011): Index reconstitution costs S&P 500 investors 20-28 bps/year
- Chen, Noronha & Singal (2004): S&P 500 additions +3.5%, deletions -8.8%

## Data Caveat

ETF ownership data (`etf_holder`) is a current snapshot, not historical. Ownership ratios are applied retrospectively. Quality filters (ROE, P/E, market cap) use point-in-time FY data with 45-day lag.

## Usage

```bash
# US backtest
python3 etf-rebalancing/backtest.py --preset us --verbose

# All exchanges
python3 etf-rebalancing/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen
python3 etf-rebalancing/screen.py --preset us
```

## Files

- `backtest.py` - Historical backtest (2005-2025)
- `screen.py` - Current qualifying stocks
- `results/` - Backtest output (JSON + CSV)

*Data: Ceta Research (FMP financial data warehouse).*
