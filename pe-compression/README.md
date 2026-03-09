# P/E Compression (Mean Reversion)

Screen for stocks where the current P/E ratio has compressed 15%+ below its 5-year historical average while fundamentals remain stable. The thesis: valuation mean reversion — if a quality company's earnings multiple compresses without a corresponding deterioration in fundamentals, it's likely to mean-revert upward.

## Signal

```
Current FY P/E < 85% of prior 5-year average FY P/E  [15%+ compression]
P/E > 5                                               [positive earnings, not distressed]
P/E < 40                                              [not speculative]
ROE > 10%                                             [fundamental quality]
D/E < 2.0                                             [manageable leverage]
Market cap > exchange-specific threshold              [liquidity filter]
Minimum 3 prior years of P/E history required
```

**Ranking:** Top 30 by lowest (current P/E / avg P/E) ratio — most compressed first.

## Academic Basis

De Bondt & Thaler (1985) documented mean reversion in stock prices after extreme valuation moves. Stocks that underperform over 3-5 years tend to outperform in the subsequent 3-5 years. P/E compression — where a stock's earnings multiple contracts relative to its own history while fundamentals stay intact — is a direct proxy for this effect.

## Parameters

| Parameter | Value |
|-----------|-------|
| Universe | Full exchange (not index-constrained) |
| Compression threshold | < 85% of 5-year historical average |
| P/E range | 5 – 40 |
| ROE filter | > 10% |
| D/E filter | < 2.0 |
| Min history | 3 prior years of FY P/E data |
| Rebalancing | Annual (January) |
| Portfolio size | Top 30, equal weight |
| Cash trigger | < 10 qualifying stocks |
| Transaction costs | Size-tiered (from costs.py) |
| Data lag | 45 days (annual filing) |

## Usage

```bash
# US backtest (default)
python3 pe-compression/backtest.py

# India backtest
python3 pe-compression/backtest.py --preset india --verbose

# All exchanges
python3 pe-compression/backtest.py --global --output results/exchange_comparison.json --verbose

# No costs (academic baseline)
python3 pe-compression/backtest.py --no-costs

# Current stock screen
python3 pe-compression/screen.py
python3 pe-compression/screen.py --preset india

# Generate charts (after running backtest --global)
python3 pe-compression/generate_charts.py
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current stock screen (live data) |
| `generate_charts.py` | Chart generation from results/ |
| `results/exchange_comparison.json` | All exchange results |
| `results/returns_{EXCHANGE}.json` | Per-exchange results |
| `charts/` | Generated PNG charts (gitignored) |

## Data Source

Ceta Research — FMP financial data warehouse.
- `financial_ratios` (FY): historical P/E ratios (all years)
- `key_metrics` (FY): ROE, market cap
- `stock_eod`: adjusted close prices

## Excluded Exchanges

- **ASX**: Price data artifact (unadjusted splits), produces >50% CAGR artifacts
- **SAO (Brazil)**: Same price data artifact

See `backtests/DATA_QUALITY_ISSUES.md` for full documentation.
