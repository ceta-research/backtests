# 52-Week Low Quality Strategy

Screen for financially strong companies trading near their 52-week lows. The combination of price depression and high Piotroski F-score identifies stocks that have sold off for non-fundamental reasons rather than deteriorating business quality.

## Strategy Logic

**Entry signal:**
- Price within 15% of 52-week low: `(price - low_52w) / low_52w ≤ 0.15`
- Piotroski F-score ≥ 7 (out of 9 possible points)
- Market cap above exchange-specific threshold

**Portfolio construction:**
- Equal weight, quarterly rebalancing (January, April, July, October)
- Up to 30 stocks, sorted by proximity to 52-week low (most depressed first)
- Cash if fewer than 5 stocks qualify

## Piotroski F-Score Components

The F-score filters out value traps — stocks cheap for a reason. Each component is 0 or 1:

**Profitability (4 points)**
| Component | Condition |
|-----------|-----------|
| F1: Positive net income | Net income > 0 |
| F2: Positive cash flow | Operating CF > 0 |
| F3: Improving ROA | ROA(t) > ROA(t-1) |
| F4: Accruals quality | OCF/Assets > NI/Assets |

**Leverage & Liquidity (3 points)**
| Component | Condition |
|-----------|-----------|
| F5: Decreasing leverage | LT debt/assets falling |
| F6: Improving liquidity | Current ratio rising |
| F7: No dilution | Equity not decreasing |

**Operating Efficiency (2 points)**
| Component | Condition |
|-----------|-----------|
| F8: Improving asset turnover | Revenue/Assets ratio rising |
| F9: Expanding gross margin | Gross profit margin rising |

Score ≥ 7: strong financial health, unlikely to deteriorate further.

## Academic Basis

- **Mean reversion**: De Bondt & Thaler (1985) — past losers tend to outperform past winners over 3-5 year horizons. Stocks near 52-week lows exhibit above-average return potential.
- **Quality filter**: Piotroski (2000) — F-score ≥ 7 separates financial winners from losers within a value universe, reducing exposure to value traps.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest, 2002-2025 |
| `screen.py` | Live screen with current qualifying stocks |
| `generate_charts.py` | Generate charts from backtest results |
| `results/` | JSON/CSV output from backtests |
| `charts/` | PNG charts for blog posts |

## Usage

```bash
# Run US backtest
python3 52-week-low/backtest.py --preset us --output results/returns_US_MAJOR.json --verbose

# Run global comparison
python3 52-week-low/backtest.py --global --output results/exchange_comparison.json --verbose

# Current stock screen
python3 52-week-low/screen.py --preset us

# Generate charts (after running backtest)
python3 52-week-low/generate_charts.py
```

## Data Requirements

- `income_statement` (FY annual): net income, gross profit, revenue
- `balance_sheet` (FY annual): total assets, current assets/liabilities, LT debt, equity
- `cash_flow_statement` (FY annual): operating cash flow
- `key_metrics` (FY annual): market cap
- `stock_eod` (daily): adjusted close prices (full history for 52-week low computation)

*Data: Ceta Research (FMP financial data warehouse)*
