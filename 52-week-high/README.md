# 52-Week High Proximity

Buys stocks closest to their 52-week high. George & Hwang (2004) showed that this signal — `adjClose / MAX(high over 252 trading days)` — explains much of what we call momentum, crashes less than classic 12-month momentum, and is driven by anchoring bias.

## Signal

```
proximity_ratio = adjClose / MAX(high over past 252 trading days)
```

A ratio of 1.0 means the stock is exactly at its 52-week high. A ratio of 0.95 means 5% below. Higher ratio = stronger signal.

**Why it works:** Investors treat the 52-week high as a psychological ceiling. When a stock approaches it, many hesitate or sell early, creating systematic underreaction. The eventual breakout produces predictable positive returns.

**Key advantage over classic momentum:** During bear markets, few stocks are near their highs, so the signal naturally fades. This reduces exposure to the momentum crashes that hurt 12-month strategies.

## Strategy

| Parameter | Value |
|-----------|-------|
| Signal | adjClose / MAX(high, 252 trading days) |
| Universe | Exchange-specific MCap threshold |
| Selection | Top 30 by proximity ratio, equal weight |
| Min stocks | 10 (cash if fewer qualify) |
| Rebalancing | Quarterly (Jan/Apr/Jul/Oct) |
| Data lag | None (price data is real-time) |
| Costs | Size-tiered (see `costs.py`) |
| Period | 2000–2025 |

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full multi-exchange historical backtest |
| `screen.py` | Live screen — current stocks nearest their 52-week high |
| `generate_charts.py` | Chart generation from `results/exchange_comparison.json` |
| `results/` | Backtest outputs (JSON, CSV) |
| `charts/` | Generated charts (gitignored; move to ts-content-creator) |

## Usage

```bash
cd backtests

# US default
python3 52-week-high/backtest.py

# Single exchange with verbose output
python3 52-week-high/backtest.py --preset india --verbose

# All exchanges
python3 52-week-high/backtest.py --global --output 52-week-high/results/exchange_comparison.json

# Live screen
python3 52-week-high/screen.py --preset us

# Generate charts (after running --global)
python3 52-week-high/generate_charts.py
```

## Academic References

- George, T. & Hwang, C. (2004). "The 52-Week High and Momentum Investing." *Journal of Finance*, 59(5), 2145-2176.
- Li, J. & Yu, J. (2012). "Investor Attention, Psychological Anchors, and Stock Return Predictability." *Journal of Financial Economics*, 104(2), 401-419.
- Jegadeesh, N. & Titman, S. (1993). "Returns to Buying Winners and Selling Losers." *Journal of Finance*, 48(1), 65-91.

## Data Quality Notes

- `high` column required from `stock_eod`. Data quality flags (sub-$1 adjClose, >200% single-period return) applied.
- Minimum 100 trading days required before computing proximity ratio (avoids early-data noise).
- Proximity > 1.1 treated as data artifact (adjClose exceeds computed 52-week high — can happen due to split adjustments).
- Exchanges excluded: ASX (adjClose/high artifacts), SAO (adjClose artifacts).
