# Industry Leader Strategy

Pick the 3 largest companies by revenue in each industry showing at least 5% year-over-year aggregate revenue growth. Annual rebalance. Equal weight.

## The Signal

Two conditions must be true at each July rebalance:

1. **Growing industry**: The average YoY revenue growth across companies in that industry is ≥ 5%. Industries with declining or flat revenues are excluded.
2. **Revenue leader**: Within qualifying industries, rank all companies by current FY revenue. Hold the top 3.

The result is a portfolio of industry leaders in sectors actively growing, rebalanced each July.

## Why It Works (or Doesn't)

Industry revenue leaders tend to have defensible market positions — pricing power, scale advantages, and customer stickiness. Companies with the most revenue in a growing industry are likely compounding that growth, not just riding the tide.

The "growing industry" filter removes exposure to secular decline — companies that happen to be the largest in a shrinking market.

The honest counterargument: when you hold top-3-by-revenue across 100+ industries, you're running close to an equal-weighted large-cap index. The alpha depends on whether revenue leaders systematically compound faster than the rest.

## Academic Basis

Related to competitive moat theory (Porter 1980), industry momentum research (Moskowitz & Grinblatt 1999), and revenue leadership as a proxy for competitive advantage.

## Parameters

| Parameter | Value | Reasoning |
|-----------|-------|-----------|
| Rebalancing | Annual (July) | After most FY filings (45-day lag) |
| Industry growth min | 5% avg YoY | Filter declining industries |
| Min industry size | 3 companies | Enough to identify a leader |
| Leaders per industry | 3 | Concentrated but not single-stock |
| Market cap min | Exchange-specific | Liquidity filter |
| Max portfolio | 300 stocks | Prevents over-diversification |
| Transaction costs | Size-tiered | Via costs.py |

## Usage

```bash
# US stocks (default)
python3 industry-leader/backtest.py

# US with output
python3 industry-leader/backtest.py --preset us --output industry-leader/results/returns_US_MAJOR.json --verbose

# All exchanges
python3 industry-leader/backtest.py --global --output industry-leader/results/exchange_comparison.json --verbose

# Current screen
python3 industry-leader/screen.py --preset us

# India
python3 industry-leader/backtest.py --preset india --verbose
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest |
| `screen.py` | Current qualifying stocks |
| `generate_charts.py` | Charts for blog posts |
| `results/` | Output files (gitignored) |
| `charts/` | Generated charts (gitignored) |

## Data Source

FMP financial data via Ceta Research warehouse. Income statement (FY), key metrics (FY), price data (EOD).

See [METHODOLOGY.md](../METHODOLOGY.md) for backtest framework details.
