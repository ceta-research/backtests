# High Yield + Quality

Dividend yield screen with Piotroski-inspired quality filters. Combines high yield (>2%) with financial health checks (ROA, leverage, liquidity, payout sustainability) to avoid yield traps.

## Strategy

**Signal:** Stocks with dividend yield above 2% that pass four quality filters:
- Return on Assets > 5% (profitability)
- Current Ratio > 1.0 (liquidity)
- Debt/Equity < 1.5 (leverage)
- Dividend Payout Ratio < 80% (sustainability)

**Selection:** Top 30 by dividend yield (descending), equal weight.

**Rebalancing:** Semi-annual (April and October).

**Min stocks:** 10 (holds cash if fewer qualify).

## Academic basis

- Arnott, Hsu & Moore (2005): Dividend-weighted portfolios outperform cap-weighted by ~2-3% annually
- Piotroski (2000): Financial health metrics separate winners from losers among cheap stocks
- Fama & French (2001): Dividend payers are increasingly a select group; quality varies widely within payers

## Usage

```bash
# Backtest US stocks
python3 high-yield-quality/backtest.py

# Backtest with specific exchange
python3 high-yield-quality/backtest.py --preset india
python3 high-yield-quality/backtest.py --exchange XETRA

# Current stock screen
python3 high-yield-quality/screen.py
python3 high-yield-quality/screen.py --preset india

# Save results
python3 high-yield-quality/backtest.py --output results/us.json --verbose
```

## Data sources

- `financial_ratios` (FY): dividendYield, dividendPayoutRatio, debtToEquityRatio, currentRatio
- `key_metrics` (FY): marketCap, returnOnAssets
- `stock_eod`: adjClose prices at rebalance dates
- `profile`: exchange membership

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current qualifying stocks (TTM data) |
| `results/` | Computed backtest results (JSON per exchange) |
