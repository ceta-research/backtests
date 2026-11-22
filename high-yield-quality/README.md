# High Yield + Quality

Dividend yield screen with quality filters. Combines a high yield (4% to 15%) with checks on profitability, free cash flow, leverage and payout sustainability to avoid yield traps.

## Strategy

**Signal:** Stocks with a dividend yield from 4% to 15% that pass these filters:
- Return on Equity > 8% (profitability)
- Free cash flow > 0 (cash generation)
- Debt/Equity from 0 to under 2.0 (leverage)
- Dividend Payout Ratio above 0 and under 80% (sustainability)
- Market cap above the exchange's floor (`cli_utils.get_mktcap_threshold`)

**Selection:** Top 30 by dividend yield (descending), equal weight.

**Rebalancing:** Annual, each January.

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

# Save results
python3 high-yield-quality/backtest.py --output results/us.json --verbose
```

## Data sources

- `financial_ratios` (FY): dividendYield, dividendPayoutRatio, debtToEquityRatio
- `key_metrics` (FY): marketCap, returnOnEquity
- `cash_flow_statement` (FY): freeCashFlow
- `stock_eod`: adjClose prices at rebalance dates
- `profile`: exchange membership

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | An older current-stock screen (yield above 2%, ROA, current ratio), not the backtest's signal; it doesn't run as committed |
| `results/` | Computed backtest results (JSON per exchange) |
