# PEG Ratio (GARP) Strategy

**Signal:** PEG < 1.0, P/E 8-30, ROE > 12%, D/E < 1.5, Market Cap > $1B
**Portfolio:** Top 30 by lowest PEG, equal weight
**Rebalancing:** Quarterly (Jan/Apr/Jul/Oct), 2000-2025
**Academic reference:** Lynch, Peter. *One Up on Wall Street* (1989)

GARP = Growth at a Reasonable Price. PEG < 1 means the stock price is cheaper than its
earnings growth rate warrants. Peter Lynch's core investment thesis from his 29%/yr run
at Fidelity Magellan.

## Setup

```bash
pip install -r ../requirements.txt
export CR_API_KEY=your_key_here
```

## Usage

```bash
# Current stock screen (TTM data, live)
python3 peg-ratio/screen.py
python3 peg-ratio/screen.py --preset india

# Backtest US stocks
python3 peg-ratio/backtest.py
python3 peg-ratio/backtest.py --verbose

# Backtest a specific exchange
python3 peg-ratio/backtest.py --preset india --output results/returns_India.json

# All exchanges
python3 peg-ratio/backtest.py --global --output results/exchange_comparison.json --verbose
```

## Signal Logic

```sql
SELECT symbol
FROM financial_ratios_ttm f
JOIN key_metrics_ttm k ON f.symbol = k.symbol
WHERE f.priceToEarningsGrowthRatioTTM > 0
  AND f.priceToEarningsGrowthRatioTTM < 1.0   -- PEG below 1 = paying less than growth warrants
  AND f.priceToEarningsRatioTTM BETWEEN 8 AND 30  -- Exclude distressed and speculative
  AND k.returnOnEquityTTM > 0.12               -- Quality: 12%+ ROE
  AND f.debtToEquityRatioTTM < 1.5             -- Leverage: manageable D/E
  AND k.marketCap > 1000000000                 -- $1B+ market cap
ORDER BY f.priceToEarningsGrowthRatioTTM ASC
LIMIT 30
```

## Exchanges Tested

See `results/exchange_comparison.json` after running `--global`.

Known exclusions (data quality):
- JPX (Japan), LSE (UK): No FY financial data in warehouse
- ASX (Australia), SAO (Brazil): Broken split-adjusted prices (see backtests/DATA_QUALITY_ISSUES.md)

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
