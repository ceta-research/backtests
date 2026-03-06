# Asset Growth Anomaly

Companies that grow their assets slowly tend to outperform those expanding aggressively. This strategy screens for quality companies with low total asset growth.

## Academic Basis

Cooper, Gulen & Schill (2008), "Asset Growth and the Cross-Section of Stock Returns", *Journal of Finance* 63(4), 1609-1651. Found that firms in the lowest asset growth decile outperformed the highest decile by ~20% annually over 1968-2003.

## Signal

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| Asset Growth (YoY) | -20% to +10% | Low growth, not distressed |
| Return on Equity | > 8% | Profitable |
| Return on Assets | > 5% | Capital efficient |
| Operating Profit Margin | > 10% | Operationally strong |
| Market Cap | > $500M | Liquid, investable |

**Sorting:** Lowest asset growth first (ascending). Top 30.

**Asset Growth** = (Total Assets current FY - Total Assets prior FY) / Total Assets prior FY

## Parameters

- **Rebalancing:** Annual (July)
- **Min stocks:** 10 (holds cash if fewer qualify)
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Transaction costs:** Size-tiered (0.1% mega-cap to 0.5% mid-cap, round-trip)
- **Data lag:** 45 days (point-in-time)

## Usage

```bash
# US stocks (default)
python3 asset-growth/backtest.py --verbose

# India
python3 asset-growth/backtest.py --preset india --output results/returns_BSE_NSE.json

# All exchanges
python3 asset-growth/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen
python3 asset-growth/screen.py
python3 asset-growth/screen.py --preset india
```

## Data Requirements

- `balance_sheet` (FY): totalAssets (two consecutive years)
- `key_metrics` (FY): returnOnEquity, returnOnAssets, marketCap
- `financial_ratios` (FY): operatingProfitMargin
- `stock_eod`: adjClose at rebalance dates
- `profile`: exchange membership

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen (TTM data) |
| `README.md` | This file |
| `results/` | Output directory for backtest results |
