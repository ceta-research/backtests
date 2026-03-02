# Classic Low P/E Strategy

Traditional value investing based on the academic evidence that stocks with low price-to-earnings ratios tend to outperform over long periods. The strategy adds profitability and balance sheet safety filters to avoid value traps.

**Academic basis:** Basu (1977), Fama-French (1992), Lakonishok-Shleifer-Vishny (1994)

## Signal

Four filters, ranked by lowest P/E:

| Filter | Threshold | Purpose |
|--------|-----------|---------|
| P/E Ratio | 0 < P/E < 15 | Classic value: cheap relative to earnings. Excludes negative P/E (unprofitable). |
| Return on Equity | > 10% | Profitability floor. Avoids cheap-but-dying companies. |
| Debt-to-Equity | < 1.0 | Balance sheet safety. Moderate leverage limit. |
| Market Cap | > $1 billion | Liquidity. Avoids micro-cap illiquidity. |

**Portfolio construction:** Top 30 stocks by lowest P/E. Equal weight. Cash if fewer than 10 qualify.

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Set your API key
export CR_API_KEY="your_key"

# Screen current stocks (live TTM data)
python3 low-pe/screen.py

# Run historical backtest (US stocks, quarterly, 2000-2025)
python3 low-pe/backtest.py

# Backtest on different exchanges
python3 low-pe/backtest.py --exchange BSE,NSE      # India
python3 low-pe/backtest.py --preset japan           # Japan
python3 low-pe/backtest.py --global                 # All exchanges

# Custom parameters
python3 low-pe/backtest.py --frequency semi-annual --risk-free-rate 0.0 --no-costs
python3 low-pe/backtest.py --output results.json --verbose
```

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Historical backtest (2000-2025). Uses shared modules for metrics, costs, and CLI. |
| `screen.py` | Live screen using current TTM data. Single SQL query, fast. |
| `generate_charts.py` | Regenerate all charts from results. Cumulative growth, annual returns, cross-exchange comparison. |
| `results/` | Pre-computed backtest results (JSON metrics + CSV returns per exchange). |
| `charts/` | Generated charts (PNG, 200 DPI). |

## Data Source

By default, scripts use the [Ceta Research](https://cetaresearch.com) SQL API. The data covers:
- Financial ratios (P/E, D/E) from FMP financial_ratios table
- Key metrics (ROE, market cap) from FMP key_metrics table
- Daily prices (adjusted) from FMP stock_eod table
- 72 global exchanges, 1985-present (varies by exchange)

Configure the data source by setting `CR_API_KEY` or passing `--api-key`.

## References

- Basu, S. (1977). "Investment Performance of Common Stocks in Relation to Their Price-Earnings Ratios." *Journal of Financial Economics*
- Fama, E. F., & French, K. R. (1992). "The Cross-Section of Expected Stock Returns." *Journal of Finance*
- Lakonishok, J., Shleifer, A., & Vishny, R. W. (1994). "Contrarian Investment, Extrapolation, and Risk." *Journal of Finance*

See [METHODOLOGY.md](../METHODOLOGY.md) for complete backtesting methodology.
