# Altman Z-Score Safety

Bankruptcy risk screening applied as an investment signal. Stocks in the "safe zone" (Z > 2.99) are held in an equal-weight portfolio, rebalanced annually. The strategy tests whether avoiding financially distressed companies generates alpha.

**Academic basis:** Altman (1968) "Financial Ratios, Discriminant Analysis and the Prediction of Corporate Bankruptcy", Dichev (1998) "Is the Risk of Bankruptcy a Systematic Risk?"

## Signal

The Altman Z-Score is computed from five financial ratios:

```
Z = 1.2*(WC/TA) + 1.4*(RE/TA) + 3.3*(EBITDA/TA) + 0.6*(MktCap/TL) + 1.0*(Rev/TA)
```

| Zone | Threshold | Interpretation |
|------|-----------|---------------|
| Safe | Z > 2.99 | Low bankruptcy probability |
| Gray | 1.81 - 2.99 | Uncertain |
| Distress | Z < 1.81 | High bankruptcy probability |

**Portfolio construction:** Equal weight all safe-zone stocks. Four tracks tested: Safe, Gray, Distress, All-ex-Distress.
**Rebalancing:** Annual (April 1), 2000-2025.
**Benchmark:** S&P 500 (SPY).
**Exclusions:** Financial Services, Utilities, stocks below exchange-specific market cap threshold.

## Key Findings

The signal works best in emerging markets and fails in the US:

| Exchange | Safe CAGR | Distress CAGR | Spread | SPY CAGR |
|----------|-----------|---------------|--------|----------|
| India | 10.7% | 3.4% | +7.3% | 7.3% |
| Brazil | 11.2% | 5.9% | +5.3% | 7.3% |
| US | 4.2% | 6.0% | -1.8% | 7.3% |

In the US, the gray zone (7.99% CAGR) actually outperforms safe-zone stocks. The Z-Score was designed for US manufacturing firms in the 1960s, and its predictive power has eroded in developed markets with better access to credit and restructuring options.

In emerging markets (India, Brazil), bankruptcy risk remains a real hazard, so the signal retains value.

## Usage

```bash
# Set your API key
export CR_API_KEY="your_key"

# Screen current safe-zone stocks (US default)
python3 altman-z/screen.py

# Advanced screen: Z>3 + Piotroski>=5 + ROE>0 + D/E<1.5
python3 altman-z/screen.py --advanced

# Screen Indian stocks
python3 altman-z/screen.py --preset india

# Full 25-year backtest (US)
python3 altman-z/backtest.py

# Backtest Indian stocks with verbose output
python3 altman-z/backtest.py --preset india --verbose

# Backtest all exchanges
python3 altman-z/backtest.py --global --output results/exchange_comparison.json

# Generate charts from results
python3 altman-z/generate_charts.py
python3 altman-z/generate_charts.py --exchange India
python3 altman-z/generate_charts.py --all
```

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Historical backtest (2000-2025). Computes Z-Scores from raw FY financials, four portfolio tracks. |
| `screen.py` | Live screen using pre-computed `altmanZScore` from scores table. Simple and advanced modes. |
| `generate_charts.py` | Generate charts from per-exchange result JSONs. Cumulative growth, annual returns, cross-exchange comparison. |
| `results/` | Pre-computed backtest results (`altman_z_metrics_{exchange}.json`). |
| `charts/` | Generated charts (PNG, 200 DPI). |

## Data Source

Scripts use the [Ceta Research](https://cetaresearch.com) SQL API. The backtest fetches raw financial statements (balance sheet, income statement, key metrics) and computes Z-Scores directly. The screen uses pre-computed scores from the `scores` table.

Configure the data source by setting `CR_API_KEY` or passing `--api-key`.

## References

- Altman, E. I. (1968). "Financial Ratios, Discriminant Analysis and the Prediction of Corporate Bankruptcy." *Journal of Finance*, 23(4), 589-609.
- Dichev, I. D. (1998). "Is the Risk of Bankruptcy a Systematic Risk?" *Journal of Finance*, 53(3), 1131-1147.
- Griffin, J. M., & Lemmon, M. L. (2002). "Book-to-Market Equity, Distress Risk, and Stock Returns." *Journal of Finance*, 57(5), 2317-2336.

See [METHODOLOGY.md](../METHODOLOGY.md) for complete backtesting methodology.
