# Oversold Quality

Buy fundamentally strong companies when they're technically oversold. Wait for mean reversion.

## Strategy

| Parameter | Value |
|-----------|-------|
| **Signal** | Piotroski F-Score >= 7 AND RSI-14 < 30 |
| **Rebalancing** | Quarterly (Jan/Apr/Jul/Oct) |
| **Portfolio** | Top 30 by lowest RSI, equal weight |
| **Min stocks** | 5 (hold cash if fewer qualify) |
| **Market cap** | Exchange-specific (see cli_utils.py) |
| **Transaction costs** | Size-tiered model |

## Logic

**Piotroski F-Score >= 7**: 9-factor accounting quality score. Measures profitability (F1-F3), leverage and liquidity (F4-F6), and operating efficiency (F7-F9). Score of 7+ = fundamentally strong company.

Unlike the standalone Piotroski strategy, this screen does NOT filter by P/B ratio. We want all quality companies, not just cheap ones. RSI provides the entry timing.

**RSI-14 < 30**: 14-period Relative Strength Index below 30 = technically oversold. At this level, average daily losses have exceeded average daily gains by more than 2.3:1 over two trading weeks. Historically, oversold conditions tend to reverse.

**Combined thesis**: Quality companies (Piotroski >= 7) with temporary selling pressure (RSI < 30) should mean revert. The quality filter prevents buying fundamentally weak stocks in structural decline.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full 2000-2025 historical backtest |
| `screen.py` | Live screen using TTM proxy + recent RSI |
| `generate_charts.py` | Generate all blog charts from results |
| `results/` | Backtest output (JSON/CSV) |
| `charts/` | Generated charts |

## Usage

```bash
# US backtest
python3 oversold-quality/backtest.py --verbose

# India
python3 oversold-quality/backtest.py --preset india --verbose

# All exchanges
python3 oversold-quality/backtest.py --global \
  --output oversold-quality/results/exchange_comparison.json --verbose

# Current screen (live data)
python3 oversold-quality/screen.py
python3 oversold-quality/screen.py --preset india

# Generate charts (after running --global)
python3 oversold-quality/generate_charts.py
```

## Expected Behavior

- **Cash periods expected**: RSI < 30 is a restrictive filter. In strong bull markets, few quality stocks are oversold. Cash periods reflect market conditions, not a bug.
- **Concentrated positions**: When few stocks qualify, the portfolio holds a small concentrated set. This increases volatility but also potential alpha.
- **Annual Piotroski + quarterly RSI**: The quality filter uses annual filings with a 45-day lag. RSI is computed from the 14 trading days before each quarterly rebalance.

## Academic References

- Piotroski, J.D. (2000). "Value Investing: The Use of Historical Financial Statement Information to Separate Winners from Losers." *Journal of Accounting Research*, 38.
- Wilder, J.W. (1978). *New Concepts in Technical Trading Systems*. Trend Research.
- Jegadeesh, N. (1990). "Evidence of Predictable Behavior of Security Returns." *Journal of Finance* — documents short-term return reversals that RSI < 30 attempts to capture.

## Data

All data from Ceta Research (FMP financial data warehouse). Survivorship-bias-reduced (includes delisted stocks). Market cap thresholds in local currency via `cli_utils.get_mktcap_threshold()`.
