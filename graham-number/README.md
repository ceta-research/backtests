# Graham Number Strategy

Buy stocks trading below their Graham Number — Benjamin Graham's fair value formula.

## Strategy

**Formula:** Graham Number = √(22.5 × EPS × BVPS)

Benjamin Graham derived this from his rule that a stock's P/E should be no more than 15 and P/B no more than 1.5. Since 15 × 1.5 = 22.5, the combined constraint becomes: P/E × P/B < 22.5.

**Signal:** Buy stocks where the current price is below their pre-computed Graham Number (i.e., P/E × P/B < 22.5 using annual filing data).

**Selection:** Top 30 by deepest discount (lowest price/Graham Number ratio), equal weight.

**Rebalancing:** Annual (January), 2000–2025.

## Filters

| Filter | Value | Reason |
|--------|-------|--------|
| price < Graham Number | Required | Trading below fair value |
| Market cap | Exchange-specific threshold | Liquidity filter |
| Min stocks | 10 | Hold cash if fewer qualify |
| Max stocks | 30 | Concentration limit |

Graham Number is pre-computed by FMP as `key_metrics.grahamNumber`. It is non-null only when EPS > 0 and BVPS > 0 (profitable company with positive book value).

## Usage

```bash
# Activate venv
source /path/to/.venv/bin/activate
cd backtests/

# Backtest US stocks
python3 graham-number/backtest.py

# Backtest with preset
python3 graham-number/backtest.py --preset india --output results/india.json

# Screen current stocks
python3 graham-number/screen.py

# Screen Indian stocks
python3 graham-number/screen.py --exchange BSE,NSE

# Run all exchanges
python3 graham-number/run_all_exchanges.py

# Generate charts (requires exchange_comparison.json)
python3 graham-number/generate_charts.py
```

## Academic Reference

Benjamin Graham, *The Intelligent Investor* (1949, revised 1973). The Graham Number formula was introduced as a simple fair value estimator combining earnings power (EPS) with asset value (BVPS).

## Data Source

Ceta Research (FMP financial data warehouse). `key_metrics.grahamNumber` (annual filings, FY period). 45-day filing lag to avoid look-ahead bias. See [METHODOLOGY.md](../METHODOLOGY.md).

## Notes on Exchange Coverage

- **France (PAR):** Insufficient FMP fundamental data for French-listed stocks. Not backtested.
- **South Africa (JNB):** Per-share metric scale mismatch between `key_metrics` and `stock_eod`. Not backtested.
- **Hong Kong (HKSE):** Included in results. Extreme drawdown driven by China real estate crisis (2022–2023). Use with caution.
- **Singapore (SES):** Average portfolio size < 10 stocks. Insufficient for robust backtest.
- **Korea (KSC):** 6 cash periods (2000–2005) due to limited early data. Results post-2006 are robust.
- **Taiwan (TAI+TWO):** 6 cash periods (2000–2005) due to limited early data. Results post-2006 are robust.
- **India (BSE+NSE):** 5 cash periods (2000–2004). High local risk-free rate (6.5%) depresses Sharpe ratio.
