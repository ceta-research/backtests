# Sector P/E Compression

Buy S&P 500 sectors when their aggregate P/E ratio falls more than 1 standard deviation below its 5-year historical average. Execution via sector ETFs (XLK, XLE, etc.). Hold SPY when no sectors are compressed.

## Signal

```
Market-cap weighted sector P/E (from S&P 500 constituent FY filings)
Z-score = (Current sector P/E - 5yr average) / 5yr std dev
Buy: z-score < -1.0 (compressed)
Hold SPY: no sectors compressed
```

**Sector P/E formula:** Sum(Market Cap) / Sum(Market Cap / Individual P/E)
This is equivalent to aggregate earnings yield and captures the economic reality better than averaging individual P/Es.

## Academic Basis

Campbell & Shiller (1988) established that valuation ratios are mean-reverting and predict long-term returns. Lakonishok, Shleifer & Vishny (1994) showed that value strategies work because investors extrapolate past performance too far. This strategy applies the same logic at the sector level.

## Parameters

| Parameter | Value |
|-----------|-------|
| Universe | S&P 500 sectors (11 GICS) |
| Signal threshold | z-score < -1.0 (1 std dev below 5yr avg) |
| Lookback | 5 years of annual FY P/E history |
| Rebalancing | Quarterly (Jan, Apr, Jul, Oct) |
| Weighting | Equal weight across compressed sectors |
| No-signal default | Hold SPY |
| Transaction costs | 0.1% per trade (ETFs) |
| Data lag | 45 days (annual filing) |

## Sector ETF Mapping

| FMP Sector | ETF | Inception |
|-----------|-----|-----------|
| Technology | XLK | Dec 1998 |
| Energy | XLE | Dec 1998 |
| Financial Services | XLF | Dec 1998 |
| Healthcare | XLV | Dec 1998 |
| Consumer Cyclical | XLY | Dec 1998 |
| Consumer Defensive | XLP | Dec 1998 |
| Industrials | XLI | Dec 1998 |
| Basic Materials | XLB | Dec 1998 |
| Utilities | XLU | Dec 1998 |
| Real Estate | XLRE | Oct 2015 |
| Communication Services | XLC | Jun 2018 |

Note: XLRE and XLC are excluded from compressed sector allocation before their inception dates.

## Usage

```bash
# US backtest (default, 2000-2025)
python3 sector-pe-compression/backtest.py

# Custom date range
python3 sector-pe-compression/backtest.py --start-year 2005 --end-year 2025

# Save results
python3 sector-pe-compression/backtest.py --output results/backtest.json --verbose

# No costs (academic baseline)
python3 sector-pe-compression/backtest.py --no-costs

# Current sector screen
python3 sector-pe-compression/screen.py
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current sector P/E z-scores |
| `generate_charts.py` | Chart generation from results |
| `results/backtest.json` | Backtest results |
| `charts/` | Generated PNG charts (gitignored) |

## Data Source

Ceta Research — FMP financial data warehouse.
- `sp500_constituent`: Sector membership (current S&P 500 list)
- `financial_ratios` (FY): Annual P/E per stock
- `key_metrics` (FY): Market cap per stock
- `stock_eod`: Sector ETF daily prices

## Notes on Survivorship Bias

This backtest uses the **current** S&P 500 constituent list for sector mapping throughout the entire history. This introduces mild survivorship bias: companies that failed and were removed from the S&P 500 before 2026 are excluded from the historical sector P/E calculations.

Impact is modest because: (1) the signal is sector-level P/E, not individual stock selection, (2) sector membership is relatively stable, (3) surviving companies are broadly representative of sector earnings over time.

A fully survivorship-bias-free version would require historical constituent membership data, which is not available in the current warehouse.

See `backtests/DATA_QUALITY_ISSUES.md` for related notes.
