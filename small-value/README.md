# Small-Cap Value Strategy

**Slug:** `small-value`
**Category:** Factor
**Rebalancing:** Annual (July)
**Universe:** Exchange-specific small-cap range (5%–200% of standard market cap threshold)

## Strategy

Screen for small-cap stocks trading below 1.5x book value, with confirmed profitability and manageable leverage. Hold the top 30 by lowest P/B ratio, equal weight. Rebalance annually in July.

This directly implements the Fama-French "small value" corner: the intersection of the size premium (SMB) and value premium (HML) that has historically shown the strongest risk-adjusted outperformance in the three-factor model. The logic: small companies attract less analyst coverage and institutional attention, creating mispricing when they also trade below book value.

## Signal

| Filter | Threshold | Source |
|--------|-----------|--------|
| Market cap (lower) | 5% of exchange standard | `key_metrics` FY |
| Market cap (upper) | 200% of exchange standard | `key_metrics` FY |
| P/B ratio (lower) | > 0 (positive book value) | `financial_ratios` FY |
| P/B ratio (upper) | < 1.5 | `financial_ratios` FY |
| Return on equity | > 5% | `key_metrics` FY |
| Debt-to-equity | < 2.5 | `financial_ratios` FY |

**Selection:** Top 30 by P/B ratio ascending (cheapest relative to book value), equal weight.

## Market Cap Bounds (Local Currency)

| Exchange | Standard | Small-Cap Min | Small-Cap Max |
|----------|---------|---------------|---------------|
| US (NYSE/NASDAQ/AMEX) | $1B | $50M | $2B |
| India (BSE/NSE) | ₹20B | ₹1B | ₹40B |
| Germany (XETRA) | €500M | €25M | €1B |
| UK (LSE) | £500M | £25M | £1B |
| Japan (JPX) | ¥100B | ¥5B | ¥200B |
| China (SHZ/SHH) | ¥2B | ¥100M | ¥4B |
| Hong Kong (HKSE) | HK$2B | HK$100M | HK$4B |
| Korea (KSC) | ₩500B | ₩25B | ₩1T |
| Canada (TSX) | C$500M | C$25M | C$1B |
| Switzerland (SIX) | CHF 500M | CHF 25M | CHF 1B |
| Sweden (STO) | SEK 5B | SEK 250M | SEK 10B |
| Taiwan (TAI) | NT$10B | NT$500M | NT$20B |
| Thailand (SET) | ฿10B | ฿500M | ฿20B |
| South Africa (JNB) | R10B | R500M | R20B |

## Parameters

```yaml
rebalancing: annual
rebalance_months: [7]   # July
min_stocks: 10          # Cash if fewer qualify
max_stocks: 30          # Top 30 by P/B ascending
weighting: equal
filing_lag: 45 days     # Point-in-time: July rebalance sees filings through ~May 17
transaction_costs: true # Size-tiered model from costs.py
```

## Academic References

- Fama, E. & French, K. (1992). "The Cross-Section of Expected Stock Returns." *Journal of Finance* 47(2), 427–465.
- Fama, E. & French, K. (1993). "Common Risk Factors in the Returns on Stocks and Bonds." *Journal of Financial Economics* 33(1), 3–56.
- Lakonishok, J., Shleifer, A., & Vishny, R. (1994). "Contrarian Investment, Extrapolation, and Risk." *Journal of Finance* 49(5), 1541–1578.

## Usage

```bash
# US backtest (default)
python3 small-value/backtest.py

# Specific exchange
python3 small-value/backtest.py --preset india
python3 small-value/backtest.py --preset japan
python3 small-value/backtest.py --preset uk

# All exchanges
python3 small-value/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen (live data)
python3 small-value/screen.py
python3 small-value/screen.py --preset india

# Generate charts (requires results/exchange_comparison.json)
python3 small-value/generate_charts.py
```

## Data Source

All data via Ceta Research (FMP financial data warehouse). See `backtests/METHODOLOGY.md` for full methodology.
