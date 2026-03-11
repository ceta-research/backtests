# Small-Cap Growth Strategy

**Slug:** `small-cap`
**Category:** Growth
**Rebalancing:** Annual (July)
**Universe:** Exchange-specific small-cap range (5%–200% of standard market cap threshold)

## Strategy

Screen for small-cap stocks with strong revenue growth, confirmed profitability, and manageable leverage. Hold the top 30 by revenue growth, equal weight. Rebalance annually in July.

This combines the academic size premium (Fama-French SMB factor) with a growth quality filter — targeting small companies that are growing real revenue and generating profit, not speculative micro-caps burning cash.

## Signal

| Filter | Threshold | Source |
|--------|-----------|--------|
| Market cap (lower) | 5% of exchange standard | `key_metrics` FY |
| Market cap (upper) | 200% of exchange standard | `key_metrics` FY |
| Revenue growth (YoY) | > 15% | `income_statement` FY (two consecutive) |
| Net income | > 0 (profitable) | `income_statement` FY |
| Debt-to-equity | < 2.0 | `financial_ratios` FY |

**Selection:** Top 30 by revenue growth (highest first), equal weight.

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
max_stocks: 30          # Top 30 by revenue growth
weighting: equal
filing_lag: 45 days     # Point-in-time: July rebalance sees filings through ~May 17
transaction_costs: true # Size-tiered model from costs.py
```

## Academic References

- Banz (1981) "The Relationship Between Return and Market Value of Common Stocks", *Journal of Financial Economics* 9(1), 3–18
- Fama & French (1992) "The Cross-Section of Expected Stock Returns", *Journal of Finance* 47(2), 427–465
- Fama & French (1993) "Common Risk Factors in the Returns on Stocks and Bonds", *Journal of Financial Economics* 33(1), 3–56

## Usage

```bash
# US backtest (default)
python3 small-cap/backtest.py

# Specific exchange
python3 small-cap/backtest.py --preset india
python3 small-cap/backtest.py --preset japan
python3 small-cap/backtest.py --preset uk

# All exchanges
python3 small-cap/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen (live data)
python3 small-cap/screen.py
python3 small-cap/screen.py --preset india

# Generate charts (requires results/exchange_comparison.json)
python3 small-cap/generate_charts.py
```

## Data Source

All data via Ceta Research (FMP financial data warehouse). See `backtests/METHODOLOGY.md` for full methodology.
