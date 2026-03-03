# Interest Coverage Screen

Screens for companies with strong debt-servicing ability, moderate leverage, and solid profitability. High interest coverage signals financial resilience during credit stress, rate hikes, and economic downturns.

## Strategy Card

```yaml
strategy:
  name: "Interest Coverage Screen"
  slug: "interest-coverage"
  description: "Screen for companies with EBIT/Interest Expense > 5x, moderate debt, and solid ROE. Companies that can easily service debt survive stress and outperform."
  academic_reference: "Altman (1968) bankruptcy prediction; Graham & Dodd (1934) fixed-charge coverage"

signal:
  filters:
    - {name: "interestCoverageRatio", operator: ">", threshold: "5.0"}
    - {name: "debtToEquityRatio", operator: ">=", threshold: "0.0"}
    - {name: "debtToEquityRatio", operator: "<", threshold: "1.5"}
    - {name: "returnOnEquity", operator: ">", threshold: "0.08"}
    - {name: "marketCap", operator: ">", threshold: "1000000000"}
  scoring: "interestCoverageRatio DESC"
  selection: "Top 30 by highest coverage, equal weight"

parameters:
  rebalancing: "quarterly"
  rebalance_months: [1, 4, 7, 10]
  min_stocks: 10
  max_stocks: 30
  weighting: "equal"
  min_market_cap: 1000000000
  transaction_costs: true

benchmarks:
  primary: "SPY"
  factor: "QUAL"
  regional: "auto"

exchanges:
  eligible: [US_MAJOR, BSE, NSE, STO, TSX, SHZ, HKSE, SET, XETRA, SHH, SIX, TAI, KSC]
  excluded:
    - {exchange: "ASX", reason: "Broken adjClose data. 314 stocks have >1000x price ratios from incorrect stock split adjustments (e.g., IIQ.AX adj close oscillates between ~25 and ~15,000). Produces 58% CAGR artifact."}
    - {exchange: "SAO", reason: "Broken adjClose data. 20+ stocks with >1000x price ratios (e.g., CTNM3.SA max adjClose 132M vs min 37.9). Produces 3,250% single-year returns."}
    - {exchange: "JPX", reason: "No FY data in key_metrics/financial_ratios tables. TTM has 4,016 symbols but FY has 0. Data pipeline gap."}
    - {exchange: "LSE", reason: "No FY data. Same as JPX. TTM has 3,745 symbols but FY has 0."}
  notes: "ASX and SAO are FMP data quality issues (adjClose not properly adjusted for splits/consolidations). JPX and LSE require data pipeline changes to ingest FY financials."

content:
  blog_posts: ["US flagship", "regional (TBD)", "comparison (if 8+ exchanges)"]
  comparison_post: true
  charts_needed: ["cumulative growth", "annual returns", "CAGR comparison", "max drawdown comparison"]
```

## Data Sources

All data from FMP via [Ceta Research API](https://cetaresearch.com).

- **Interest coverage ratio**: `financial_ratios.interestCoverageRatio` (EBIT / Interest Expense)
- **Debt-to-equity**: `financial_ratios.debtToEquityRatio`
- **Return on equity**: `key_metrics.returnOnEquity`
- **Market cap**: `key_metrics.marketCap`
- **Prices**: `stock_eod.adjClose`

## Usage

```bash
# Backtest US stocks (default)
python3 interest-coverage/backtest.py

# Backtest Indian stocks
python3 interest-coverage/backtest.py --preset india

# Backtest all exchanges
python3 interest-coverage/backtest.py --global --output results/exchange_comparison.json --verbose

# Run current screen
python3 interest-coverage/screen.py --preset us

# No transaction costs (academic baseline)
python3 interest-coverage/backtest.py --no-costs
```

## Signal Rationale

Interest coverage (EBIT / Interest Expense) measures how many times over a company can pay its debt interest from operating earnings. The 5x threshold means earnings can drop 80% and the company still meets obligations.

Additional filters:
- **D/E < 1.5, D/E >= 0**: Moderate leverage. Excludes both negative-equity companies and those with excessive debt.
- **ROE > 8%**: The business is worth owning, not just solvent.
- **Market cap > $1B**: Reliable data, investable.

## References

- Altman, E. (1968). "Financial Ratios, Discriminant Analysis and the Prediction of Corporate Bankruptcy." *Journal of Finance*, 23(4), 589-609.
- Graham, B. & Dodd, D. (1934). *Security Analysis*. McGraw-Hill.
