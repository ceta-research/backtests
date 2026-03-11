# FCF Yield Value Strategy

Buy stocks with high free cash flow yield combined with quality filters. FCF yield (FCF/Market Cap) measures actual cash generation after capex — the money a business genuinely produces for shareholders, not the accounting-adjusted earnings that EBITDA and P/E track.

## The Signal

A stock qualifies when all four filters pass:

| Filter | Threshold | Why |
|--------|-----------|-----|
| FCF Yield | > 8% | Top ~20% of large-cap universe. Cheap on actual cash. |
| Return on Equity | > 10% | Business generates solid returns on capital |
| Interest Coverage | > 3x | Can service debt comfortably |
| Operating Margin | > 10% | Genuine pricing power, not a thin-margin commodity |

Portfolio: top 30 by FCF yield (highest first), equal weight. Rebalance annually in July.

**Why FCF over P/E or EV/EBITDA:** EBITDA ignores capital expenditure. Two companies can have identical EBITDA while one spends 3x the capex to maintain its asset base. FCF yield captures this — it's the cash that's actually left after the business has paid for everything it needs.

## Academic Basis

Gray & Vogel (2012) compared 9 valuation metrics from 1971-2010. FCF/TEV was the second-best performing metric at 16.6% annually, beating P/E (14.3%), P/B (14.5%), and EV/EBITDA (17.7% was top). The Novy-Marx (2013) finding that combining value with quality improves results motivates the ROE and margin filters.

## Usage

```bash
# Screen current stocks (US default)
python3 fcf-yield/screen.py

# Screen Germany
python3 fcf-yield/screen.py --preset germany

# Run backtest (US, annual, 2000-2025)
python3 fcf-yield/backtest.py

# Run backtest all exchanges
python3 fcf-yield/backtest.py --global --output results/exchange_comparison.json --verbose

# Run without transaction costs
python3 fcf-yield/backtest.py --no-costs
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current stock screen (live TTM data) |
| `generate_charts.py` | Chart generation from results |
| `results/` | JSON/CSV output from backtest runs |
| `charts/` | Generated PNG charts |

## Methodology

- **Universe**: Full exchange (NYSE+NASDAQ+AMEX for US). Not index-constrained.
- **Data**: FMP financial data via Ceta Research warehouse
- **Rebalancing**: Annual, July. FY annual filings used with 45-day lag.
- **Costs**: Size-tiered transaction costs (0.05-0.15% per trade, varies by market cap)
- **Min holding**: 10 stocks required. Cash position if fewer qualify.
- **Period**: 2000-2025 (25 annual periods)

See [METHODOLOGY.md](../METHODOLOGY.md) for full methodology documentation.

## Exchange Eligibility

Exchanges tested: US (NYSE+NASDAQ+AMEX), UK (LSE), Germany (XETRA), Japan (JPX), Hong Kong (HKSE), Korea (KSC), Taiwan (TAI+TWO), Indonesia (JKT), Thailand (SET), Canada (TSX+TSXV), China (SHH+SHZ), Sweden (STO), Switzerland (SIX), Norway (OSL).

Excluded: India (BSE+NSE) — too few qualifying stocks under combined filters; Brazil (SAO) and Australia (ASX) — price data artifacts; France (PAR) — data pipeline gap; South Africa (JNB) — historically thin universe.

## Data Source

Data: Ceta Research (FMP financial data warehouse). TTM metrics updated daily. Historical FY data available 2000-2025 for most exchanges.
