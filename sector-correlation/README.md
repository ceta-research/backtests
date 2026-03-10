# Sector Correlation Regime Strategy

**Category:** Sector | **Frequency:** Monthly | **Universe:** US Sector ETFs (SPDR)

A market timing strategy that detects high-correlation regimes — when all sectors move together — and shifts defensively. Uses the 9 S&P 500 SPDR sector ETFs as a market stress indicator.

---

## Strategy Logic

**Signal:** 60-day rolling average pairwise correlation across all 9 sector ETFs (36 pairs).

| Regime | Threshold | Allocation |
|--------|-----------|------------|
| **High** | avg corr > 0.70 | Defensive sectors: XLU + XLV + XLP (equal weight) |
| **Medium** | 0.40 – 0.70 | SPY buy-and-hold (100%) |
| **Low** | avg corr < 0.40 | All 9 sector ETFs (equal weight) |

Transaction costs: 0.1% applied when regime changes (ETF trading cost).

---

## Sector ETFs

| ETF | Sector |
|-----|--------|
| XLK | Technology |
| XLE | Energy |
| XLF | Financials |
| XLV | Healthcare |
| XLY | Consumer Discretionary |
| XLP | Consumer Staples |
| XLI | Industrials |
| XLB | Materials |
| XLU | Utilities |

---

## Academic Basis

- Longin & Solnik (2001). "Extreme Correlation of International Equity Markets." *Journal of Finance*, 56(2), 649-676. Documents that equity correlations increase during bear markets.
- Kritzman et al. (2012). "Regime Shifts: Implications for Dynamic Strategies." *Financial Analysts Journal*, 68(3), 22-39. Studies dynamic allocation across market regimes.

---

## Usage

```bash
# Run backtest (US, 2000-2025)
python3 sector-correlation/backtest.py

# Save results to file
python3 sector-correlation/backtest.py --output results/backtest_results.json --verbose

# Screen: current regime
python3 sector-correlation/screen.py

# Generate charts (requires results file)
python3 sector-correlation/generate_charts.py
```

---

## Key Finding

The strategy underperforms SPY buy-and-hold by ~2% annually over 25 years, despite correctly identifying high-correlation stress regimes. The paradox: by the time correlations spike above 0.70, markets have often already fallen. Defensive positioning captures less upside than it avoids downside.

**Correlation as a risk indicator works. Correlation as a trading signal doesn't.**

---

## Data

- Source: FMP data warehouse (via Ceta Research API)
- Table: `stock_eod` (ETF daily prices)
- Date range: 1998-12-22 (ETF inception) to present
- All 9 SPDR sector ETFs + SPY available with full history
