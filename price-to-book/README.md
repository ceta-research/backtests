# Price-to-Book Value Screen

Screen for stocks trading below 1.5x book value with positive ROE (returns on equity > 8%).

## Strategy

**Signal:** Price-to-Book ratio 0–1.5, ROE > 8%, market cap above exchange threshold
**Portfolio:** Top 30 by lowest P/B, equal weight
**Rebalancing:** Annual (January), 2000–2025
**Min stocks:** 10 (hold cash if fewer qualify)

The price-to-book ratio measures what the market pays for a company's net assets (total assets minus total liabilities). A P/B below 1.0 means the market values the company at less than its accounting book value. The ROE filter removes value traps — companies with low P/B but no profitability.

## Academic Basis

Fama and French (1992) documented the book-to-market premium across decades of US stock data. Along with size, B/M (inverse of P/B) became the second factor in the Fama-French three-factor model. The premium has been replicated internationally by Fama & French (1998) and others.

Gray & Vogel (2012) showed P/B underperforms EV/EBITDA and FCF-based screens as a standalone signal, but the academic evidence for a value premium remains robust.

## Usage

```bash
# Screen current stocks (US)
python3 price-to-book/screen.py

# Screen Indian stocks
python3 price-to-book/screen.py --preset india

# Backtest US
python3 price-to-book/backtest.py --preset us --output results/returns_us.json --verbose

# Backtest all exchanges
python3 price-to-book/backtest.py --global --output results/exchange_comparison.json --verbose
```

## Files

- `backtest.py` — Full historical backtest (annual, 2000–2025)
- `screen.py` — Current qualifying stocks (live TTM data)
- `generate_charts.py` — Chart generation from results JSON
- `results/` — Computed backtest results (JSON/CSV)
- `charts/` — Generated charts (PNG)

## Notes

- P/B works best for asset-heavy sectors (financials, industrials, materials, energy)
- Asset-light companies (tech, pharma) have P/B that reflects intangibles not on the balance sheet
- Financial sector stocks dominate low P/B screens — expect heavy sector concentration
- Tangible book value (excluding goodwill/intangibles) is a stricter alternative metric

## References

- Fama, E. & French, K. (1992). "The Cross-Section of Expected Stock Returns." *Journal of Finance*, 47(2), 427–465.
- Fama, E. & French, K. (1998). "Value versus Growth: The International Evidence." *Journal of Finance*, 53(6), 1975–1999.
- Rosenberg, B., Reid, K. & Lanstein, R. (1985). "Persuasive Evidence of Market Inefficiency." *Journal of Portfolio Management*, 11(3), 9–16.
- Gray, W. & Vogel, J. (2012). "Analyzing Valuation Measures: A Performance Horse-Race over the Past 40 Years." *Journal of Portfolio Management*, 39(1), 112–121.
- Lakonishok, J., Shleifer, A. & Vishny, R. (1994). "Contrarian Investment, Extrapolation, and Risk." *Journal of Finance*, 49(5), 1541–1578.
