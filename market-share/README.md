# Market Share Gain Screen

Annual factor strategy that selects stocks gaining sector-relative market share, filtered for profitability and quality.

## Strategy

**Signal:** YoY revenue growth exceeds sector median by ≥ 10 percentage points. Sector medians computed dynamically at each rebalance date using all qualifying stocks in the same GICS sector.

**Quality filters:**
- Return on equity > 8%
- Operating profit margin > 5%
- Market cap > exchange-specific threshold (e.g., $500M USD for US)

**Selection:** Top 30 by excess revenue growth, equal weight. Hold cash if fewer than 10 qualify.

**Rebalancing:** Annual, July. Annual filings available with 45-day lag from May 17 cutoff.

**Period:** 2000–2024 (25 annual periods)

**Transaction costs:** Size-tiered model (0.1% mega-cap, 0.3% large-cap, 0.5% mid-cap)

## Academic Basis

Revenue growth as a market share proxy is supported by Jegadeesh & Livnat (2006), who show revenue surprises predict returns beyond earnings surprises. Piotroski & So (2012) document that fundamental momentum signals (including revenue trends) predict cross-sectional returns in the context of value-investing anomalies. The sector-relative formulation follows factor construction conventions in Fama & French (1993) and subsequent literature.

**Key limitation:** This strategy cannot observe actual market share — FMP has no direct market share data. Sector-relative revenue growth is an indirect proxy. Companies can grow revenue faster than their sector without actually gaining share (e.g., new segments, geographic expansion). The results should be interpreted as "revenue growth momentum relative to peers," not strict market share.

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Historical backtest, 2000–2024, all exchanges |
| `screen.py` | Current live screen using TTM/most recent FY data |
| `generate_charts.py` | Chart generation from exchange_comparison.json |
| `results/exchange_comparison.json` | Full results for all 15 exchanges |
| `results/returns_*.json` | Per-exchange annual return series |

## Usage

```bash
# Activate venv
source /Users/swas/Desktop/Swas/Kite/ATO_SUITE/.venv/bin/activate

cd /Users/swas/Desktop/Swas/Kite/ATO_SUITE/backtests

# Run live screen (current stocks)
python3 market-share/screen.py                        # US default
python3 market-share/screen.py --preset india
python3 market-share/screen.py --exchange XETRA

# Run historical backtest
python3 market-share/backtest.py                      # US default
python3 market-share/backtest.py --preset india --verbose
python3 market-share/backtest.py --preset canada --output market-share/results/returns_TSX.json

# Run all exchanges (updates exchange_comparison.json)
python3 market-share/backtest.py --global \
    --output market-share/results/exchange_comparison.json --verbose

# Generate charts (after backtest run)
python3 market-share/generate_charts.py
```

## Results Summary (2000–2024)

| Exchange | CAGR | Excess vs SPY | Sharpe | MaxDD | Cash% | Avg Stocks |
|----------|------|---------------|--------|-------|-------|------------|
| India (BSE+NSE) | 11.82% | +3.99% | 0.184 | -42.1% | 20% | 20.7 |
| Canada (TSX) | 6.72% | -1.11% | 0.222 | -26.6% | 4% | 22.2 |
| Switzerland (SIX) | 6.40% | -1.43% | 0.337 | -49.3% | 12% | 13.9 |
| UK (LSE) | 5.60% | -2.23% | 0.102 | -40.3% | 0% | 18.5 |
| Sweden (STO) | 4.46% | -3.37% | 0.112 | -46.5% | 44% | 17.5 |
| Germany (XETRA) | 3.53% | -4.30% | 0.080 | -46.3% | 0% | 18.3 |
| US (NYSE+NASDAQ+AMEX) | 3.34% | -4.50% | 0.072 | -41.4% | 0% | 22.7 |
| Thailand (SET) | 3.33% | -4.50% | 0.043 | -44.2% | 28% | 22.7 |
| Australia (ASX) | 3.22% | -4.61% | -0.017 | -31.9% | 24% | 20.2 |
| Brazil (SAO) | 5.69% | -2.14% | -0.246† | -32.6% | 24% | 21.1 |
| Japan (JPX) | 1.98% | -5.86% | 0.088 | -59.4% | 20% | 25.1 |
| Taiwan (TAI) | 1.69% | -6.14% | 0.050 | -29.1% | 32% | 27.5 |
| Korea (KSC) | 1.26% | -6.58% | -0.110 | -35.8% | 36% | 23.2 |
| Hong Kong (HKSE) | -1.99% | -9.82% | -0.223 | -80.3% | 4% | 18.2 |
| China (SHZ+SHH) | -2.58% | -10.41% | -0.129 | -72.7% | 0% | 21.4 |

*†Brazil Sharpe negative because regional risk-free rate = 10.5%, not a data artifact.*

**Excluded from code and content:** JNB (South Africa, 76% cash rate, avg 9.8 stocks), SES (Singapore, 56% cash rate, avg 7.7 stocks).

**Key finding:** India is the only market where this signal adds meaningful alpha (+3.99% excess CAGR, 60% win rate). Developed markets systematically underperform — high-revenue-growth companies trade at elevated multiples, and rate sensitivity translates signal quality into performance drag.

## Methodology

Full methodology: [METHODOLOGY.md](../METHODOLOGY.md) (public)

Point-in-time: 45-day lag for current FY filing, 410-day lag for prior FY filing.
Data quality: entry price > $1, single-period return capped at 200%.
Benchmark: S&P 500 Total Return (SPY).
