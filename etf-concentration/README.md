# ETF Concentration Backtest

Buy quality stocks that are least weight-concentrated across ETFs.

## Signal

For each stock, compute average weight per ETF position (`AVG(weightPercentage)` across all ETFs holding it). Stocks with low average weight are held by ETFs but not heavily weighted. They are less subject to passive flow distortion (Wurgler 2011 feedback loop).

**Distinct from etf-crowding:** Crowding counts how many ETFs hold a stock. Concentration measures how heavily each ETF weights it. A stock in 100 ETFs at 0.01% average = low concentration, high crowding. A stock in 5 ETFs at 5% average = high concentration, low crowding.

### Filters
- ROE > 10%
- P/E between 0 and 40
- Market cap > exchange threshold
- Held by >= 5 ETFs

### Portfolio
- Bottom 30 by average ETF weight (least concentrated)
- Equal weight
- Cash if fewer than 10 stocks qualify

### Rebalancing
- Annual (July)
- 2005-2025

## Academic Basis

- **Wurgler (2011):** Cap-weighted indices create feedback loops concentrating passive flows in the largest stocks.
- **Plyakha, Uppal & Vilkov (2021):** Equal-weighted S&P 500 earned ~2% more annually than cap-weighted (1964-2016).
- **DeMiguel, Garlappi & Uppal (2009):** Naive 1/N equal-weight outperforms most sophisticated optimization strategies.
- **Cremers & Petajisto (2009):** "Active share" shows many funds marketed as diversified are highly concentrated.

## Data Caveat

ETF holdings data (`etf_holder`) is a current snapshot, not historical. The concentration signal is applied retrospectively (look-ahead bias). Quality filters (ROE, P/E, market cap) use point-in-time FY data and are bias-free.

## Usage

```bash
# US backtest
python3 etf-concentration/backtest.py --preset us --output results/returns_US.json --verbose

# All exchanges
python3 etf-concentration/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen
python3 etf-concentration/screen.py --preset us --top 50

# Generate charts
python3 etf-concentration/generate_charts.py
```

## Transaction Costs

Size-tiered (see `costs.py`): >$10B = 0.1%, $2-10B = 0.3%, <$2B = 0.5% one-way. Disable with `--no-costs`.
