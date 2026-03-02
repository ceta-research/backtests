# Ceta Research Backtests

Reproducible quantitative strategy backtests. Three ways to run: local (DuckDB), cloud Code Execution, or cloud Projects.

## Strategies

| Strategy | Backtest | Screen | Description |
|----------|----------|--------|-------------|
| [QARP](qarp/) | `qarp/backtest.py` | `qarp/screen.py` | Quality at a Reasonable Price. 7-factor signal combining Piotroski F-Score, ROE, D/E, P/E, current ratio, income quality, and market cap. Semi-annual rebalance, 2000-2025. |
| [Piotroski](piotroski/) | `piotroski/backtest.py` | `piotroski/screen.py` | Piotroski F-Score applied to value stocks. Bottom 20% by P/B, 9-signal scoring, annual April rebalance. Three portfolio tracks: Score 8-9, Score 0-2, All Value. 1985-2025. |
| [Low P/E](low-pe/) | `low-pe/backtest.py` | `low-pe/screen.py` | Classic value investing. P/E < 15, ROE > 10%, D/E < 1.0, top 30 by lowest P/E. Quarterly rebalance, 2000-2025. |

## Quick start

```bash
# 1. Clone
git clone https://github.com/ceta-research/backtests.git
cd backtests

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set your API key
export CR_API_KEY="your_key_here"

# 4. Run locally (fetches data, caches in DuckDB, runs on your machine)
python3 qarp/backtest.py
python3 low-pe/screen.py
python3 piotroski/backtest.py --verbose

# 5. Or run on cloud compute (no local dependencies needed)
python3 qarp/screen.py --cloud --preset us
python3 qarp/backtest.py --cloud --preset us
```

## Three ways to run

### 1. Local mode (default)

Scripts fetch data via the Ceta Research SQL API, cache in a local DuckDB instance, and run the backtest on your machine. Requires `duckdb` and `requests`.

```bash
export CR_API_KEY="your_key_here"
python3 qarp/backtest.py
```

### 2. Cloud: Code Execution API

Submit self-contained scripts to run on managed cloud compute. Good for screens and quick analyses.

```bash
python3 qarp/screen.py --cloud --preset us
```

Or use the client directly:

```python
from cr_client import CetaResearch

cr = CetaResearch()
result = cr.execute_code("print('hello from the cloud')")
print(result["stdout"])
```

### 3. Cloud: Projects API

Upload multi-file projects and run on cloud compute. Good for full backtests with shared modules.

```bash
python3 qarp/backtest.py --cloud --preset us
```

Or use the Projects API directly:

```python
from cr_client import CetaResearch

cr = CetaResearch()
project = cr.create_project("my-analysis", dependencies=["duckdb", "requests"])
cr.upsert_file(project["id"], "main.py", "print('hello')")
result = cr.run_project(project["id"])
print(result["stdout"])
```

See `examples/` for complete working examples.

## API surface

The `cr_client.py` client provides three API groups:

| API | Methods | Use case |
|-----|---------|----------|
| **Data Explorer** | `query()`, `query_saved()` | SQL queries against financial data warehouse |
| **Code Execution** | `execute_code()`, `execute_from_repo()`, `get_execution_status()`, `get_execution_files()`, `cancel_execution()`, `get_execution_limits()` | Run single scripts on cloud compute |
| **Projects** | `create_project()`, `upsert_file()`, `run_project()`, `list_projects()`, `get_run()`, `import_project_from_git()` | Multi-file project management and execution |

## Data sources

### Ceta Research (default)

Set `CR_API_KEY` and run. Scripts use the [Ceta Research](https://cetaresearch.com) SQL API out of the box. Backward-compatible with `TS_API_KEY`.

The client supports two formats:
- **JSON** (default): For quick queries, returns `list[dict]`. Easy iteration.
- **Parquet** (bulk): For backtests, returns raw bytes. 10-100x smaller payloads, loads directly into DuckDB.

### Swapping providers

Each backtest has a `fetch_data_via_api()` function that loads data into DuckDB tables. Replace that function to use a different source.

### CSV / Parquet (bring your own data)

```python
import duckdb
con = duckdb.connect(":memory:")
con.execute("CREATE TABLE prices_cache AS SELECT * FROM read_csv('my_prices.csv')")
# ... populate remaining tables, then pass `con` to run_backtest()
```

## Common CLI flags

All backtests share these flags (via `cli_utils.py`):

```bash
# Exchange selection
python3 qarp/backtest.py --exchange BSE,NSE          # Specific exchanges
python3 qarp/backtest.py --preset india               # Exchange preset
python3 qarp/backtest.py --global                     # All exchanges

# Parameters
python3 qarp/backtest.py --frequency quarterly         # Rebalancing frequency
python3 qarp/backtest.py --risk-free-rate 0.0          # Risk-free rate (default: 2%)
python3 low-pe/backtest.py --no-costs                  # Disable transaction costs

# Cloud execution
python3 qarp/backtest.py --cloud                       # Run on cloud compute

# Output
python3 qarp/backtest.py --output results.json --verbose
```

Available presets: `us`, `india`, `china`, `japan`, `korea`, `uk`, `germany`, `france`, `australia`, `canada`, `brazil`, `hongkong`, `taiwan`, `thailand`, `singapore`, and more. See `cli_utils.py` for the full list.

## How it works

1. **Fetch**: Scripts query the Ceta Research SQL API for historical financial data
2. **Cache**: Data is loaded into a local in-memory DuckDB database
3. **Backtest**: Screening and return calculations run entirely on your machine (or cloud with `--cloud`)
4. **Results**: Summary statistics printed to console, optionally saved to JSON

See [METHODOLOGY.md](METHODOLOGY.md) for complete backtesting methodology, metrics definitions, and exchange coverage.

## Project structure

```
backtests/
  cr_client.py            # Ceta Research API client (data + code exec + projects)
  ts_client.py            # Backward-compat shim (re-exports CetaResearch as TradingStudio)
  cloud_runner.py         # Cloud execution helper (Projects API wrapper)
  data_utils.py           # Data loading, price lookups, rebalance dates
  metrics.py              # Shared metrics computation (Sharpe, Sortino, Calmar, etc.)
  costs.py                # Transaction cost models (size-tiered, flat)
  cli_utils.py            # Shared CLI args, exchange presets
  requirements.txt        # requests, duckdb
  METHODOLOGY.md          # Complete backtesting methodology
  qarp/
    backtest.py           # Full QARP backtest (2000-2025)
    screen.py             # Current QARP screen
    generate_charts.py    # Regenerate charts from results
    README.md             # Strategy details
    results/              # Pre-computed results
    charts/               # Generated charts
  piotroski/
    backtest.py           # Full Piotroski backtest (1985-2025)
    screen.py             # Current Piotroski screen
    README.md             # Strategy details
  low-pe/
    backtest.py           # Full Low P/E backtest (2000-2025)
    screen.py             # Current Low P/E screen
    generate_charts.py    # Regenerate charts from results
    README.md             # Strategy details
    results/              # Pre-computed results
    charts/               # Generated charts
  examples/
    code_execution_example.py  # Code Execution API example
    projects_example.py        # Projects API example
```

## Metrics

All backtests compute a comprehensive metrics suite via the shared `metrics.py` module:

**Risk-adjusted:** Sharpe Ratio, Sortino Ratio, Calmar Ratio
**Risk:** Max Drawdown, VaR 95%, CVaR 95%, Annualized Volatility
**Relative:** Information Ratio, Tracking Error, Up/Down Capture, Alpha, Beta
**Return:** CAGR, Total Return, Win Rate

See [METHODOLOGY.md](METHODOLOGY.md#metrics-suite) for all 17 Tier 1 + Tier 2 metrics with formulas.

## License

MIT
