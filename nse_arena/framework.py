"""NSE Strategy Arena — Framework
---------------------------------
ATO_Simulator-equivalent for intraday + EOD backtesting via CR API.

3-step pipeline:
  Step 1 (Scanner):  SQL identifies candidates (gap-ups, breakouts, signals)
  Step 2 (Orders):   SQL computes entry/exit prices
  Step 3 (Simulate): Python portfolio simulation -> metrics

Both Steps 1+2 run server-side as a single SQL query.
Step 3 runs locally using the trade records returned.

Mirrors:
  ATO_Simulator.simulator.util.parse_input_config  -> config_sweep()
  ATO_Simulator.simulator.steps.simulate_step      -> IntradaySimulator
  ATO_Simulator.simulator.driver                   -> run_strategy() / run_sweep()
"""

import sys
import json
import glob as glob_mod
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from itertools import product
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from cr_client import CetaResearch
from metrics import compute_metrics
from nse_arena.charges import nse_intraday_charges


# ---------------------------------------------------------------------------
# Strategy base class
# ---------------------------------------------------------------------------

class Strategy(ABC):
    """Base class for all NSE strategies."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Display name (e.g. 'Gap-Up Pullback Scalp')."""
        ...

    @property
    @abstractmethod
    def strategy_type(self) -> str:
        """'intraday' or 'eod'."""
        ...

    @abstractmethod
    def build_sql(self, config: dict) -> str:
        """SQL query returning trade records.

        Must return columns:
            symbol, trade_date, entry_price, exit_price, exit_type,
            <sort_key>, bench_ret
        """
        ...

    @abstractmethod
    def default_config(self) -> dict:
        ...

    @abstractmethod
    def sweep_grid(self) -> dict:
        """Parameter grid: {key: [values]}. Single values auto-wrapped."""
        ...

    @property
    def sort_key(self) -> str:
        """Field to rank trades by (highest first) for position selection."""
        return "signal_strength"

    @property
    def periods_per_year(self) -> int:
        return 252  # Trading days

    @property
    def risk_free_rate(self) -> float:
        return 0.065  # India 10Y Gsec


# ---------------------------------------------------------------------------
# Intraday portfolio simulator
# ---------------------------------------------------------------------------

class IntradaySimulator:
    """Portfolio simulator for intraday strategies.

    All positions open and close within the same trading day.
    Adapted from ATO_Simulator.simulator.steps.simulate_step.process_step.

    State machine:
      - Cash pool = initial_capital (constant, no compounding within day)
      - Each day: pick top max_positions trades, allocate order_value each
      - Deduct NSE intraday charges per trade
      - Daily return = sum(trade PnL) / initial_capital
    """

    def __init__(self, initial_capital, max_positions, order_value, charges_fn):
        self.initial_capital = initial_capital
        self.max_positions = max_positions
        self.order_value = order_value
        self.charges = charges_fn(order_value)

    def simulate(self, trades, sort_key="signal_strength"):
        """Process trades into (daily_returns, bench_returns) lists."""
        by_date = defaultdict(list)
        bench_by_date = {}
        for t in trades:
            d = str(t["trade_date"])
            by_date[d].append(t)
            bench_by_date[d] = t.get("bench_ret") or 0.0

        daily_rets = []
        bench_rets = []

        for d in sorted(by_date.keys()):
            day = by_date[d]
            day.sort(key=lambda t: t.get(sort_key) or 0, reverse=True)
            selected = day[:self.max_positions]

            pnl = sum(
                (t["exit_price"] - t["entry_price"]) / t["entry_price"]
                * self.order_value - self.charges
                for t in selected
                if t.get("entry_price") and t.get("exit_price")
                and t["entry_price"] > 0
            )

            daily_rets.append(pnl / self.initial_capital)
            bench_rets.append(bench_by_date[d])

        return daily_rets, bench_rets


# ---------------------------------------------------------------------------
# Config sweep (Cartesian product)
# ---------------------------------------------------------------------------

def config_sweep(grid: dict):
    """Yield every combination of parameter grid values.

    Mirrors ATO_Simulator.simulator.util.parse_input_config.create_config_iterator().
    Single values are auto-wrapped in lists.
    """
    keys = list(grid.keys())
    vals = [v if isinstance(v, list) else [v] for v in grid.values()]
    for combo in product(*vals):
        yield dict(zip(keys, combo))


def count_configs(grid: dict) -> int:
    """Count total configurations without generating them."""
    total = 1
    for v in grid.values():
        total *= len(v) if isinstance(v, list) else 1
    return total


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------

def run_strategy(client, strategy, config=None, verbose=True):
    """Run a single strategy configuration. Returns summary dict."""
    cfg = config or strategy.default_config()
    sql = strategy.build_sql(cfg)

    if verbose:
        compact = {k: v for k, v in cfg.items()
                   if k not in ("start_date", "end_date")}
        print(f"  Config: {compact}")

    trades = client.query(sql, memory_mb=16384, threads=6, timeout=600)

    if not trades:
        if verbose:
            print("    No trades returned.")
        return {"error": "no trades", "config": cfg, "strategy": strategy.name}

    sim = IntradaySimulator(
        initial_capital=cfg.get("initial_capital", 500_000),
        max_positions=cfg.get("max_positions", 5),
        order_value=cfg.get("order_value", 50_000),
        charges_fn=nse_intraday_charges,
    )
    daily_rets, bench_rets = sim.simulate(trades, sort_key=strategy.sort_key)

    if not daily_rets:
        return {"error": "no active days", "config": cfg, "strategy": strategy.name}

    result = compute_metrics(
        period_returns=daily_rets,
        benchmark_returns=bench_rets,
        periods_per_year=strategy.periods_per_year,
        risk_free_rate=strategy.risk_free_rate,
    )

    n = len(trades)
    p = result["portfolio"]
    c = result["comparison"]

    wins = sum(1 for t in trades
               if t.get("exit_price") and t.get("entry_price")
               and t["exit_price"] > t["entry_price"])
    signal_exits = sum(1 for t in trades if t.get("exit_type") == "signal")

    summary = {
        "strategy": strategy.name,
        "type": strategy.strategy_type,
        "config": cfg,
        "trades": n,
        "active_days": len(daily_rets),
        "win_rate": round(wins / n * 100, 1) if n else 0,
        "signal_exit_pct": round(signal_exits / n * 100, 1) if n else 0,
        "cagr": round(p["cagr"] * 100, 2),
        "max_dd": round(p["max_drawdown"] * 100, 2),
        "sharpe": round(p["sharpe_ratio"], 3) if p["sharpe_ratio"] else None,
        "sortino": round(p["sortino_ratio"], 3) if p["sortino_ratio"] else None,
        "calmar": round(p["calmar_ratio"], 3) if p["calmar_ratio"] else None,
        "volatility": round(p["annualized_volatility"] * 100, 2),
        "bench_cagr": round(result["benchmark"]["cagr"] * 100, 2),
        "excess": round(c["excess_cagr"] * 100, 2),
        "down_capture": round(c["down_capture"] * 100, 1) if c["down_capture"] else None,
        "full_metrics": result,
    }

    if verbose:
        print(f"    Trades: {n:,}  |  Signal exits: {summary['signal_exit_pct']}%  "
              f"|  Win rate: {summary['win_rate']}%")
        print(f"    CAGR: {summary['cagr']:+.2f}%  |  MaxDD: {summary['max_dd']:.1f}%  "
              f"|  Sharpe: {summary['sharpe']:.3f}  |  Calmar: {summary['calmar']:.3f}")

    return summary


def run_sweep(client, strategy, grid=None, verbose=True):
    """Run parameter sweep. Returns list of valid results sorted by Calmar."""
    grid = grid or strategy.sweep_grid()
    configs = list(config_sweep(grid))

    print(f"\n{'='*70}")
    print(f"  {strategy.name} -- Parameter Sweep ({len(configs)} configs)")
    print(f"{'='*70}")

    results = []
    for i, cfg in enumerate(configs):
        if verbose:
            print(f"\n  [{i+1}/{len(configs)}]", end=" ")
        r = run_strategy(client, strategy, cfg, verbose=verbose)
        results.append(r)

    valid = [r for r in results if "error" not in r]
    valid.sort(key=lambda x: x.get("calmar") or -999, reverse=True)
    return valid


# ---------------------------------------------------------------------------
# EOD results collector
# ---------------------------------------------------------------------------

def collect_eod_india_results(backtests_dir=None):
    """Read existing India/NSE results from completed backtests.

    Scans for exchange_comparison.json files and extracts India entries.
    Returns list of dicts compatible with print_comparison().

    Note: EOD exchange_comparison.json stores values already as percentages
    (e.g. cagr=17.23 means 17.23%, max_drawdown=-6.61 means -6.61%).
    Sharpe/Calmar/Sortino are ratios (not percentages).
    """
    if backtests_dir is None:
        backtests_dir = str(Path(__file__).parent.parent)

    results = []
    pattern = os.path.join(backtests_dir, "*/results/exchange_comparison.json")

    for filepath in sorted(glob_mod.glob(pattern)):
        strategy_dir = os.path.basename(os.path.dirname(os.path.dirname(filepath)))
        try:
            with open(filepath) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        # Find India entry (keys vary: "BSE_NSE", "BSE+NSE", "India", "BSE", "NSE")
        india = None
        for key in ("BSE_NSE", "BSE+NSE", "India"):
            if key in data:
                india = data[key]
                break
        # Fallback: try BSE or NSE individually
        if not india:
            for key in ("BSE", "NSE"):
                if key in data and isinstance(data[key], dict) and "portfolio" in data[key]:
                    india = data[key]
                    break

        if not india or not isinstance(india, dict):
            continue

        # Extract metrics — values already in percentage form
        port = india.get("portfolio", {})
        comp = india.get("comparison", {})
        spy = india.get("spy", {})

        cagr = port.get("cagr")
        if cagr is None:
            continue

        max_dd = port.get("max_drawdown")
        sharpe = port.get("sharpe_ratio")
        calmar = port.get("calmar_ratio")

        # Values are already percentages in the JSON, pass through directly
        results.append({
            "strategy": strategy_dir,
            "type": "eod",
            "trades": india.get("avg_stocks_when_invested", "?"),
            "active_days": india.get("invested_periods", india.get("n_periods", "?")),
            "win_rate": comp.get("win_rate", 0),
            "cagr": round(cagr, 2),
            "max_dd": round(max_dd, 2) if max_dd is not None else None,
            "sharpe": round(sharpe, 3) if sharpe is not None else None,
            "calmar": round(calmar, 3) if calmar is not None else None,
            "volatility": round(port.get("annualized_volatility", 0), 2),
            "bench_cagr": round(spy.get("cagr", 0), 2),
            "excess": round(comp.get("excess_cagr", 0), 2),
            "down_capture": round(comp.get("down_capture", 0), 1),
            "data_years": india.get("years"),
            "cash_pct": round(india.get("cash_periods", 0) / india["n_periods"] * 100, 1)
                        if india.get("n_periods") else None,
        })

    return results


# ---------------------------------------------------------------------------
# Comparison table
# ---------------------------------------------------------------------------

def print_comparison(results, title="NSE Strategy Comparison"):
    """Print ranked comparison table. Sorts by Calmar ratio (CAGR/|MaxDD|)."""
    valid = [r for r in results if r.get("cagr") is not None]
    valid.sort(key=lambda x: x.get("calmar") or -999, reverse=True)

    print(f"\n{'='*100}")
    print(f"  {title} -- Ranked by Calmar Ratio (CAGR / |MaxDD|)")
    print(f"{'='*100}")
    print(f"  {'#':<3} {'Strategy':<25} {'Type':<9} {'CAGR':>7} {'MaxDD':>7} "
          f"{'Sharpe':>7} {'Calmar':>7} {'Excess':>7} {'DownCap':>8} {'WinR':>6}")
    print(f"  {'-'*96}")

    for i, r in enumerate(valid, 1):
        max_dd = f"{r['max_dd']:.1f}%" if r.get('max_dd') is not None else "N/A"
        sharpe = f"{r['sharpe']:.3f}" if r.get('sharpe') is not None else "N/A"
        calmar = f"{r['calmar']:.3f}" if r.get('calmar') is not None else "N/A"
        dcap = f"{r['down_capture']:.0f}%" if r.get('down_capture') is not None else "N/A"

        excess = r.get('excess')
        excess_str = f"{excess:>+6.1f}%" if excess is not None else "    N/A"
        win_rate = r.get('win_rate') or 0

        print(f"  {i:<3} {r['strategy']:<25} {r.get('type','?'):<9} "
              f"{r['cagr']:>+6.1f}% {max_dd:>7} {sharpe:>7} {calmar:>7} "
              f"{excess_str} {dcap:>8} {win_rate:>5.1f}%")

    print(f"{'='*100}")

    # Best picks
    if valid:
        best_calmar = valid[0]
        best_cagr = max(valid, key=lambda x: x.get("cagr") or -999)
        lowest_dd = min(valid, key=lambda x: abs(x.get("max_dd") or 999))

        print(f"\n  Best risk-adjusted (Calmar): {best_calmar['strategy']} "
              f"({best_calmar['cagr']:+.1f}% CAGR, {best_calmar.get('max_dd','?')}% MaxDD)")
        print(f"  Highest CAGR:               {best_cagr['strategy']} "
              f"({best_cagr['cagr']:+.1f}% CAGR)")
        print(f"  Lowest drawdown:            {lowest_dd['strategy']} "
              f"({lowest_dd.get('max_dd','?')}% MaxDD)")
