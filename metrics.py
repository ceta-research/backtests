"""Comprehensive backtesting metrics computation.

Computes 17 Tier 1 + Tier 2 advanced metrics for strategy backtests.
Pure stdlib (math only, no numpy/pandas dependency).

Usage:
    from metrics import compute_metrics

    result = compute_metrics(
        period_returns=[0.05, -0.02, 0.08, ...],
        benchmark_returns=[0.03, -0.01, 0.06, ...],
        periods_per_year=2,  # semi-annual
        risk_free_rate=0.02,
    )
    print(result["portfolio"]["cagr"])   # e.g. 0.0996
    print(result["comparison"]["sharpe_ratio"])  # e.g. 0.523

See METHODOLOGY.md for formula definitions and interpretation guides.
"""

import math
import sys


def warn_if_truncated(acct, universe_name=None, stream=None):
    """Emit one loud stderr line per benchmark-truncated leg. WARNS, NEVER RAISES.

    Truncation is a legitimate result, not an error. A market whose local index
    starts late (^OSEAX begins 2013-03-05) still ran every rebalance; the
    benchmark simply cannot price the early ones. Raising here would stall the
    re-run campaign on every such leg.

    The raising checks inside `period_accounting` are code-correctness gates that
    data cannot trip. This is the data-quality signal, and it is deliberately
    non-fatal. Keeping the two separate is what lets a re-run proceed.
    """
    if not acct.get("window_truncated"):
        return
    who = universe_name or acct.get("universe") or "?"
    bench = acct.get("benchmark_symbol")
    first = acct.get("benchmark_first_date")
    # Build from parts so a missing symbol/date drops out instead of printing
    # "benchmark None starts None".
    bits = []
    if bench:
        bits.append(f"benchmark {bench}")
        if first:
            bits[-1] += f" starts {first}"
    bits.append(f"{acct['unmeasured_periods']} of {acct['total_rebalances']} "
                f"rebalances unmeasured")
    measured = acct["window_label"].split(" (")[0]
    req_a = (acct.get("requested_start") or "?")[:4]
    req_b = (acct.get("requested_end") or "?")[:4]
    # The n_periods == 0 label is a sentence, not a span, so the generic
    # "Measured window is X, NOT Y" phrasing renders as "Measured window is NO
    # MEASURED PERIODS, NOT 2002-2027" -- true but unreadable, and this is the
    # loudest channel the fix has. Say it plainly instead.
    if acct.get("n_periods", 0) == 0:
        tail = (f". NOTHING was measured: this leg has no benchmark-priced "
                f"period at all, so every published metric is null. The "
                f"strategy still ran {req_a}-{req_b}.")
    else:
        tail = f". Measured window is {measured}, NOT {req_a}-{req_b}."
    print(f"WARNING {who}: " + "; ".join(bits) + tail,
          file=stream or sys.stderr)


def period_accounting(executed, valid, cash_periods, *,
                      benchmark_symbol=None, benchmark_first_date=None,
                      date_key="rebalance_date", end_key="exit_date",
                      universe_name=None, warn=True):
    """Derive period counts + measured-window provenance for a backtest record.

    Blocker B006. Closes two defects.

    (a) CASH COUNTING. `cash_periods` must be counted over the SAME collection
        passed here as `executed`: every rebalance the strategy actually ran.
        `invested_periods` is DERIVED here and never counted separately, so
        `cash + invested == total_rebalances` holds by construction. Counting
        cash over one population and deriving invested from another is what
        published 22 records with a negative invested_periods (as low as -51).

    (b) SILENT WINDOW TRUNCATION. Periods the benchmark cannot price are dropped
        from `valid` (data_utils.get_benchmark_return returns None, with no
        per-period fallback), so a run LABELLED 2002-2025 can measure only
        2013-2025. `years` was already honest; nothing carried the window START
        or said the window had been cut. `window_label` does both, pre-formatted
        so it cannot be copied into a heading and come out wrong.

    Three populations, deliberately distinguished:
        results   every record the loop appended
        executed  results the strategy actually ran (portfolio_return not None)
        valid     executed AND benchmark-priced. The metrics population;
                  len(valid) == n_periods.

    ON `executed` BEING INERT TODAY, which an earlier version of this docstring
    got wrong. It claimed the filter excludes trailing stubs, naming
    sector-momentum's grid running past its price-fetch cap. It does not. An
    AST census of every results.append in the repo found 204 dict literals
    writing `portfolio_return` and NOT ONE writing None, literally or
    conditionally. sector-momentum and sector-rotation record their unrunnable
    trailing periods as portfolio_return 0.0 with stocks_held 0 -- as CASH --
    so those periods pass the filter and inflate both cash_periods and
    total_rebalances in exactly the way the filter was said to prevent.

    So `executed == results` everywhere right now, and the filter is a forward
    guard: it is what makes `valid` a subset by construction, and it is what a
    topic that DOES start recording unrun periods as None would need. Do not
    read it as a live stub exclusion. The trailing-period inflation in those two
    topics is real, is ~3 periods per exchange, and is logged in
    DATA_QUALITY_ISSUES.md rather than fixed here, because capping a rebalance
    grid needs a backtest run to verify and this change ships without one.

    `cash_periods` counts over `executed`; `n_periods` counts `valid`. They live
    in DIFFERENT populations ON PURPOSE. That is what preserves the honest
    full-window cash rate (Norway sat in cash for 84 of 95 rebalances, not 84 of
    the 50 the benchmark happened to price). NEVER relate the two.

    Raises ValueError -- not `assert`, which `python -O` strips, and this is the
    only gate this repo has -- on any wiring regression.

    Args:
        executed: list[dict] - rebalances the strategy actually ran
        valid: list[dict] - subset of executed that the benchmark can price
        cash_periods: int - cash count over `executed`, using the topic's own
            predicate (predicates differ: stocks_held == 0, pairs_active <
            MIN_PAIRS_ACTIVE, y["is_cash"], ...), so the caller counts and this
            function owns everything derived from it.
        benchmark_symbol: str|None - e.g. "^OSEAX". Diagnostic only.
        benchmark_first_date: str|None - first date the benchmark prices.
        date_key / end_key: str - record keys for period start/end. Yearly and
            pairs records use "year" for both.
        universe_name: str|None - used only in the truncation warning.
        warn: bool - emit the stderr truncation line. Pass False for inner loops
            that would otherwise warn once per pair.

    Returns:
        dict of 13 keys, meant to be splatted LAST into the output record so a
        stale local cannot shadow it.
    """
    total_rebalances = len(executed)
    n_periods = len(valid)

    if isinstance(cash_periods, bool) or not isinstance(cash_periods, int):
        raise ValueError(
            f"cash_periods must be int, got {type(cash_periods).__name__}")
    if not 0 <= cash_periods <= total_rebalances:
        raise ValueError(
            f"cash_periods={cash_periods} outside [0, "
            f"total_rebalances={total_rebalances}]. Cash must be counted over "
            "`executed` (every rebalance run), not `valid` and not `results`.")
    if not 0 <= n_periods <= total_rebalances:
        raise ValueError(
            f"n_periods={n_periods} > total_rebalances={total_rebalances}. "
            "`valid` must be a subset of `executed`.")

    invested_periods = total_rebalances - cash_periods
    if cash_periods + invested_periods != total_rebalances:
        # Unreachable given the derivation above; catches a future edit that
        # replaces the derivation with an independent count.
        raise ValueError("cash_periods + invested_periods != total_rebalances")

    def _d(rows, idx, key):
        if not rows or key is None:
            return None
        row = rows[idx]
        if not isinstance(row, dict):
            return None
        v = row.get(key)
        if isinstance(v, str):
            return v
        if hasattr(v, "isoformat"):
            return v.isoformat()
        if isinstance(v, int):
            return str(v)          # yearly records key on an int year
        return None

    measured_start = _d(valid, 0, date_key)
    measured_end = _d(valid, -1, end_key) or _d(valid, -1, date_key)
    requested_start = _d(executed, 0, date_key)
    requested_end = _d(executed, -1, end_key) or _d(executed, -1, date_key)

    unmeasured = total_rebalances - n_periods
    truncated = unmeasured > 0

    def _yr(s):
        return s[:4] if isinstance(s, str) and len(s) >= 4 else "?"

    def _bench_phrase():
        if benchmark_symbol and benchmark_first_date:
            return f"benchmark {benchmark_symbol} starts {benchmark_first_date}; "
        if benchmark_symbol:
            return f"benchmark {benchmark_symbol}; "
        return ""

    if n_periods == 0:
        window_label = (
            f"NO MEASURED PERIODS (requested "
            f"{_yr(requested_start)}-{_yr(requested_end)}; "
            + (f"benchmark {benchmark_symbol} prices none of it; "
               if benchmark_symbol else "benchmark prices none of it; ")
            + f"all {total_rebalances} rebalances unmeasured)")
    elif truncated:
        window_label = (
            f"{_yr(measured_start)}-{_yr(measured_end)} ("
            + _bench_phrase()
            + f"{unmeasured} of {total_rebalances} rebalances unmeasured; "
              f"requested {_yr(requested_start)}-{_yr(requested_end)})")
    else:
        window_label = f"{_yr(measured_start)}-{_yr(measured_end)}"

    acct = {
        "n_periods": n_periods,
        "total_rebalances": total_rebalances,
        "cash_periods": cash_periods,
        "invested_periods": invested_periods,
        "measured_start": measured_start,
        "measured_end": measured_end,
        "requested_start": requested_start,
        "requested_end": requested_end,
        "benchmark_symbol": benchmark_symbol,
        "benchmark_first_date": benchmark_first_date,
        "window_truncated": truncated,
        "unmeasured_periods": unmeasured,
        "window_label": window_label,
    }
    if warn:
        warn_if_truncated(acct, universe_name)
    return acct


def compute_metrics(period_returns, benchmark_returns, periods_per_year,
                    risk_free_rate=0.02, additional_benchmarks=None,
                    years=None):
    """Compute full metrics suite for a strategy vs benchmark(s).

    Args:
        period_returns: list[float] - portfolio returns per period (e.g. 0.05 for 5%)
        benchmark_returns: list[float] - primary benchmark returns (same length)
        periods_per_year: int - 1 (annual), 2 (semi-annual), 4 (quarterly), 12 (monthly)
        risk_free_rate: float - annual risk-free rate (default 0.02 = 2%)
        additional_benchmarks: dict[str, list[float]] - optional extra benchmarks
            e.g. {"INDA": [0.03, ...], "QUAL": [0.02, ...]}
        years: optional float - explicit wall-clock years spanned by the returns.
            If None (default), derived as `len(returns) / periods_per_year`.
            This default is correct ONLY when `periods_per_year` matches the
            actual sampling frequency — which is the case for every current
            caller in this repo (quarterly backtests pass ppy=4, annual pass
            ppy=1, etc.). If you ever pass daily returns that are calendar-day
            forward-filled together with ppy=252, the default will under-count
            years by ~1/1.45 and deflate CAGR. Pass `years` explicitly in that
            case. See strategy-backtester/lib/equity_curve.py for the type-safe
            variant used in the position-level simulator.

    Returns:
        dict with keys: "portfolio", "benchmark", "comparison", "additional_benchmarks"
        All return values are raw floats (e.g. 0.0996 for 9.96% CAGR).
    """
    n = len(period_returns)
    if n < 2:
        return _empty_metrics()

    ppy = periods_per_year
    rf_period = risk_free_rate / ppy

    # Portfolio metrics
    port = _compute_series_metrics(period_returns, ppy, risk_free_rate, years=years)

    # Benchmark metrics
    bench = _compute_series_metrics(benchmark_returns, ppy, risk_free_rate, years=years)

    # Comparison metrics (portfolio vs primary benchmark)
    comp = _compute_comparison(period_returns, benchmark_returns, ppy, risk_free_rate,
                               port["cagr"], bench["cagr"])

    result = {
        "portfolio": port,
        "benchmark": bench,
        "comparison": comp,
    }

    # Additional benchmarks
    if additional_benchmarks:
        result["additional_benchmarks"] = {}
        for name, bench_rets in additional_benchmarks.items():
            if len(bench_rets) == n:
                ab_metrics = _compute_series_metrics(bench_rets, ppy, risk_free_rate, years=years)
                ab_comp = _compute_comparison(period_returns, bench_rets, ppy,
                                              risk_free_rate, port["cagr"], ab_metrics["cagr"])
                result["additional_benchmarks"][name] = {
                    "metrics": ab_metrics,
                    "comparison": ab_comp,
                }

    return result


def _compute_series_metrics(returns, ppy, risk_free_rate, years=None):
    """Compute metrics for a single return series.

    `years`: optional wall-clock years. If None, uses `len(returns) / ppy`.
    See compute_metrics() docstring for when to pass explicitly.
    """
    n = len(returns)
    if n < 2:
        return {}

    rf_period = risk_free_rate / ppy

    # Cumulative return and drawdown
    cumulative = 1.0
    peak = 1.0
    max_dd = 0.0
    dd_start = 0
    max_dd_duration = 0
    current_dd_start = 0
    in_drawdown = False

    cumulative_values = []
    for i, r in enumerate(returns):
        cumulative *= (1 + r)
        cumulative_values.append(cumulative)

        if cumulative > peak:
            if in_drawdown:
                duration = i - current_dd_start
                if duration > max_dd_duration:
                    max_dd_duration = duration
                in_drawdown = False
            peak = cumulative
        else:
            if not in_drawdown:
                current_dd_start = i
                in_drawdown = True

        dd = (cumulative - peak) / peak if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    # If still in drawdown at end, count duration to end
    if in_drawdown:
        duration = n - current_dd_start
        if duration > max_dd_duration:
            max_dd_duration = duration

    # CAGR. Default: sample-count-based (correct when ppy matches sampling).
    # Caller may pass wall-clock `years` for forward-filled / mixed-frequency
    # curves where n / ppy is wrong.
    if years is None:
        years = n / ppy
    if cumulative > 0 and years > 0:
        cagr = cumulative ** (1.0 / years) - 1
    else:
        cagr = -1.0

    total_return = cumulative - 1

    # Volatility (annualized)
    mean_r = sum(returns) / n
    variance = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
    vol = math.sqrt(variance) * math.sqrt(ppy)

    # Sharpe ratio
    sharpe = (cagr - risk_free_rate) / vol if vol > 0 else None

    # Sortino ratio (downside deviation)
    downside_sq = []
    for r in returns:
        diff = r - rf_period
        if diff < 0:
            downside_sq.append(diff ** 2)
        else:
            downside_sq.append(0.0)
    downside_var = sum(downside_sq) / n if n > 0 else 0
    downside_dev = math.sqrt(downside_var) * math.sqrt(ppy)
    sortino = (cagr - risk_free_rate) / downside_dev if downside_dev > 0 else None

    # Calmar ratio
    calmar = cagr / abs(max_dd) if max_dd != 0 else None

    # VaR 95% (historical method - 5th percentile)
    sorted_returns = sorted(returns)
    var_index = max(0, int(math.ceil(n * 0.05)) - 1)
    var_95 = sorted_returns[var_index]

    # CVaR 95% (expected shortfall)
    tail_returns = [r for r in sorted_returns if r <= var_95]
    cvar_95 = sum(tail_returns) / len(tail_returns) if tail_returns else var_95

    # Best/worst period
    best_period = max(returns)
    worst_period = min(returns)

    # Pct negative periods
    neg_count = sum(1 for r in returns if r < 0)
    pct_negative = neg_count / n

    # Max consecutive losses
    max_consec = 0
    current_consec = 0
    for r in returns:
        if r < 0:
            current_consec += 1
            if current_consec > max_consec:
                max_consec = current_consec
        else:
            current_consec = 0

    # Skewness
    if n >= 3 and variance > 0:
        std = math.sqrt(variance)
        skewness = (n / ((n - 1) * (n - 2))) * sum(((r - mean_r) / std) ** 3 for r in returns)
    else:
        skewness = None

    # Kurtosis (excess)
    if n >= 4 and variance > 0:
        std = math.sqrt(variance)
        m4 = sum(((r - mean_r) / std) ** 4 for r in returns)
        kurtosis = ((n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3))) * m4 - \
                   (3 * (n - 1) ** 2) / ((n - 2) * (n - 3))
    else:
        kurtosis = None

    return {
        "cagr": cagr,
        "total_return": total_return,
        "max_drawdown": max_dd,
        "max_dd_duration_periods": max_dd_duration if max_dd_duration > 0 else None,
        "annualized_volatility": vol,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "calmar_ratio": calmar,
        "var_95": var_95,
        "cvar_95": cvar_95,
        "best_period": best_period,
        "worst_period": worst_period,
        "pct_negative_periods": pct_negative,
        "max_consecutive_losses": max_consec,
        "skewness": skewness,
        "kurtosis": kurtosis,
    }


def _compute_comparison(port_returns, bench_returns, ppy, risk_free_rate,
                        port_cagr, bench_cagr):
    """Compute comparison metrics between portfolio and benchmark."""
    n = len(port_returns)
    if n < 2:
        return {}

    # Excess returns
    excess = [p - b for p, b in zip(port_returns, bench_returns)]
    excess_cagr = port_cagr - bench_cagr

    # Win rate
    wins = sum(1 for e in excess if e > 0)
    win_rate = wins / n

    # Tracking error and information ratio
    excess_mean = sum(excess) / n
    excess_var = sum((e - excess_mean) ** 2 for e in excess) / (n - 1) if n > 1 else 0
    tracking_error = math.sqrt(excess_var) * math.sqrt(ppy)
    info_ratio = (excess_mean * ppy) / tracking_error if tracking_error > 0 else None

    # Up/down capture
    up_port = []
    up_bench = []
    down_port = []
    down_bench = []
    for p, b in zip(port_returns, bench_returns):
        if b > 0:
            up_port.append(p)
            up_bench.append(b)
        elif b < 0:
            down_port.append(p)
            down_bench.append(b)

    up_capture = None
    if up_bench:
        up_bench_mean = sum(up_bench) / len(up_bench)
        if up_bench_mean != 0:
            up_capture = (sum(up_port) / len(up_port)) / up_bench_mean

    down_capture = None
    if down_bench:
        down_bench_mean = sum(down_bench) / len(down_bench)
        if down_bench_mean != 0:
            down_capture = (sum(down_port) / len(down_port)) / down_bench_mean

    # Beta and Alpha (CAPM)
    port_mean = sum(port_returns) / n
    bench_mean = sum(bench_returns) / n
    cov_sum = sum((p - port_mean) * (b - bench_mean) for p, b in zip(port_returns, bench_returns))
    bench_var_sum = sum((b - bench_mean) ** 2 for b in bench_returns)

    if bench_var_sum > 0:
        beta = cov_sum / bench_var_sum
        # Jensen's alpha (annualized)
        alpha = port_cagr - (risk_free_rate + beta * (bench_cagr - risk_free_rate))
    else:
        beta = None
        alpha = None

    return {
        "excess_cagr": excess_cagr,
        "win_rate": win_rate,
        "information_ratio": info_ratio,
        "tracking_error": tracking_error,
        "up_capture": up_capture,
        "down_capture": down_capture,
        "beta": beta,
        "alpha": alpha,
    }


def compute_drawdown_series(cumulative_values):
    """Compute rolling drawdown series from cumulative values.

    Args:
        cumulative_values: list[float] - cumulative growth values (e.g. [1.0, 1.05, 1.02, ...])

    Returns:
        list[float] - drawdown at each point (e.g. [0.0, 0.0, -0.0286, ...])
    """
    if not cumulative_values:
        return []

    peak = cumulative_values[0]
    drawdowns = []
    for v in cumulative_values:
        if v > peak:
            peak = v
        dd = (v - peak) / peak if peak > 0 else 0
        drawdowns.append(dd)
    return drawdowns


def compute_annual_returns(period_returns, benchmark_returns, period_dates,
                           periods_per_year):
    """Aggregate period returns to annual returns.

    Args:
        period_returns: list[float] - portfolio returns per period
        benchmark_returns: list[float] - benchmark returns per period
        period_dates: list[str] - ISO date strings (e.g. "2020-01-01")
        periods_per_year: int

    Returns:
        list[dict] with keys: year, portfolio, benchmark, excess
    """
    annual = {}
    for pr, br, d in zip(period_returns, benchmark_returns, period_dates):
        year = d[:4]
        if year not in annual:
            annual[year] = {"port_cum": 1.0, "bench_cum": 1.0, "n": 0}
        annual[year]["port_cum"] *= (1 + pr)
        annual[year]["bench_cum"] *= (1 + br)
        annual[year]["n"] += 1

    result = []
    for year in sorted(annual.keys()):
        d = annual[year]
        # Only include years with enough periods
        min_periods = max(1, periods_per_year // 2)
        if d["n"] >= min_periods:
            port_annual = d["port_cum"] - 1
            bench_annual = d["bench_cum"] - 1
            result.append({
                "year": int(year),
                "portfolio": port_annual,
                "benchmark": bench_annual,
                "excess": port_annual - bench_annual,
            })
    return result


def compute_rolling_cagr(period_returns, periods_per_year, window_years=3):
    """Compute rolling N-year CAGR.

    Args:
        period_returns: list[float]
        periods_per_year: int
        window_years: int - rolling window in years (default 3)

    Returns:
        list[tuple(int, float)] - (period_index, rolling_cagr) pairs
    """
    window = window_years * periods_per_year
    if len(period_returns) < window:
        return []

    result = []
    for i in range(window, len(period_returns) + 1):
        window_returns = period_returns[i - window:i]
        cum = 1.0
        for r in window_returns:
            cum *= (1 + r)
        if cum > 0:
            cagr = cum ** (1.0 / window_years) - 1
        else:
            cagr = -1.0
        result.append((i - 1, cagr))
    return result


def format_metrics(metrics, strategy_name="Strategy", benchmark_name="S&P 500"):
    """Format metrics dict for console display.

    Args:
        metrics: dict from compute_metrics()
        strategy_name: display name for portfolio column
        benchmark_name: display name for benchmark column
    """
    p = metrics["portfolio"]
    b = metrics["benchmark"]
    c = metrics["comparison"]

    lines = []
    lines.append("")
    lines.append("=" * 65)
    lines.append(f"  {strategy_name} vs {benchmark_name}")
    lines.append("=" * 65)

    def pct(v, decimals=2):
        if v is None:
            return "N/A".rjust(10)
        return f"{v * 100:>{9}.{decimals}f}%"

    def num(v, decimals=3):
        if v is None:
            return "N/A".rjust(10)
        return f"{v:>{10}.{decimals}f}"

    header = f"  {'Metric':<28} {strategy_name:>12} {benchmark_name:>12}"
    lines.append(header)
    lines.append("  " + "-" * 54)

    # Return metrics
    lines.append(f"  {'CAGR':<28} {pct(p.get('cagr'))} {pct(b.get('cagr'))}")
    lines.append(f"  {'Total Return':<28} {pct(p.get('total_return'), 1)} {pct(b.get('total_return'), 1)}")

    # Risk metrics
    lines.append(f"  {'Max Drawdown':<28} {pct(p.get('max_drawdown'))} {pct(b.get('max_drawdown'))}")
    lines.append(f"  {'Volatility (ann.)':<28} {pct(p.get('annualized_volatility'))} {pct(b.get('annualized_volatility'))}")
    lines.append(f"  {'VaR 95%':<28} {pct(p.get('var_95'))} {pct(b.get('var_95'))}")

    # Risk-adjusted
    lines.append(f"  {'Sharpe Ratio':<28} {num(p.get('sharpe_ratio'))} {num(b.get('sharpe_ratio'))}")
    lines.append(f"  {'Sortino Ratio':<28} {num(p.get('sortino_ratio'))} {num(b.get('sortino_ratio'))}")
    lines.append(f"  {'Calmar Ratio':<28} {num(p.get('calmar_ratio'))} {num(b.get('calmar_ratio'))}")

    # Comparison
    lines.append("")
    lines.append(f"  {'--- Relative ---':<28}")
    lines.append(f"  {'Excess CAGR':<28} {pct(c.get('excess_cagr'))}")
    lines.append(f"  {'Win Rate':<28} {pct(c.get('win_rate'), 1)}")
    lines.append(f"  {'Information Ratio':<28} {num(c.get('information_ratio'))}")
    lines.append(f"  {'Tracking Error':<28} {pct(c.get('tracking_error'))}")
    lines.append(f"  {'Up Capture':<28} {pct(c.get('up_capture'), 1)}")
    lines.append(f"  {'Down Capture':<28} {pct(c.get('down_capture'), 1)}")
    lines.append(f"  {'Beta':<28} {num(c.get('beta'))}")
    lines.append(f"  {'Alpha (Jensen)':<28} {pct(c.get('alpha'))}")

    lines.append("=" * 65)
    return "\n".join(lines)


def _empty_metrics():
    """Return empty metrics dict for edge cases (n < 2)."""
    empty_series = {
        "cagr": None, "total_return": None, "max_drawdown": None,
        "max_dd_duration_periods": None, "annualized_volatility": None,
        "sharpe_ratio": None, "sortino_ratio": None, "calmar_ratio": None,
        "var_95": None, "cvar_95": None, "best_period": None, "worst_period": None,
        "pct_negative_periods": None, "max_consecutive_losses": None,
        "skewness": None, "kurtosis": None,
    }
    empty_comp = {
        "excess_cagr": None, "win_rate": None, "information_ratio": None,
        "tracking_error": None, "up_capture": None, "down_capture": None,
        "beta": None, "alpha": None,
    }
    return {
        "portfolio": empty_series.copy(),
        "benchmark": empty_series.copy(),
        "comparison": empty_comp,
    }
