"""Transaction cost models for backtesting.

Size-tiered costs based on market capitalization (default).
See METHODOLOGY.md Section 5 for rationale and alternatives.

Usage:
    from costs import tiered_cost, apply_costs

    cost = tiered_cost(5_000_000_000)  # $5B market cap -> 0.003 (0.3%)
    net_return = apply_costs(0.15, cost)  # 15% raw -> 14.4% net (round-trip)
"""

# Default cost tiers: (min_market_cap, one_way_rate)
DEFAULT_TIERS = [
    (10_000_000_000, 0.001),  # >$10B: 0.1% one-way
    (2_000_000_000, 0.003),   # $2-10B: 0.3% one-way
    (0, 0.005),               # <$2B: 0.5% one-way
]


def tiered_cost(market_cap, tiers=None):
    """Size-tiered one-way transaction cost.

    Args:
        market_cap: float or None - company market cap in dollars
        tiers: list of (min_cap, rate) tuples, sorted descending by min_cap.
               Defaults to DEFAULT_TIERS.

    Returns:
        float - one-way cost rate (e.g. 0.003 for 0.3%)
    """
    if tiers is None:
        tiers = DEFAULT_TIERS

    if market_cap is None:
        return tiers[-1][1]  # Smallest tier (most conservative)

    for min_cap, rate in tiers:
        if market_cap >= min_cap:
            return rate

    return tiers[-1][1]


def flat_cost(rate=0.001):
    """Flat one-way transaction cost.

    Args:
        rate: float - cost rate (default 0.001 = 0.1%)

    Returns:
        float - one-way cost rate
    """
    return rate


def apply_costs(raw_return, entry_cost, exit_cost=None):
    """Apply round-trip transaction costs to a raw return.

    Args:
        raw_return: float - raw period return (e.g. 0.15 for 15%)
        entry_cost: float - one-way entry cost rate
        exit_cost: float or None - one-way exit cost rate (defaults to entry_cost)

    Returns:
        float - net return after costs
    """
    if exit_cost is None:
        exit_cost = entry_cost
    return raw_return - entry_cost - exit_cost
