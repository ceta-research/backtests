"""Transaction cost models for backtesting.

Size-tiered costs based on market capitalization (default).
See METHODOLOGY.md Section 5 for rationale and alternatives.

Usage:
    from costs import tiered_cost, apply_costs

    cost = tiered_cost(5_000_000_000)  # $5B market cap -> 0.003 (0.3%)
    net_return = apply_costs(0.15, cost)  # 15% raw -> 14.4% net (round-trip)

CURRENCY CAVEAT (see DATA_QUALITY_ISSUES.md, "Cost tiers vs local currency"):
DEFAULT_TIERS is calibrated in USD, but FMP reports `profile.marketCap` in each
company's LOCAL currency. Callers that pass a local-currency market cap without
an `fx_per_usd` compare, say, yen against a dollar threshold. Pass
`fx_per_usd=get_fx_per_usd(exchanges)` to tier correctly outside the US.
The default of 1.0 preserves historical behaviour so published results stay
reproducible; do not change it without re-running the affected backtests.
"""

# Default cost tiers: (min_market_cap, one_way_rate). Thresholds are USD.
DEFAULT_TIERS = [
    (10_000_000_000, 0.001),  # >$10B: 0.1% one-way
    (2_000_000_000, 0.003),   # $2-10B: 0.3% one-way
    (0, 0.005),               # <$2B: 0.5% one-way
]

# Approximate units of local currency per USD, keyed by exchange code.
# Derived from the same calibration as cli_utils.MKTCAP_THRESHOLD_MAP so the two
# stay consistent. Precision beyond ~2 significant figures does not matter here:
# these only decide which of three cost tiers a holding lands in.
FX_PER_USD = {
    # North America
    "NYSE": 1.0, "NASDAQ": 1.0, "AMEX": 1.0,
    "TSX": 1.38, "TSXV": 1.38,             # CAD
    # Europe
    "LSE": 0.79,                            # GBP
    "XETRA": 0.92, "FSX": 0.92,             # EUR
    "PAR": 0.92, "AMS": 0.92, "BRU": 0.92,
    "MIL": 0.92, "BME": 0.92,
    "SIX": 0.88,                            # CHF
    "STO": 10.9, "OSL": 10.9,               # SEK / NOK
    # Asia-Pacific
    "BSE": 83.0, "NSE": 83.0,               # INR
    "SHZ": 7.25, "SHH": 7.25,               # CNY
    "HKSE": 7.8,                            # HKD
    "JPX": 150.0,                           # JPY
    "KSC": 1350.0, "KOE": 1350.0,           # KRW
    "ASX": 1.55,                            # AUD
    "TAI": 32.0, "TWO": 32.0,               # TWD
    "SET": 35.0,                            # THB
    "SGX": 1.35, "SES": 1.35,               # SGD
    # Other regions
    "SAO": 5.0,                             # BRL
    "BMV": 17.0,                            # MXN
    "JSE": 18.2, "JNB": 18.2,               # ZAR
    "SAU": 3.75,                            # SAR
    "TLV": 3.65,                            # ILS
    "JKT": 16000.0,                         # IDR
}


def get_fx_per_usd(exchanges):
    """Units of local currency per USD for the given exchange(s).

    Mirrors cli_utils.get_mktcap_threshold's resolution rules.

    Args:
        exchanges: list[str] or None - exchange codes (e.g. ["BSE", "NSE"]).
                   None or empty (global mode) returns 1.0.

    Returns:
        float - local currency units per USD. Unknown exchanges return 1.0
                (assumes USD-like scale, matching the threshold map's default).
    """
    if not exchanges:
        return 1.0

    rates = [FX_PER_USD.get(e.upper()) for e in exchanges]
    known = [r for r in rates if r is not None]
    if not known:
        return 1.0
    # Mixed-currency universes (rare) take the max, which is the conservative
    # choice: it scales thresholds up and charges the higher cost tier.
    return max(known)


def tiered_cost(market_cap, tiers=None, fx_per_usd=1.0):
    """Size-tiered one-way transaction cost.

    Args:
        market_cap: float or None - company market cap, in the SAME currency
                    that fx_per_usd describes (FMP reports local currency).
        tiers: list of (min_cap, rate) tuples, sorted descending by min_cap,
               with thresholds in USD. Defaults to DEFAULT_TIERS.
        fx_per_usd: float - units of local currency per USD. Tier thresholds are
                    multiplied by this before comparison, so a local-currency
                    market cap is tiered against a local-currency threshold.
                    Defaults to 1.0 (treat market_cap as already USD).

    Returns:
        float - one-way cost rate (e.g. 0.003 for 0.3%)
    """
    if tiers is None:
        tiers = DEFAULT_TIERS

    if market_cap is None:
        return tiers[-1][1]  # Smallest tier (most conservative)

    for min_cap, rate in tiers:
        if market_cap >= min_cap * fx_per_usd:
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
