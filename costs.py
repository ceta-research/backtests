"""Transaction cost models for backtesting.

Size-tiered costs based on market capitalization (default).
See METHODOLOGY.md Section 5 for rationale and alternatives.

Usage:
    from costs import tiered_cost, apply_costs

    cost = tiered_cost(5_000_000_000)  # $5B market cap -> 0.003 (0.3%)
    net_return = apply_costs(0.15, cost)  # 15% raw -> 14.4% net (round-trip)

Two conventions for what a period pays (METHODOLOGY.md, "Transaction Costs"): apply_costs charges
every booked name a full round trip each period, kept or not; trade_costs charges only the trades a
series of books makes, each sized to the trade.

CURRENCY CAVEAT (see DATA_QUALITY_ISSUES.md, "Cost tiers vs local currency"):
DEFAULT_TIERS is calibrated in USD, but FMP reports `profile.marketCap` in each
company's LOCAL currency. Callers that pass a local-currency market cap without
an `fx_per_usd` compare, say, yen against a dollar threshold. Pass
`fx_per_usd=get_fx_per_usd(exchanges)` to tier correctly outside the US.
The default of 1.0 preserves historical behaviour so published results stay
reproducible; do not change it without re-running the affected backtests.
"""
import math

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
    "KLS": 4.46,                            # MYR
    # Other regions
    "SAO": 5.0,                             # BRL
    "MEX": 17.0, "BMV": 17.0,               # MXN
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


def trade_costs(books):
    """Each period's cost under the "trades" convention: only real trades are charged, each sized to the trade.

    Args:
        books: one series' period books, in order. A book is {symbol: (gross_return, rate)}: every name
               the series books in the period, with its return before costs and its one-way rate at
               that rebalance. None is a cash period, and an empty book counts as one.

    Returns:
        list[float] - one cost per period, as a fraction of the portfolio, to subtract from the
                      period's mean return.

    The position is the last invested period's book at its end weights, (1 + r) over the book's sum of
    (1 + r), each name with the rate it carried there. It is empty at the start, after a cash period,
    and when every name ended at -100%.

    A rebalance into a book of n names is charged to the period that starts at it:
      * a name in the book trades the gap between 1/n and its weight in the position (0 for an entry),
        at the new period's rate;
      * a name in the position that the book leaves out sells its weight at the rate it last carried.
        That rate is one period old: a name that leaves isn't screened, so it has no new cap.

    A cash period sells the position at those rates. The sale is charged to the last invested period,
    and the cash period itself costs 0.0: a cash row stores one return for every series, and two series'
    sales differ, so the row can't carry them. The last book is not sold: it is still held at the run's end.

    Each sum is math.fsum's, so a cost doesn't depend on the order of a book's names.
    """
    terms = [[] for _ in books]   # each period's trades, weight times rate
    held, last = {}, None         # the position {symbol: (end weight, rate)}, and the last invested period
    for i, book in enumerate(books):
        if not book:
            if held:
                terms[last] += [w * rate for w, rate in held.values()]
            held = {}
            continue
        target = 1.0 / len(book)
        terms[i] += [abs(target - held.get(s, (0.0, 0.0))[0]) * rate for s, (_, rate) in book.items()]
        terms[i] += [w * rate for s, (w, rate) in held.items() if s not in book]
        total = math.fsum(1.0 + r for r, _ in book.values())
        held = {s: ((1.0 + r) / total, rate) for s, (r, rate) in book.items()} if total > 0 else {}
        last = i
    return [math.fsum(t) for t in terms]
