"""Shared CLI utilities for backtest scripts.

Common argument parsing, exchange presets, output formatting, and result saving.
Used by all strategy backtests (qarp/, piotroski/, low-pe/, etc.).

Usage:
    from cli_utils import add_common_args, resolve_exchanges, save_results

    parser = argparse.ArgumentParser()
    add_common_args(parser)
    # ... add strategy-specific args ...
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
"""

import argparse
import json
import os


# Exchange presets for CLI convenience
EXCHANGE_PRESETS = {
    # North America
    "us": {"name": "US_MAJOR", "exchanges": ["NYSE", "NASDAQ", "AMEX"]},
    "nyse": {"name": "NYSE", "exchanges": ["NYSE"]},
    "nasdaq": {"name": "NASDAQ", "exchanges": ["NASDAQ"]},
    "canada": {"name": "Canada", "exchanges": ["TSX", "TSXV"]},
    # Europe
    "uk": {"name": "LSE", "exchanges": ["LSE"]},
    "germany": {"name": "XETRA", "exchanges": ["XETRA"]},
    "france": {"name": "PAR", "exchanges": ["PAR"]},
    "switzerland": {"name": "SIX", "exchanges": ["SIX"]},
    "sweden": {"name": "STO", "exchanges": ["STO"]},
    "norway": {"name": "OSL", "exchanges": ["OSL"]},
    # Asia-Pacific
    "india": {"name": "India", "exchanges": ["BSE", "NSE"]},
    "china": {"name": "China", "exchanges": ["SHZ", "SHH"]},
    "hongkong": {"name": "HKSE", "exchanges": ["HKSE"]},
    "japan": {"name": "JPX", "exchanges": ["JPX"]},
    "korea": {"name": "KSC", "exchanges": ["KSC"]},
    "australia": {"name": "ASX", "exchanges": ["ASX"]},
    "taiwan": {"name": "Taiwan", "exchanges": ["TAI", "TWO"]},
    "thailand": {"name": "SET", "exchanges": ["SET"]},
    "singapore": {"name": "SGX", "exchanges": ["SGX"]},
    # Other
    "brazil": {"name": "SAO", "exchanges": ["SAO"]},
    "mexico": {"name": "BMV", "exchanges": ["BMV"]},
    "southafrica": {"name": "JSE", "exchanges": ["JNB"]},
    "saudi": {"name": "SAU", "exchanges": ["SAU"]},
    "israel": {"name": "TLV", "exchanges": ["TLV"]},
}

# Regional risk-free rates (10-year government bond yields, approximate)
# Used for Sharpe/Sortino ratio calculations. Users can override with --risk-free-rate
REGIONAL_RISK_FREE_RATES = {
    # North America
    "NYSE": 0.020, "NASDAQ": 0.020, "AMEX": 0.020,  # US 10Y Treasury
    "TSX": 0.025, "TSXV": 0.025,  # Canada 10Y
    # Europe
    "LSE": 0.035,    # UK Gilt
    "XETRA": 0.020, "FSX": 0.020,  # Germany Bund
    "PAR": 0.025,    # France OAT
    "SIX": 0.005,    # Switzerland
    "STO": 0.020, "OSL": 0.030,  # Sweden, Norway
    "AMS": 0.025, "BRU": 0.025, "MIL": 0.030,  # Netherlands, Belgium, Italy
    # Asia-Pacific
    "BSE": 0.065, "NSE": 0.065,  # India 10Y
    "SHZ": 0.025, "SHH": 0.025,  # China 10Y
    "HKSE": 0.030,   # Hong Kong
    "JPX": 0.001,    # Japan 10Y (near zero)
    "KSC": 0.030, "KOE": 0.030,  # South Korea
    "ASX": 0.035,    # Australia
    "TAI": 0.010, "TWO": 0.010,  # Taiwan
    "SET": 0.025,    # Thailand
    "SGX": 0.025,    # Singapore
    # Other
    "SAO": 0.105,    # Brazil (high inflation)
    "BMV": 0.080,    # Mexico
    "JSE": 0.090, "JNB": 0.090,    # South Africa
    "SAU": 0.035,    # Saudi Arabia
    "TLV": 0.030,    # Israel
}

# Market cap thresholds by exchange (local currency)
#
# FMP stores marketCap in LOCAL CURRENCY, not USD. These thresholds are set to
# capture liquid mid-to-large cap stocks appropriate for each market structure.
#
# Target: ~$200-500M USD-equivalent for standard strategies
#
# For unlisted exchanges: target_local = target_usd_millions × exchange_rate
# Example: New exchange XYZ with rate 25 XYZ/USD, target $300M
#          → threshold = 300 × 25 = 7,500,000,000 (7.5B XYZ)
#
MKTCAP_THRESHOLD_MAP = {
    # North America (USD) - large-cap focus
    "NYSE": 1_000_000_000, "NASDAQ": 1_000_000_000, "AMEX": 1_000_000_000,  # $1B USD
    "TSX": 500_000_000, "TSXV": 500_000_000,  # C$500M ≈ $362M USD

    # Europe - mid-to-large cap
    "LSE": 500_000_000,       # £500M ≈ $635M USD
    "XETRA": 500_000_000, "FSX": 500_000_000,  # €500M ≈ $545M USD
    "PAR": 500_000_000, "AMS": 500_000_000, "BRU": 500_000_000,  # €500M
    "MIL": 500_000_000, "BME": 500_000_000,  # €500M
    "SIX": 500_000_000,       # CHF 500M ≈ $568M USD
    "STO": 5_000_000_000, "OSL": 5_000_000_000,  # SEK/NOK 5B ≈ $460M USD

    # Asia-Pacific - liquid mid-cap
    "BSE": 20_000_000_000, "NSE": 20_000_000_000,  # ₹20B ≈ $240M USD
    "SHZ": 2_000_000_000, "SHH": 2_000_000_000,  # ¥2B ≈ $276M USD
    "HKSE": 2_000_000_000,    # HK$2B ≈ $256M USD
    "JPX": 100_000_000_000,   # ¥100B ≈ $667M USD
    "KSC": 500_000_000_000, "KOE": 500_000_000_000,  # ₩500B ≈ $370M USD
    "ASX": 500_000_000,       # A$500M ≈ $323M USD
    "TAI": 10_000_000_000, "TWO": 10_000_000_000,  # NT$10B ≈ $312M USD
    "SET": 10_000_000_000,    # ฿10B ≈ $286M USD
    "SGX": 500_000_000, "SES": 500_000_000,  # S$500M ≈ $370M USD

    # Other regions
    "SAO": 1_000_000_000,     # R$1B ≈ $200M USD (limited universe)
    "BMV": 2_000_000_000,     # MXN 2B ≈ $118M USD
    "JSE": 10_000_000_000, "JNB": 10_000_000_000,  # R10B ≈ $550M USD
    "SAU": 1_000_000_000,     # SAR 1B ≈ $267M USD
    "TLV": 1_000_000_000,     # ₪1B ≈ $274M USD
    "JKT": 5_000_000_000_000, # IDR 5T ≈ $310M USD
    "KLS": 1_000_000_000,     # MYR 1B ≈ $224M USD
}

# Lower thresholds for asset-growth and stock-split strategies
# Target: ~$100-250M USD-equivalent (half of standard)
MKTCAP_THRESHOLD_MAP_LOW = {k: v // 2 for k, v in MKTCAP_THRESHOLD_MAP.items()}


def add_common_args(parser):
    """Add common CLI arguments used by all backtest scripts.

    Adds: --exchange, --preset, --global, --api-key, --base-url,
          --output, --verbose, --risk-free-rate, --frequency, --no-costs
    """
    # Exchange selection
    parser.add_argument("--exchange", type=str,
                        help="Exchange code(s), comma-separated (e.g. BSE,NSE)")
    parser.add_argument("--preset", type=str, choices=sorted(EXCHANGE_PRESETS.keys()),
                        help="Use a preset exchange group")
    parser.add_argument("--global", dest="global_bt", action="store_true",
                        help="Backtest all exchanges (no filter)")

    # API
    parser.add_argument("--api-key", type=str,
                        help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str,
                        help="API base URL")

    # Output
    parser.add_argument("--output", type=str,
                        help="Output JSON file for results")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose output")

    # Parameters
    parser.add_argument("--risk-free-rate", type=float, default=None,
                        help="Annual risk-free rate (auto-detected from exchange, or specify manually)")
    parser.add_argument("--frequency", type=str,
                        choices=["monthly", "quarterly", "semi-annual", "annual"],
                        help="Rebalancing frequency (overrides strategy default)")
    parser.add_argument("--no-costs", action="store_true",
                        help="Disable transaction costs (academic baseline)")


def resolve_exchanges(args, default_exchanges=None, default_name="US_MAJOR"):
    """Parse exchange arguments and return (exchange_list, universe_name).

    Args:
        args: parsed argparse namespace
        default_exchanges: list[str] or None - default if nothing specified
        default_name: str - default universe name

    Returns:
        tuple(list[str] | None, str) - (exchanges, universe_name)
        exchanges is None for global mode.
    """
    if default_exchanges is None:
        default_exchanges = ["NYSE", "NASDAQ", "AMEX"]

    if args.global_bt:
        return None, "Global"
    elif args.preset:
        preset = EXCHANGE_PRESETS[args.preset]
        return preset["exchanges"], preset["name"]
    elif args.exchange:
        exchanges = [e.strip().upper() for e in args.exchange.split(",")]
        name = "_".join(exchanges) if len(exchanges) > 1 else exchanges[0]
        return exchanges, name
    else:
        return default_exchanges, default_name


def get_risk_free_rate(exchanges, user_override=None):
    """Get appropriate risk-free rate for a set of exchanges.

    Args:
        exchanges: list[str] or None - exchange codes (e.g. ["BSE", "NSE"])
        user_override: float or None - user-specified rate via --risk-free-rate

    Returns:
        float - annual risk-free rate (e.g. 0.02 for 2%)

    Logic:
        1. If user_override provided, use that
        2. If single exchange or homogeneous region, use regional rate
        3. If multiple exchanges from different regions, use weighted average
        4. If global or unknown, default to 2% (US rate)
    """
    if user_override is not None:
        return user_override

    if not exchanges or len(exchanges) == 0:
        return 0.02  # Global default

    # Get rates for all exchanges
    rates = []
    for ex in exchanges:
        rate = REGIONAL_RISK_FREE_RATES.get(ex, 0.02)
        rates.append(rate)

    # If all same rate, return it
    if len(set(rates)) == 1:
        return rates[0]

    # Multiple regions - return average (simple, equal-weighted)
    return sum(rates) / len(rates)


def get_mktcap_threshold(exchanges, use_low_threshold=False):
    """Get market cap threshold (local currency) for given exchanges.

    FMP stores marketCap in local currency. Returns threshold in the
    appropriate currency for the exchange(s) being backtested.

    Args:
        exchanges: list[str] or None - exchange codes (e.g. ["BSE", "NSE"])
        use_low_threshold: bool - use low threshold map (for asset-growth, stock-split)

    Returns:
        int - threshold in local currency
        Examples: 1_000_000_000 for NYSE ($1B USD)
                  20_000_000_000 for BSE (₹20B ≈ $240M USD)
                  10_000_000_000 for JSE (R10B ≈ $550M USD)

    Logic:
        1. If global mode (exchanges=None), use $1B USD default
        2. If single exchange, use exchange-specific threshold
        3. If multiple exchanges, use min() (most conservative filter)
        4. If exchange unknown, default to 1B local (assumes USD-like scale)
    """
    threshold_map = MKTCAP_THRESHOLD_MAP_LOW if use_low_threshold else MKTCAP_THRESHOLD_MAP
    default = 500_000_000 if use_low_threshold else 1_000_000_000

    if not exchanges or len(exchanges) == 0:
        return default  # Global mode

    # Get thresholds for all exchanges
    thresholds = []
    for ex in exchanges:
        threshold = threshold_map.get(ex, default)
        thresholds.append(threshold)

    # Use minimum (conservative: don't include tiny stocks when mixing currencies)
    return min(thresholds)


def save_results(metrics, period_results, output_dir, universe_name,
                 strategy_name="strategy"):
    """Save backtest results to JSON and CSV files.

    Creates:
        - {output_dir}/returns_{universe_name}.csv
        - {output_dir}/{strategy_name}_metrics_{universe_name}.json (if output_dir specified)

    Args:
        metrics: dict - from compute_metrics()
        period_results: list[dict] - raw period-level results
        output_dir: str - directory to save files
        universe_name: str - e.g. "US_MAJOR", "BSE"
        strategy_name: str - e.g. "qarp", "piotroski"
    """
    os.makedirs(output_dir, exist_ok=True)

    # Save metrics JSON
    json_path = os.path.join(output_dir, f"{strategy_name}_metrics_{universe_name}.json")
    with open(json_path, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"  Metrics saved to {json_path}")

    # Save period-level CSV
    csv_path = os.path.join(output_dir, f"returns_{universe_name}.csv")
    if period_results:
        headers = list(period_results[0].keys())
        with open(csv_path, "w") as f:
            f.write(",".join(headers) + "\n")
            for row in period_results:
                values = [str(row.get(h, "")) for h in headers]
                f.write(",".join(values) + "\n")
        print(f"  Returns saved to {csv_path}")


def print_header(strategy_name, universe_name, exchanges, signal_desc):
    """Print standard backtest header.

    Args:
        strategy_name: str - e.g. "QARP BACKTEST"
        universe_name: str - e.g. "US_MAJOR"
        exchanges: list[str] or None
        signal_desc: str - signal description
    """
    print("=" * 65)
    print(f"  {strategy_name}")
    ex_str = f" ({', '.join(exchanges)})" if exchanges else ""
    print(f"  Universe: {universe_name}{ex_str}")
    print(f"  Signal: {signal_desc}")
    print("=" * 65)
