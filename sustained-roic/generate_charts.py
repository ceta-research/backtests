#!/usr/bin/env python3
"""
Generate charts for Sustained ROIC backtest results.

Reads exchange_comparison.json and produces:
- Per-exchange: cumulative growth + annual returns bar charts
- Comparison: CAGR across exchanges, max drawdown comparison

Usage:
    python3 sustained-roic/generate_charts.py
"""

import json
import os
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

CHART_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")
RESULTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "results", "exchange_comparison.json")

# Chart style
plt.rcParams.update({
    'figure.figsize': (12, 6),
    'font.size': 12,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

COLORS = {
    'sustained': '#2563eb',   # Blue
    'single_year': '#f59e0b', # Amber
    'low': '#dc2626',         # Red
    'spy': '#6b7280',         # Gray
}

REGION_MAP = {
    'US_MAJOR': 'us',
    'India': 'india',
    'XETRA': 'germany',
    'LSE': 'uk',
    'China': 'china',
    'HKSE': 'hongkong',
}


def cumulative_growth(returns):
    """Convert list of period returns to cumulative growth of $10,000."""
    growth = [10000]
    for r in returns:
        growth.append(growth[-1] * (1 + r / 100))
    return growth


def plot_cumulative(data, exchange_name, region_key):
    """Plot cumulative growth: sustained vs SPY."""
    annual = data.get('annual_returns', [])
    if not annual:
        return

    years = [a['year'] for a in annual]
    sust_rets = [a['sustained'] for a in annual]
    spy_rets = [a['spy'] for a in annual]

    sust_growth = cumulative_growth(sust_rets)
    spy_growth = cumulative_growth(spy_rets)

    x_labels = years + [years[-1] + 1]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(x_labels, sust_growth, color=COLORS['sustained'],
            linewidth=2.5, label=f'Sustained ROIC ({data["portfolios"]["sustained"]["cagr"]}% CAGR)')
    ax.plot(x_labels, spy_growth, color=COLORS['spy'],
            linewidth=2, linestyle='--', label=f'S&P 500 ({data["portfolios"]["sp500"]["cagr"]}% CAGR)')

    ax.set_title(f'Growth of $10,000: Sustained ROIC vs S&P 500 ({exchange_name})',
                 fontsize=14, fontweight='bold')
    ax.set_xlabel('Year')
    ax.set_ylabel('Portfolio Value ($)')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax.legend(loc='upper left', fontsize=11)

    start_yr = years[0]
    end_yr = years[-1] + 1
    ax.annotate(f'{start_yr}-{end_yr}, Annual Rebalance, Equal Weight, Size-Tiered Costs',
                xy=(0.5, -0.12), xycoords='axes fraction', ha='center',
                fontsize=9, color='gray')

    plt.tight_layout()
    fname = f'1_{region_key}_cumulative_growth.png'
    plt.savefig(os.path.join(CHART_DIR, fname), dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved {fname}")


def plot_annual_returns(data, exchange_name, region_key):
    """Plot annual returns bar chart: sustained vs SPY."""
    annual = data.get('annual_returns', [])
    if not annual:
        return

    years = [a['year'] for a in annual]
    sust_rets = [a['sustained'] for a in annual]
    spy_rets = [a['spy'] for a in annual]

    x = np.arange(len(years))
    width = 0.35

    fig, ax = plt.subplots(figsize=(14, 6))
    bars1 = ax.bar(x - width/2, sust_rets, width, color=COLORS['sustained'],
                   label='Sustained ROIC', alpha=0.85)
    bars2 = ax.bar(x + width/2, spy_rets, width, color=COLORS['spy'],
                   label='S&P 500', alpha=0.85)

    ax.axhline(y=0, color='black', linewidth=0.5)
    ax.set_title(f'Annual Returns: Sustained ROIC vs S&P 500 ({exchange_name})',
                 fontsize=14, fontweight='bold')
    ax.set_xlabel('Year')
    ax.set_ylabel('Return (%)')
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha='right', fontsize=9)
    ax.legend(loc='upper left', fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{x:.0f}%'))

    plt.tight_layout()
    fname = f'2_{region_key}_annual_returns.png'
    plt.savefig(os.path.join(CHART_DIR, fname), dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved {fname}")


def plot_comparison_cagr(all_data):
    """Plot CAGR comparison across exchanges."""
    exchanges = []
    sust_cagrs = []
    spy_cagrs = []

    for name, data in all_data.items():
        if 'error' in data:
            continue
        exchanges.append(name)
        sust_cagrs.append(data['portfolios']['sustained']['cagr'])
        spy_cagrs.append(data['portfolios']['sp500']['cagr'])

    # Sort by sustained CAGR descending
    sorted_idx = sorted(range(len(sust_cagrs)), key=lambda i: sust_cagrs[i], reverse=True)
    exchanges = [exchanges[i] for i in sorted_idx]
    sust_cagrs = [sust_cagrs[i] for i in sorted_idx]
    spy_cagrs = [spy_cagrs[i] for i in sorted_idx]

    x = np.arange(len(exchanges))
    width = 0.35

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.barh(x - width/2, sust_cagrs, width, color=COLORS['sustained'],
            label='Sustained ROIC', alpha=0.85)
    ax.barh(x + width/2, spy_cagrs, width, color=COLORS['spy'],
            label='S&P 500 (benchmark)', alpha=0.85)

    ax.set_title('Sustained ROIC CAGR by Exchange',
                 fontsize=14, fontweight='bold')
    ax.set_xlabel('CAGR (%)')
    ax.set_yticks(x)
    ax.set_yticklabels(exchanges, fontsize=11)
    ax.legend(loc='lower right', fontsize=11)
    ax.axvline(x=0, color='black', linewidth=0.5)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{x:.0f}%'))

    plt.tight_layout()
    fname = '1_comparison_cagr.png'
    plt.savefig(os.path.join(CHART_DIR, fname), dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved {fname}")


def plot_comparison_drawdown(all_data):
    """Plot max drawdown comparison across exchanges."""
    exchanges = []
    sust_dd = []
    spy_dd = []

    for name, data in all_data.items():
        if 'error' in data:
            continue
        exchanges.append(name)
        sust_dd.append(data['portfolios']['sustained']['max_drawdown'])
        spy_dd.append(data['portfolios']['sp500']['max_drawdown'])

    # Sort by sustained drawdown (least negative first)
    sorted_idx = sorted(range(len(sust_dd)), key=lambda i: sust_dd[i], reverse=True)
    exchanges = [exchanges[i] for i in sorted_idx]
    sust_dd = [sust_dd[i] for i in sorted_idx]
    spy_dd = [spy_dd[i] for i in sorted_idx]

    x = np.arange(len(exchanges))
    width = 0.35

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.barh(x - width/2, sust_dd, width, color=COLORS['sustained'],
            label='Sustained ROIC', alpha=0.85)
    ax.barh(x + width/2, spy_dd, width, color=COLORS['spy'],
            label='S&P 500', alpha=0.85)

    ax.set_title('Maximum Drawdown by Exchange',
                 fontsize=14, fontweight='bold')
    ax.set_xlabel('Max Drawdown (%)')
    ax.set_yticks(x)
    ax.set_yticklabels(exchanges, fontsize=11)
    ax.legend(loc='lower left', fontsize=11)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{x:.0f}%'))

    plt.tight_layout()
    fname = '2_comparison_drawdown.png'
    plt.savefig(os.path.join(CHART_DIR, fname), dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved {fname}")


def main():
    os.makedirs(CHART_DIR, exist_ok=True)

    if not os.path.exists(RESULTS_FILE):
        print(f"Results file not found: {RESULTS_FILE}")
        print("Run: python3 sustained-roic/backtest.py --global --output sustained-roic/results/exchange_comparison.json")
        sys.exit(1)

    with open(RESULTS_FILE) as f:
        all_data = json.load(f)

    print("Generating charts...")

    # Per-exchange charts
    for exchange_key, region_key in REGION_MAP.items():
        if exchange_key not in all_data or 'error' in all_data[exchange_key]:
            print(f"  Skipping {exchange_key} (no data)")
            continue
        print(f"\n  {exchange_key}:")
        plot_cumulative(all_data[exchange_key], exchange_key, region_key)
        plot_annual_returns(all_data[exchange_key], exchange_key, region_key)

    # Comparison charts
    print(f"\n  Comparison:")
    plot_comparison_cagr(all_data)
    plot_comparison_drawdown(all_data)

    print(f"\nAll charts saved to {CHART_DIR}/")
    print(f"Move to ts-content-creator blog dirs before publishing.")


if __name__ == '__main__':
    main()
