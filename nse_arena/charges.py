"""NSE brokerage charges — Zerodha rates.

Intraday (MIS): Lower STT (sell-side only), both-side exchange/SEBI.
Delivery (CNC): Higher STT (both sides), stamp duty. Zero brokerage.

Adapted from ATO_Simulator.util.broker_functions.calculate_charges()
"""


def nse_intraday_charges(order_value: float) -> float:
    """Round-trip intraday brokerage for NSE equity (Zerodha MIS)."""
    brokerage_per_leg = min(order_value * 0.0003, 20.0)
    brokerage = brokerage_per_leg * 2
    stt = order_value * 0.00025               # 0.025% sell-side only
    exchange = order_value * 0.0000345 * 2    # 0.00345% both sides
    sebi = order_value * 0.000001 * 2         # Rs 10/crore both sides
    stamp = order_value * 0.00003             # 0.003% buy-side only
    gst = (brokerage + exchange) * 0.18
    return round(brokerage + stt + exchange + sebi + stamp + gst, 2)


def nse_delivery_charges(order_value: float) -> float:
    """Round-trip delivery brokerage for NSE equity (Zerodha CNC).

    Zerodha charges ZERO brokerage on delivery trades. Only statutory charges.
    """
    stt = order_value * 0.001 * 2             # 0.1% both sides
    exchange = order_value * 0.0000345 * 2    # 0.00345% both sides
    sebi = order_value * 0.000001 * 2         # Rs 10/crore both sides
    stamp = order_value * 0.00015             # 0.015% buy-side only
    gst = exchange * 0.18                     # GST on exchange txn only (zero brokerage)
    return round(stt + exchange + sebi + stamp + gst, 2)
