#!/usr/bin/env python3
"""Offline check of costs.trade_costs, the walk of the "trades" cost convention (no network).

The convention charges only real trades, each sized to the trade. A series' position is its last invested
period's book at its end weights, (1 + r) over the book's sum of (1 + r), each name with the rate it carried
there. A rebalance into n names is charged to the period that starts at it: a name in the book trades the gap
between 1/n and its weight in the position (0 for an entry) at the new period's rate, and a name that leaves
sells its weight at the rate it last carried. A cash period (None, or an empty book) sells the position, and
that sale is charged to the last invested period. The run's last book is not sold.

RUN is one series over twelve periods, each expected cost typed here with its sum beside it: a first period, a
carried book with drift across a tier change, an exit at the rate the name last carried beside an entry, an
unchanged book with equal returns (nothing to pay), a book that doubles in size, two cash periods in a row,
re-entry, a name at -100% that leaves at no weight, a period that books nothing, and a book all at -100%, which
leaves no position. SHORT holds a run that starts in cash, a one-period run and an empty one. ORDER is one book
typed in two insertion orders, on values whose left-to-right sums differ: the costs must be bit-equal.

Every mutant of costs.py must fail a check, and by a wrong number, not an error, but for the one that reads an
empty book as a rebalance, which can only divide by zero.

Usage:
    python scripts/test_costs.py [repo root]
"""
import os
import sys
import types

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "costs.py")
with open(PATH) as fh:
    SRC = fh.read()

HI, MID, LO = 0.005, 0.003, 0.001   # the three one-way tiers
TOL = 1e-12


def load(src):
    mod = types.ModuleType("costs_under_test")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


# (book or None, expected cost). A book is {symbol: (return before costs, one-way rate)}.
RUN = [
    # 0  the first period, every name an entry: 1/2 x 0.005 + 1/2 x 0.003 = 0.004.
    #    End weights: A 1.2/2.2 = 6/11, B 1.0/2.2 = 5/11.
    ({"A": (0.20, HI), "B": (0.00, MID)}, 0.004),
    # 1  both carried, A now on the 0.3% tier: each trades |1/2 - 6/11| = |1/2 - 5/11| = 1/22 at its new rate,
    #    1/22 x 0.003 x 2 = 3/11000. (A at its old 0.5% would give 1/22 x 0.008 = 4/11000.)
    #    End weights: A 1.5/2.5 = 0.6, B 1.0/2.5 = 0.4.
    ({"A": (0.50, MID), "B": (0.00, MID)}, 3 / 11000),
    # 2  A leaves beside C's entry, B carried: A sells 0.6 at the 0.3% it last carried (0.0018, where its entry
    #    rate would give 0.003), B trades |1/2 - 0.4| x 0.003 = 0.0003, C enters at 1/2 x 0.001 = 0.0005: 0.0026.
    #    End weights: 1/2 each, on equal returns.
    ({"B": (0.10, MID), "C": (0.10, LO)}, 0.0026),
    # 3  the same book after equal returns: no gap, nothing to pay.
    #    End weights: B 1.25/1.75 = 5/7, C 0.5/1.75 = 2/7.
    ({"B": (0.25, MID), "C": (-0.50, LO)}, 0.0),
    # 4  the book grows to four: B trades 5/7 - 1/4 = 13/28 and C 2/7 - 1/4 = 1/28, (13 x 0.003 + 0.001)/28 =
    #    1/700; D and E enter at 1/4 x 0.005 each, 0.0025. The next period is cash, so this row also takes the
    #    sale of its book at 1/4 each: 1/4 x (0.003 + 0.001 + 0.005 + 0.005) = 0.0035. 1/700 + 0.006 = 13/1750.
    ({"B": (0.0, MID), "C": (0.0, LO), "D": (0.0, HI), "E": (0.0, HI)}, 13 / 1750),
    # 5, 6  cash twice: the sale is on period 4, and the second cash period has nothing to sell.
    (None, 0.0),
    (None, 0.0),
    # 7  re-entry from an empty position: 1/2 x 0.003 + 1/2 x 0.005 = 0.004. (Against period 4's book it would
    #    read 0.0035.) D ends at -100%: end weights B 1, D 0.
    ({"B": (0.0, MID), "D": (-1.0, HI)}, 0.004),
    # 8  D leaves at no weight and pays nothing; B trades |1/2 - 1| x 0.003 = 0.0015, E enters at 0.0025: 0.004.
    #    The next period books nothing, so this row takes the sale too, 1/2 x 0.003 + 1/2 x 0.005 = 0.004: 0.008.
    ({"B": (0.0, MID), "E": (0.0, HI)}, 0.008),
    # 9  a period that books no name: a cash period.
    ({}, 0.0),
    # 10 entries from an empty position, 1/2 x 0.003 + 1/2 x 0.001 = 0.002. Both names end at -100%, so the sum
    #    of (1 + r) is zero and no position is left.
    ({"B": (-1.0, MID), "C": (-1.0, LO)}, 0.002),
    # 11 an entry from an empty position, 1 x 0.003. The run ends invested and pays no sale.
    ({"B": (0.0, MID)}, 0.003),
]
SHORT = {
    "a run that starts in cash": ([None, None, {"A": (0.10, HI)}], [0.0, 0.0, 0.005]),
    # entries only, 1/2 x 0.005 + 1/2 x 0.001, and no sale at the end
    "a one-period run": ([{"A": (0.10, HI), "B": (0.20, LO)}], [0.003]),
    "no period": ([], []),
}
# One book in two insertion orders, then the same names carried. 1.1 + 1.2 + 1.3 + 1.7 and the second period's
# four trades each add to a different value left to right from either end, so a walk that adds in book order
# gives two answers (the last two mutants).
ORDER_FIRST = [("A", (0.1, HI)), ("B", (0.2, MID)), ("C", (0.3, LO)), ("D", (0.7, HI))]
ORDER_SECOND = {"A": (0.0, HI), "B": (0.0, MID), "C": (0.0, LO), "D": (0.0, HI)}


def in_order(items):
    return [dict(items), {s: ORDER_SECOND[s] for s, _ in items}]


def failures(src):
    try:
        walk = load(src).trade_costs
        got = walk([b for b, _ in RUN])
        short = {name: walk(books) for name, (books, _) in SHORT.items()}
        fwd, back = walk(in_order(ORDER_FIRST)), walk(in_order(ORDER_FIRST[::-1]))
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    out = [f"period {i}: cost {g!r}, want {want!r}" for i, (g, (_, want)) in enumerate(zip(got, RUN))
           if abs(g - want) > TOL]
    if len(got) != len(RUN):
        out.append(f"{len(got)} costs for {len(RUN)} periods")
    for name, (_, want) in SHORT.items():
        if len(short[name]) != len(want) or any(abs(g - w) > TOL for g, w in zip(short[name], want)):
            out.append(f"{name}: costs {short[name]}, want {want}")
    if fwd != back:
        out.append(f"one book in two orders: {[x.hex() for x in fwd]} against {[x.hex() for x in back]}")
    return out


SALE ="[w * rate for w, rate in held.values()]"
END = "    return [math.fsum(t) for t in terms]"
MUTANTS = {
    "an entry charged nothing": ("held.get(s, (0.0, 0.0))[0]", "held.get(s, (target, 0.0))[0]"),
    "end weights not drifted": ("{s: ((1.0 + r) / total, rate) for", "{s: (target, rate) for"),
    "a carried name at its old rate": ("* rate for s, (_, rate) in book.items()]",
                                       "* held.get(s, (0.0, rate))[1] for s, (_, rate) in book.items()]"),
    "an exit at the rate the name entered on": ("((1.0 + r) / total, rate)",
                                                "((1.0 + r) / total, held.get(s, (0.0, rate))[1])"),
    "an exit charged nothing": ("        terms[i] += [w * rate for s, (w, rate) in held.items() if s not in book]\n",
                                ""),
    "no sale in a cash period": (f"                terms[last] += {SALE}\n", "                pass\n"),
    "the cash sale on the cash period": (f"terms[last] += {SALE}", f"terms[i] += {SALE}"),
    "no reset after cash": ("            held = {}\n            continue", "            continue"),
    "an empty book read as a rebalance": ("if not book:", "if book is None:"),
    "a book all at -100% keeps a position": ("if total > 0 else {}",
                                             "if total > 0 else {s: (target, rate) for s, (_, rate) in book.items()}"),
    "a sale at the run's end": (END, f"    if held:\n        terms[last] += {SALE}\n{END}"),
    "the trades added in book order": (
        "[math.fsum(t) for t in terms]", "[__import__('functools').reduce(float.__add__, t, 0.0) for t in terms]"),
    "the end values added in book order": (
        "math.fsum(1.0 + r for r, _ in book.values())",
        "__import__('functools').reduce(float.__add__, [1.0 + r for r, _ in book.values()], 0.0)"),
}
BY_ERROR = {"an empty book read as a rebalance"}   # 1 / len({}) can only raise


def main():
    base = failures(SRC)
    if base:
        print("test_costs: FAIL", base)
        sys.exit(1)
    survived, errored = [], []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_costs: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        got = failures(SRC.replace(old, new))
        if not got:
            survived.append(name)
        elif name not in BY_ERROR and all(f.startswith("error:") for f in got):
            errored.append(name)
    if survived or errored:
        print("test_costs: FAIL (mutants survived:", survived, "; caught only by an error:", errored, ")")
        sys.exit(1)
    print(f"test_costs: PASS ({len(RUN)} periods, {len(SHORT)} short runs and one book in two orders; "
          f"{len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
