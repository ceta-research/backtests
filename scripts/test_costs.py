#!/usr/bin/env python3
"""Offline check of costs.trade_costs, the walk of the "trades" cost convention (no network).

The convention charges only real trades, each sized to the trade. A series' position is its last invested
period's book at its end weights, (1 + r) over the book's sum of (1 + r), each name with the rate it carried
there. A rebalance into n names is charged to the period that starts at it: a name in the book trades the gap
between 1/n and its weight in the position (0 for an entry) at the new period's rate, and a name that leaves
sells its weight at the rate it last carried. A cash period (None, or an empty book) sells the position, and
that sale is charged to the last invested period. The run's last book is not sold.

RUN is one series over thirteen periods, each expected cost typed here with its sum beside it: a first period, a
carried book with drift where one tier falls and one rises, an exit at the rate the name last carried (a rate
no name of the new book has) beside an entry, an unchanged book with equal returns (nothing to pay), a book that
doubles in size, a cash sale at drifted weights, two cash periods in a row, re-entry, a name at -100% that leaves
at no weight beside a survivor that keeps the whole position, a period that books nothing, a book all at -100%,
which leaves no position, and a book that shrinks with a position held. SHORT holds a run that starts in cash, a
one-period run and an empty one. ORDER is one book typed in two insertion orders, on values whose left-to-right
sums differ: the costs must be bit-equal. OFF_RANGE holds the returns no book can hold (under -100%, not
finite): each must raise ValueError.

Every mutant of costs.py must fail a check, and by a wrong answer, not an error.

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
    # 1  both carried, each trading |1/2 - 6/11| = |1/2 - 5/11| = 1/22 at its new rate: A's tier fell to 0.1% and
    #    B's rose to 0.5%, (0.001 + 0.005)/22 = 3/11000. (Their old rates would give 4/11000, the lower of old
    #    and new 2/11000, the higher 5/11000.) End weights: A 1.5/2.5 = 0.6, B 1.0/2.5 = 0.4.
    ({"A": (0.50, LO), "B": (0.00, HI)}, 3 / 11000),
    # 2  A leaves beside C's entry, B carried: A sells 0.6 at the 0.1% it last carried, a rate no name of the new
    #    book has (0.0006, where its entry rate would give 0.003 and B's new one 0.0018), B trades |1/2 - 0.4| x
    #    0.003 = 0.0003, C enters at 1/2 x 0.005 = 0.0025: 0.0034. End weights: 1/2 each, on equal returns.
    ({"B": (0.10, MID), "C": (0.10, HI)}, 0.0034),
    # 3  the same book after equal returns: no gap, nothing to pay.
    #    End weights: B 1.25/1.75 = 5/7, C 0.5/1.75 = 2/7.
    ({"B": (0.25, MID), "C": (-0.50, HI)}, 0.0),
    # 4  the book grows to four: B trades 5/7 - 1/4 = 13/28 and C 2/7 - 1/4 = 1/28 at its new 0.1%, (13 x 0.003 +
    #    0.001)/28 = 1/700; D and E enter at 1/4 x 0.005 each, 0.0025. D doubles, so the end weights are B, C and
    #    E 1/5 and D 2/5. The next period is cash, so this row also takes the sale of its book at those weights:
    #    (0.003 + 0.001 + 2 x 0.005 + 0.005)/5 = 0.0038 (at 1/4 each it would read 0.0035). 1/700 + 0.0063.
    ({"B": (0.0, MID), "C": (0.0, LO), "D": (1.0, HI), "E": (0.0, HI)}, 1 / 700 + 0.0063),
    # 5, 6  cash twice: the sale is on period 4, and the second cash period has nothing to sell.
    (None, 0.0),
    (None, 0.0),
    # 7  re-entry from an empty position: 1/2 x 0.003 + 1/2 x 0.005 = 0.004. (Against period 4's book it would
    #    read 0.0026.) D ends at -100%: end weights B 1, D 0.
    ({"B": (0.0, MID), "D": (-1.0, HI)}, 0.004),
    # 8  D leaves at no weight and pays nothing; B, the survivor, holds the whole position and trades |1/3 - 1| x
    #    0.003 = 0.002 (an entry would pay 0.001); C enters at 1/3 x 0.001 and E at 1/3 x 0.005, 0.002: 0.004.
    #    End weights: B 1/3.5 = 2/7, C 1.5/3.5 = 3/7, E 2/7. The next period books nothing, so this row takes the
    #    sale too, (2 x 0.003 + 3 x 0.001 + 2 x 0.005)/7 = 0.019/7 (at 1/3 each it would read 0.003).
    ({"B": (0.0, MID), "C": (0.5, LO), "E": (0.0, HI)}, 0.004 + 0.019 / 7),
    # 9  a period that books no name: a cash period.
    ({}, 0.0),
    # 10 entries from an empty position, 1/2 x 0.003 + 1/2 x 0.001 = 0.002. Both names end at -100%, so the sum
    #    of (1 + r) is zero and no position is left.
    ({"B": (-1.0, MID), "C": (-1.0, LO)}, 0.002),
    # 11 entries from an empty position, (0.005 + 0.003 + 0.001)/3 = 0.003.
    #    End weights: A 1.5/3.5 = 3/7, B 2/7, C 2/7.
    ({"A": (0.5, HI), "B": (0.0, MID), "C": (0.0, LO)}, 0.003),
    # 12 the book shrinks to two with a position held: A trades |1/2 - 3/7| = 1/14 at 0.003 and B |1/2 - 2/7| =
    #    3/14 at 0.005, 0.018/14; C sells 2/7 at its 0.001, 0.004/14: 0.022/14 = 11/7000. (A target of 1/3, one
    #    over the larger book, would give 0.017/21.) The run ends invested and pays no sale.
    ({"A": (0.0, MID), "B": (0.0, HI)}, 11 / 7000),
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
# Returns no book can hold. Under -100% the name's end weight is negative and its sale a negative cost; the book
# beside each keeps the sum of (1 + r) positive, so nothing else stops the walk.
OFF_RANGE = {"a return under -100%": -1.5, "a return that is not a number": float("nan"),
             "an infinite return": float("inf")}


def in_order(items):
    return [dict(items), {s: ORDER_SECOND[s] for s, _ in items}]


def off_range(walk, r):
    """'' when the walk raises ValueError on a book with return r, else what it did."""
    try:
        return f"no ValueError, costs {walk([{'A': (r, HI), 'B': (1.0, LO)}, {'B': (0.0, LO)}])}"
    except ValueError:
        return ""


def failures(src):
    try:
        walk = load(src).trade_costs
        got = walk([b for b, _ in RUN])
        short = {name: walk(books) for name, (books, _) in SHORT.items()}
        fwd, back = walk(in_order(ORDER_FIRST)), walk(in_order(ORDER_FIRST[::-1]))
        refused = {name: off_range(walk, r) for name, r in OFF_RANGE.items()}
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    out = [f"period {i}: cost {g!r}, want {want!r}" for i, (g, (_, want)) in enumerate(zip(got, RUN))
           if abs(g - want) > TOL]
    out += [f"{name}: {why}" for name, why in refused.items() if why]
    if len(got) != len(RUN):
        out.append(f"{len(got)} costs for {len(RUN)} periods")
    for name, (_, want) in SHORT.items():
        if len(short[name]) != len(want) or any(abs(g - w) > TOL for g, w in zip(short[name], want)):
            out.append(f"{name}: costs {short[name]}, want {want}")
    if fwd != back:
        out.append(f"one book in two orders: {[x.hex() for x in fwd]} against {[x.hex() for x in back]}")
    return out


SALE ="[w * rate for w, rate in held.values()]"
EXIT = "terms[i] += [w * rate for s, (w, rate) in held.items() if s not in book]"
CARRIED = "* rate for s, (_, rate) in book.items()]"
TARGET = "target = 1.0 / len(book)"
RANGE = "math.isfinite(r) and r >= -1.0"
END = "    return [math.fsum(t) for t in terms]"
MUTANTS = {
    "an entry charged nothing": ("held.get(s, (0.0, 0.0))[0]", "held.get(s, (target, 0.0))[0]"),
    "end weights not drifted": ("{s: ((1.0 + r) / total, rate) for", "{s: (target, rate) for"),
    "a carried name at its old rate": (CARRIED, "* held.get(s, (0.0, rate))[1] for s, (_, rate) in book.items()]"),
    "a carried name at the lower of its old and new rates": (
        CARRIED, "* min(rate, held.get(s, (0.0, rate))[1]) for s, (_, rate) in book.items()]"),
    "the target weight over the larger of the old and new books": (TARGET, "target = 1.0 / max(len(book), len(held))"),
    "an exit at the rate the name entered on": ("((1.0 + r) / total, rate)",
                                                "((1.0 + r) / total, held.get(s, (0.0, rate))[1])"),
    "an exit at the new book's first name's rate": (EXIT, EXIT.replace("w * rate", "w * next(iter(book.values()))[1]")),
    "an exit charged nothing": (f"        {EXIT}\n", ""),
    "no sale in a cash period": (f"                terms[last] += {SALE}\n", "                pass\n"),
    "the cash sale on the cash period": (f"terms[last] += {SALE}", f"terms[i] += {SALE}"),
    "the cash sale at equal weights": (f"terms[last] += {SALE}",
                                       "terms[last] += [rate / len(held) for w, rate in held.values()]"),
    "no reset after cash": ("            held = {}\n            continue", "            continue"),
    # two edits: the empty book flows on as a rebalance into no name, without dividing by zero
    "an empty book read as a rebalance": [("if not book:", "if book is None:"),
                                          (TARGET, f"{TARGET} if book else 0.0")],
    "a return under -100% taken": (RANGE, "math.isfinite(r)"),
    "a return that is not finite taken": (RANGE, "r >= -1.0 or r != r"),
    "a book all at -100% keeps a position": ("if total > 0 else {}",
                                             "if total > 0 else {s: (target, rate) for s, (_, rate) in book.items()}"),
    "a name at -100% empties the whole position": (
        "        last = i\n",
        "        if any(r <= -1.0 for r, _ in book.values()):\n            held = {}\n        last = i\n"),
    "a sale at the run's end": (END, f"    if held:\n        terms[last] += {SALE}\n{END}"),
    "the trades added in book order": (
        "[math.fsum(t) for t in terms]", "[__import__('functools').reduce(float.__add__, t, 0.0) for t in terms]"),
    "the end values added in book order": (
        "math.fsum(1.0 + r for r, _ in book.values())",
        "__import__('functools').reduce(float.__add__, [1.0 + r for r, _ in book.values()], 0.0)"),
}


def main():
    base = failures(SRC)
    if base:
        print("test_costs: FAIL", base)
        sys.exit(1)
    survived, errored = [], []
    for name, edits in MUTANTS.items():
        src = SRC
        for old, new in edits if isinstance(edits, list) else [edits]:
            if SRC.count(old) != 1:
                print(f"test_costs: FAIL (mutant anchor not unique: {name})")
                sys.exit(1)
            src = src.replace(old, new)
        got = failures(src)
        if not got:
            survived.append(name)
        elif all(f.startswith("error:") for f in got):
            errored.append(name)
    if survived or errored:
        print("test_costs: FAIL (mutants survived:", survived, "; caught only by an error:", errored, ")")
        sys.exit(1)
    print(f"test_costs: PASS ({len(RUN)} periods, {len(SHORT)} short runs, one book in two orders and "
          f"{len(OFF_RANGE)} returns refused; {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
