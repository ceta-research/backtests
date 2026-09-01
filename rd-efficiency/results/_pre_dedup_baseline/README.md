# Pre-dedup baseline: the five domicile legs that exist nowhere else

Snapshot of the 2026-08-31 evening run, **before** `screen_stocks()` gained its
one-listing-per-company guard. Every other baseline file is recoverable from commit `82842c0`;
these five domicile legs were generated after that commit and only ever lived in a session
scratchpad under `/private/tmp`, which is ephemeral.

They are the source for the before/after pairs published in the comparison blog's "What Changed"
section, and they are what makes the dedup fix's impact reproducible. Kept for provenance only —
**no published figure should ever be read from this directory.**

Preserved 2026-09-01.
