"""Google Sheets write helpers — pure, unit-testable.

Google Sheets hard-caps a single cell at 50 000 characters. Bad HTML
extraction occasionally yields a multi-KB "title" / note that exceeds
that limit and makes the whole `values.update` 400 — discarding a
finished, paid-for LLM pass (see v38: classification completed, $0.52
spent, then the sheet write threw HttpError 400).

``clamp_cells`` defensively truncates every over-long string cell so a
single rogue value can never abort the batch write.
"""

from __future__ import annotations

from typing import Any

# 45 000, not 50 000 — leaves margin for USER_ENTERED re-encoding
# (e.g. a value Sheets reinterprets/expands) and for safety.
SHEETS_CELL_MAX = 45_000


def clamp_cells(
    rows: list[list[Any]], max_len: int = SHEETS_CELL_MAX
) -> list[list[Any]]:
    """Return a new grid with every over-long string cell truncated.

    Pure and non-mutating. Non-string cells (int / float / None) pass
    through untouched so numeric columns keep their type.
    """
    return [
        [
            (c[:max_len] if isinstance(c, str) and len(c) > max_len else c)
            for c in row
        ]
        for row in rows
    ]
