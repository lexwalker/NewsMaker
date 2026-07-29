"""UTF-8 console setup that survives being imported (not just run).

Every entry-point script forces UTF-8 stdio — a Windows console defaults to
cp1251 and a single Cyrillic headline in a print() kills a multi-hour run with
UnicodeEncodeError. The idiom used so far was:

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", ...)

which is fine for a process that only ever RUNS the script, and wrong the
moment the script is IMPORTED: the replaced wrapper loses its last reference,
and when the garbage collector finalises it, TextIOWrapper closes the very
buffer the new wrapper is writing to. Under pytest — where sys.stdout is the
capture object and nothing else holds it — the whole suite died with
``ValueError: I/O operation on closed file`` as soon as enough modules were
collected to trigger a GC pass (jul-30: 36 files was the tipping point, and
collecting fewer, or any single file alone, passed — which is exactly why this
looked like flakiness rather than a bug).

``reconfigure`` changes the encoding of the EXISTING stream, so there is no
orphan wrapper to finalise and nothing to close.
"""

from __future__ import annotations

import sys


def force_utf8_stdio(*, line_buffering: bool = True) -> None:
    """Best-effort UTF-8 on stdout/stderr. Safe to call on import, twice, or
    under a test harness that replaced the streams with its own objects."""
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            # A capture object / StringIO with no reconfigure: leave it alone.
            # It is already unicode-capable, which is all we actually need.
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace",
                        line_buffering=line_buffering)
        except (ValueError, OSError):
            # Detached, closed, or not a text stream — printing is still
            # possible, so never let console setup abort the program.
            continue
