"""UTF-8 console setup must be safe to IMPORT, not just to run.

The old idiom (sys.stdout = TextIOWrapper(sys.stdout.buffer, ...)) left the
replaced wrapper unreferenced; its GC finaliser closed the buffer the new
wrapper writes to. Under pytest that buffer belongs to the capture object, so
the entire suite died with «ValueError: I/O operation on closed file» once
enough modules were collected to trigger a GC pass — 36 files was the tipping
point on jul-30, and any single file alone passed, which is why it read as
flakiness for weeks while 859 tests silently never ran.
"""

import gc
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.core.console import force_utf8_stdio  # noqa: E402


def test_survives_a_stream_without_reconfigure() -> None:
    """A StringIO has no .reconfigure — must be left alone, not crash."""
    orig = sys.stdout
    try:
        sys.stdout = io.StringIO()
        force_utf8_stdio()
        print("Кириллица")
        assert "Кириллица" in sys.stdout.getvalue()
    finally:
        sys.stdout = orig


def test_does_not_close_the_stream_it_replaces() -> None:
    """The actual regression: after setup + a GC pass, the stream still writes."""
    orig = sys.stdout
    try:
        buf = io.BytesIO()
        sys.stdout = io.TextIOWrapper(buf, encoding="utf-8")
        force_utf8_stdio()
        gc.collect()
        print("Опубликовано")
        sys.stdout.flush()
        assert not sys.stdout.closed
        assert "Опубликовано" in buf.getvalue().decode("utf-8")
    finally:
        sys.stdout = orig


def test_sets_utf8_on_a_real_text_stream() -> None:
    orig = sys.stdout
    try:
        sys.stdout = io.TextIOWrapper(io.BytesIO(), encoding="cp1251")
        force_utf8_stdio()
        assert sys.stdout.encoding.lower().replace("-", "") == "utf8"
    finally:
        sys.stdout = orig


def test_is_idempotent() -> None:
    orig = sys.stdout
    try:
        sys.stdout = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
        force_utf8_stdio()
        force_utf8_stdio()
        assert not sys.stdout.closed
    finally:
        sys.stdout = orig


def test_cyrillic_survives_a_cp1251_target() -> None:
    """errors='replace' keeps a Cyrillic headline from killing a long run."""
    orig = sys.stdout
    try:
        buf = io.BytesIO()
        sys.stdout = io.TextIOWrapper(buf, encoding="cp1251")
        force_utf8_stdio()
        print("АВТОВАЗ представил Lada Azimut")
        sys.stdout.flush()
        assert "АВТОВАЗ" in buf.getvalue().decode("utf-8")
    finally:
        sys.stdout = orig
