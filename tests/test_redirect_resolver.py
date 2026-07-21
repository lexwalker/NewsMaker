"""_resolve_same_site_redirect — the ixbt-class marked-source unwrapper.

jul-21 editor: «ссылка на первоисточник есть в самой статье» — ixbt wraps its
«Источник» link in api.ixbt.com/to/<encrypted>, a SAME-site redirect that
Tier 0.5 discarded, leaving self@low. One HEAD resolves the 302 to the real
target (observed: x.com/Tesla/status/…)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import batch_fetch_test as bf  # noqa: E402


class _FakeResp:
    def __init__(self, location):
        self.headers = {"location": location} if location else {}


class _FakeClient:
    def __init__(self, location):
        self._loc = location
        self.calls = []

    def head(self, url, **kw):
        self.calls.append(url)
        return _FakeResp(self._loc)


ART = "https://www.ixbt.com/news/2026/07/21/starlink-tesla-cybercab.html"


def test_same_site_redirect_is_resolved() -> None:
    c = _FakeClient("https://x.com/Tesla/status/2079325767260139962")
    out = bf._resolve_same_site_redirect(c, "https://api.ixbt.com/to/abc123", ART)
    assert out == "https://x.com/Tesla/status/2079325767260139962"
    assert c.calls  # the HEAD actually happened


def test_external_hint_costs_nothing() -> None:
    c = _FakeClient("https://elsewhere.com/x")
    ext = "https://www.gazeta.ru/business/news/2026/07/14/28893349.shtml"
    assert bf._resolve_same_site_redirect(c, ext, ART) == ext
    assert not c.calls  # no HEAD for an already-external hint


def test_same_site_non_redirect_path_untouched() -> None:
    c = _FakeClient("https://x.com/whatever")
    hint = "https://www.ixbt.com/news/2026/07/20/other-article.html"
    assert bf._resolve_same_site_redirect(c, hint, ART) == hint
    assert not c.calls  # path has no redirect marker


def test_redirect_back_to_same_site_is_ignored() -> None:
    c = _FakeClient("https://www.ixbt.com/news/2026/07/21/self-loop.html")
    hint = "https://api.ixbt.com/to/abc123"
    assert bf._resolve_same_site_redirect(c, hint, ART) == hint


def test_failure_degrades_to_input() -> None:
    class _Boom:
        def head(self, url, **kw):
            raise OSError("network down")
    hint = "https://api.ixbt.com/to/abc123"
    assert bf._resolve_same_site_redirect(_Boom(), hint, ART) == hint
