from datetime import datetime, timedelta, timezone

from news_agent.core import fetch_checkpoint as ck

NOW = datetime(2026, 7, 15, 11, 36, tzinfo=timezone.utc)
SINCE = datetime(2026, 7, 14, 18, 44, tzinfo=timezone.utc)
PREV = datetime(2026, 7, 14, 20, 44, tzinfo=timezone.utc)
URLS = ["https://a.ru/", "https://b.com/", "https://c.org/", "https://d.net/"]


def _fp(urls=URLS, prev=PREV, cls="8c1ac9a4", hot=False):
    return ck.fingerprint(
        classifier_version=cls, previous_run_at=prev, urls=urls,
        max_lookback_hours=48, overlap_minutes=120, max_articles=5000, hot=hot)


def _begin(path, fp):
    return ck.begin(
        path, fingerprint=fp, run_ts="2026-07-15T11:36:33+00:00",
        report_tab="ТЕСТ v47", articles_tab="ТЕСТ статьи v47",
        since=SINCE, now=NOW, using_fallback=False, previous_run_at=PREV,
        total_sources=len(URLS))


def test_roundtrip_resume(tmp_path):
    p = ck.checkpoint_path(tmp_path / "state.json")
    fp = _fp()
    _begin(p, fp)
    ck.append_source(p, src_idx=1, source_result={"url": "https://a.ru/"},
                     rows=[{"article_url": "https://a.ru/1", "title": "A1"}])
    ck.append_source(p, src_idx=2, source_result={"url": "https://b.com/"},
                     rows=[{"article_url": "https://b.com/1", "title": "B1"},
                           {"article_url": "https://b.com/2", "title": "B2"}])
    loaded = ck.load(p, fp)
    assert loaded is not None
    assert loaded.max_done_idx == 2
    assert loaded.done_idx == {1, 2}
    assert len(loaded.rows) == 3
    assert len(loaded.results) == 2
    assert loaded.header.articles_tab == "ТЕСТ статьи v47"
    assert loaded.header.since_iso == SINCE.isoformat()
    assert loaded.header.now_iso == NOW.isoformat()


def test_fingerprint_stable_across_relaunch():
    # previous_run_at is the anchor: same across crash→resume (state didn't
    # advance), so the fingerprint must be identical even though `now` differs.
    assert _fp() == _fp()


def test_fingerprint_changes_on_source_list_edit():
    assert _fp() != _fp(urls=URLS + ["https://e.ru/"])


def test_fingerprint_changes_on_reorder():
    # We resume by index, so order is part of identity.
    assert _fp() != _fp(urls=list(reversed(URLS)))


def test_fingerprint_changes_on_classifier_bump():
    assert _fp() != _fp(cls="deadbeef")


def test_fingerprint_hot_vs_full_distinct():
    assert _fp(hot=False) != _fp(hot=True)


def test_load_none_on_fingerprint_mismatch(tmp_path):
    p = ck.checkpoint_path(tmp_path / "state.json")
    _begin(p, _fp())
    ck.append_source(p, src_idx=1, source_result={}, rows=[{"x": 1}])
    # A different source list → different fingerprint → discard.
    assert ck.load(p, _fp(urls=URLS[:2])) is None


def test_load_none_when_too_old(tmp_path):
    p = ck.checkpoint_path(tmp_path / "state.json")
    fp = _fp()
    h = _begin(p, fp)
    ck.append_source(p, src_idx=1, source_result={}, rows=[{"x": 1}])
    # Rewrite the header's saved_at to 13h ago; default max_age is 12h.
    import json
    lines = p.read_text(encoding="utf-8").splitlines()
    hrec = json.loads(lines[0])
    hrec["saved_at"] = (datetime.now(timezone.utc) - timedelta(hours=13)).isoformat()
    lines[0] = json.dumps(hrec, ensure_ascii=False)
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    assert ck.load(p, fp) is None
    # But a generous age budget still loads it.
    assert ck.load(p, fp, max_age_hours=48) is not None


def test_torn_final_line_is_skipped(tmp_path):
    p = ck.checkpoint_path(tmp_path / "state.json")
    fp = _fp()
    _begin(p, fp)
    ck.append_source(p, src_idx=1, source_result={}, rows=[{"x": 1}])
    ck.append_source(p, src_idx=2, source_result={}, rows=[{"x": 2}])
    # Simulate a crash mid-append of source 3: a partial, unterminated JSON line.
    with open(p, "a", encoding="utf-8") as f:
        f.write('{"kind": "src", "src_idx": 3, "rows": [{"x": 3}], "resul')
    loaded = ck.load(p, fp)
    assert loaded is not None
    assert loaded.max_done_idx == 2          # source 3 dropped → re-fetched
    assert loaded.done_idx == {1, 2}
    assert len(loaded.rows) == 2


def test_load_none_when_no_sources_done(tmp_path):
    p = ck.checkpoint_path(tmp_path / "state.json")
    fp = _fp()
    _begin(p, fp)  # header only, no source completed before the crash
    assert ck.load(p, fp) is None


def test_clear_removes_file(tmp_path):
    p = ck.checkpoint_path(tmp_path / "state.json")
    _begin(p, _fp())
    ck.append_source(p, src_idx=1, source_result={}, rows=[{"x": 1}])
    assert p.exists()
    ck.clear(p)
    assert not p.exists()


def test_checkpoint_path_distinct_for_hot():
    full = ck.checkpoint_path("data/state.json")
    hot = ck.checkpoint_path("data/state_hot.json")
    assert full != hot
    assert full.name == "state_fetch_ckpt.jsonl"
    assert hot.name == "state_hot_fetch_ckpt.jsonl"


def test_begin_overwrites_prior_file(tmp_path):
    p = ck.checkpoint_path(tmp_path / "state.json")
    fp = _fp()
    _begin(p, fp)
    ck.append_source(p, src_idx=1, source_result={}, rows=[{"x": 1}])
    # A fresh begin() (new run, same params) must reset — no stale source rows.
    _begin(p, fp)
    assert ck.load(p, fp) is None  # header only again
