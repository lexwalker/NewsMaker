from datetime import datetime, timezone

from news_agent.core.dedup import recent_model_dup_hint, title_is_duplicate


def test_exact_match_is_duplicate() -> None:
    assert title_is_duplicate("Toyota unveils 2026 Camry", ["Toyota unveils 2026 Camry"], threshold=0.85)


def test_near_match_is_duplicate() -> None:
    assert title_is_duplicate(
        "Toyota unveils 2026 Camry",
        ["Toyota Unveils the 2026 Camry Sedan"],
        threshold=0.80,
    )


def test_unrelated_title_not_duplicate() -> None:
    assert not title_is_duplicate(
        "BYD overtakes Tesla in global EV sales",
        ["Toyota unveils 2026 Camry"],
        threshold=0.85,
    )


def test_empty_inputs_safe() -> None:
    assert not title_is_duplicate("", [], threshold=0.85)
    assert not title_is_duplicate("anything", [], threshold=0.85)


# ----------- Plan P3-D: advisory recent-model dup hint --------------------

_NOW = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)


def test_recent_model_hint_fires_for_recent_model() -> None:
    recent = {
        "geely emgrand": ("2026-05-10T09:00:00+00:00",
                          "https://other.example/emgrand-old"),
    }
    hint = recent_model_dup_hint(
        "Geely Emgrand", recent,
        "https://new.example/emgrand-new", now=_NOW,
    )
    assert hint is not None
    assert "Geely Emgrand" in hint
    assert "4 дн" in hint  # 14 May - 10 May


def test_recent_model_hint_none_when_not_seen() -> None:
    assert recent_model_dup_hint(
        "Toyota Camry",
        {"geely emgrand": ("2026-05-10T09:00:00+00:00", "u")},
        "https://x.example/y", now=_NOW,
    ) is None


def test_recent_model_hint_none_for_same_url_rerun() -> None:
    """Same URL = re-run of identical article, not a dup signal."""
    same = "https://same.example/article-1"
    recent = {"lada azimut": ("2026-05-13T09:00:00+00:00", same)}
    assert recent_model_dup_hint(
        "Lada Azimut", recent, same, now=_NOW,
    ) is None


def test_recent_model_hint_empty_brand_model_safe() -> None:
    assert recent_model_dup_hint("", {"x": ("t", "u")},
                                 "url", now=_NOW) is None
    assert recent_model_dup_hint("   ", {"x": ("t", "u")},
                                 "url", now=_NOW) is None


def test_recent_model_hint_today() -> None:
    recent = {"haval h9": ("2026-05-14T06:00:00+00:00",
                           "https://a.example/h9")}
    hint = recent_model_dup_hint(
        "Haval H9", recent, "https://b.example/h9", now=_NOW,
    )
    assert hint is not None and "сегодня" in hint


def test_recent_model_hint_bad_timestamp_degrades() -> None:
    """Unparseable last_seen → 'недавно', never raises."""
    recent = {"byd seal": ("not-a-date",
                           "https://a.example/seal")}
    hint = recent_model_dup_hint(
        "BYD Seal", recent, "https://b.example/seal", now=_NOW,
    )
    assert hint is not None and "недавно" in hint


def test_dedup_store_recent_brand_models(tmp_path) -> None:
    """DedupStore.recent_brand_models reads only within window + parses
    launch_brand_model from cached JSON. Pre-P3-D rows (no bm) skipped."""
    import json

    from news_agent.adapters.storage import DedupStore

    store = DedupStore(tmp_path / "t.sqlite")
    fresh_json = json.dumps({"verdict": "Точно новость",
                             "launch_brand_model": "Geely Emgrand"})
    no_bm_json = json.dumps({"verdict": "Точно новость"})
    store.mark_many_with_cache([
        ("h1", "https://a.example/1", "t1", None, "a.example",
         "RU", fresh_json),
        ("h2", "https://a.example/2", "t2", None, "a.example",
         "RU", no_bm_json),
    ])
    out = store.recent_brand_models("RU", days=30)
    assert "geely emgrand" in out
    assert out["geely emgrand"][1] == "https://a.example/1"
    # h2 had no launch_brand_model → not present
    assert len(out) == 1
    # Different portal → nothing
    assert store.recent_brand_models("KZ", days=30) == {}


# ----------- Hybrid Stage 2a: semantic event-key dup hint -----------------

from news_agent.core.dedup import recent_event_dup_hint  # noqa: E402


def test_event_hint_fires_on_same_signature() -> None:
    recent = {
        "jaguar|type 01|spy_shot": (
            "2026-05-10T09:00:00+00:00",  # 4 days before _NOW (05-14)
            "https://old.example/jag-spy",
            "jaguar type 01 (spy_shot)",
        )
    }
    h = recent_event_dup_hint(
        "Jaguar", "Type 01", "spy_shot", recent,
        "https://new.example/jag-new", now=_NOW,
    )
    assert h is not None
    assert "jaguar type 01 (spy_shot)" in h
    assert "4 дн" in h  # _NOW 2026-05-14 − 2026-05-10 = ~4 дн. назад


def test_event_hint_none_when_type_generic() -> None:
    recent = {"x|y|other": ("2026-05-10T00:00:00+00:00", "u", "x y (other)")}
    assert recent_event_dup_hint("x", "y", "other", recent, "u2",
                                  now=_NOW) is None


def test_event_hint_none_when_model_empty() -> None:
    recent = {"geely||launch": ("2026-05-10T00:00:00+00:00", "u", "g")}
    assert recent_event_dup_hint("geely", "", "launch", recent, "u2",
                                  now=_NOW) is None


def test_event_hint_none_same_url_rerun() -> None:
    same = "https://same.example/a"
    recent = {"byd|seal|recall": ("2026-05-13T00:00:00+00:00", same,
                                  "byd seal (recall)")}
    assert recent_event_dup_hint("byd", "seal", "recall", recent, same,
                                 now=_NOW) is None


def test_event_hint_not_seen_returns_none() -> None:
    assert recent_event_dup_hint("toyota", "camry", "launch", {},
                                 "u", now=_NOW) is None


def test_event_hint_bad_timestamp_degrades() -> None:
    recent = {"vw|golf|facelift": ("not-a-date", "https://a.ex/x",
                                   "vw golf (facelift)")}
    h = recent_event_dup_hint("vw", "golf", "facelift", recent,
                              "https://b.ex/y", now=_NOW)
    assert h is not None and "недавно" in h


def test_dedup_store_recent_event_keys(tmp_path) -> None:
    import json

    from news_agent.adapters.storage import DedupStore

    store = DedupStore(tmp_path / "ev.sqlite")
    good = json.dumps({"verdict": "Точно новость", "event_brand": "jaguar",
                       "event_model": "type 01", "event_type": "spy_shot"})
    vague = json.dumps({"event_brand": "geely", "event_model": "",
                        "event_type": "launch"})
    other = json.dumps({"event_brand": "kia", "event_model": "ev9",
                        "event_type": "other"})
    pre = json.dumps({"verdict": "Точно новость"})  # pre-Stage-1
    store.mark_many_with_cache([
        ("h1", "https://a.ex/1", "t", None, "a.ex", "RU", good),
        ("h2", "https://a.ex/2", "t", None, "a.ex", "RU", vague),
        ("h3", "https://a.ex/3", "t", None, "a.ex", "RU", other),
        ("h4", "https://a.ex/4", "t", None, "a.ex", "RU", pre),
    ])
    out = store.recent_event_keys("RU", days=30)
    assert list(out.keys()) == ["jaguar|type 01|spy_shot"]
    assert out["jaguar|type 01|spy_shot"][2] == "jaguar type 01 (spy_shot)"
    assert store.recent_event_keys("KZ", days=30) == {}
