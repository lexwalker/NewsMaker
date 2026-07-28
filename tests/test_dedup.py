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


def test_dedup_brand_canonicalisation_kgm_ssangyong(tmp_path) -> None:
    """v41 regression: two articles about the SAME car keyed under
    different brand aliases (KGM vs SsangYong) must collapse to one
    bucket. Pre-fix the cross-run advisory couldn't fire on this."""
    import json
    from news_agent.adapters.storage import DedupStore

    store = DedupStore(tmp_path / "kgm.sqlite")
    blob_a = json.dumps({"verdict": "Точно новость",
                         "launch_brand_model": "KGM Torres"})
    blob_b = json.dumps({"verdict": "Точно новость",
                         "launch_brand_model": "SsangYong Torres"})
    store.mark_many_with_cache([
        ("h1", "https://a.example/1", "KGM Torres", None,
         "a.example", "RU", blob_a),
        ("h2", "https://b.example/2", "SsangYong Torres", None,
         "b.example", "RU", blob_b),
    ])
    out = store.recent_brand_models("RU", days=30)
    # Both rows must collapse to ONE canonical "kgm torres" key
    assert "kgm torres" in out
    assert "ssangyong torres" not in out
    assert len(out) == 1


def test_dedup_store_lede_column_persistence(tmp_path) -> None:
    """Lede text is persisted on the 8-tuple write signature and
    survives round-trip. Pre-fix history rows lacked lede entirely
    (cross-run dedup PoC discovered 0/5798 had it)."""
    from news_agent.adapters.storage import DedupStore

    store = DedupStore(tmp_path / "lede.sqlite")
    store.mark_many_with_cache([
        ("h1", "https://a.example/1", "KGM Torres launched in Russia",
         None, "a.example", "RU", None,
         "Кроссовер KGM Torres вышел на российский рынок..."),
    ])
    out = store.recent_for_brand("RU", "KGM", days=30)
    assert len(out) == 1
    assert out[0]["lede"].startswith("Кроссовер KGM Torres")
    assert out[0]["url"] == "https://a.example/1"


def test_dedup_store_lede_column_backward_compat(tmp_path) -> None:
    """7-tuple writes (pre-lede signature) still work, lede stays empty."""
    from news_agent.adapters.storage import DedupStore

    store = DedupStore(tmp_path / "compat.sqlite")
    store.mark_many_with_cache([
        ("h1", "https://a.example/1", "Old title", None,
         "a.example", "RU", '{"event_brand": "Audi"}'),
    ])
    out = store.recent_for_brand("RU", "Audi", days=30)
    assert len(out) == 1
    assert out[0]["lede"] == ""  # no lede stored → empty, not error


def test_dedup_recent_for_brand_canonicalises(tmp_path) -> None:
    """Querying for 'SsangYong' returns rows stored as 'KGM Torres'."""
    import json as _json
    from news_agent.adapters.storage import DedupStore

    store = DedupStore(tmp_path / "kgm2.sqlite")
    blob = _json.dumps({"event_brand": "KGM", "event_model": "Torres"})
    store.mark_many_with_cache([
        ("h1", "https://a.example/1", "KGM Torres", None,
         "a.example", "RU", blob,
         "KGM Torres updated for 2027 model year"),
    ])
    # Querying via the historical brand name must still hit
    out_via_old_name = store.recent_for_brand("RU", "SsangYong", days=30)
    out_via_new_name = store.recent_for_brand("RU", "KGM", days=30)
    assert len(out_via_old_name) == 1
    assert len(out_via_new_name) == 1
    assert out_via_old_name[0]["url"] == out_via_new_name[0]["url"]


def test_dedup_event_keys_canonical_brand(tmp_path) -> None:
    """Event-key match canonicalises the brand half — Mercedes-AMG and
    bare AMG articles about the SAME event collapse to one event_key."""
    import json
    from news_agent.adapters.storage import DedupStore

    store = DedupStore(tmp_path / "amg.sqlite")
    blob_a = json.dumps({
        "verdict": "Точно новость",
        "event_brand": "mercedes-amg", "event_model": "gt 4-door",
        "event_type": "reveal",
    })
    blob_b = json.dumps({
        "verdict": "Точно новость",
        "event_brand": "AMG", "event_model": "gt 4-door",
        "event_type": "reveal",
    })
    store.mark_many_with_cache([
        ("h1", "https://a.example/1", "t1", None, "a.example",
         "RU", blob_a),
        ("h2", "https://b.example/2", "t2", None, "b.example",
         "RU", blob_b),
    ])
    out = store.recent_event_keys("RU", days=30)
    # Both rows collapse to ONE canonical key
    assert len(out) == 1
    key = list(out.keys())[0]
    assert key.startswith("mercedes-amg|")


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

# --- token-subset fallback (jul-14): multi-model reveals normalise the model
#     differently across write-ups ("go" vs "go and cyber" — the Goodwood MG
#     story ran 4 times). Same brand+type + token containment ⇒ same event.

def _recent_entry(display="MG GO! reveal"):
    return {"mg|go and cyber|reveal": ("2026-07-13T10:00:00+00:00",
                                       "https://a.example/1", display)}


def test_event_hint_model_token_subset_matches():
    h = recent_event_dup_hint("mg", "go", "reveal", _recent_entry(),
                              "https://b.example/2")
    assert h is not None and "возможно дубль" in h


def test_event_hint_superset_direction_matches():
    recent = {"mg|go|reveal": ("2026-07-13T10:00:00+00:00",
                               "https://a.example/1", "MG Go")}
    h = recent_event_dup_hint("mg", "go and cyber", "reveal", recent,
                              "https://b.example/2")
    assert h is not None


def test_event_hint_different_models_do_not_match():
    recent = {"tesla|model 3|launch": ("2026-07-13T10:00:00+00:00",
                                       "https://a.example/1", "Model 3")}
    assert recent_event_dup_hint("tesla", "model y", "launch", recent,
                                 "https://b.example/2") is None


def test_event_hint_token_prefix_is_not_subset():
    recent = {"byd|sealion|launch": ("2026-07-13T10:00:00+00:00",
                                     "https://a.example/1", "Sealion")}
    assert recent_event_dup_hint("byd", "seal", "launch", recent,
                                 "https://b.example/2") is None


def test_event_hint_subset_requires_same_brand_and_type():
    assert recent_event_dup_hint("zeekr", "go", "reveal", _recent_entry(),
                                 "https://b.example/2") is None
    assert recent_event_dup_hint("mg", "go", "launch", _recent_entry(),
                                 "https://b.example/2") is None

def test_event_hint_reveal_motorshow_compatible():
    # MG-Goodwood forensics: same debut typed reveal by one write-up and
    # motorshow by another must still match (with subset model fallback too).
    recent = {"mg|go!|reveal": ("2026-07-10T01:29:00+00:00",
                                "https://a.example/1", "MG Go! concept")}
    h = recent_event_dup_hint("mg", "go! and cyber", "motorshow", recent,
                              "https://b.example/2")
    assert h is not None


def test_event_hint_other_types_not_folded():
    recent = {"mg|go!|launch": ("2026-07-10T01:29:00+00:00",
                                "https://a.example/1", "MG Go! sales")}
    assert recent_event_dup_hint("mg", "go!", "reveal", recent,
                                 "https://b.example/2") is None



# --- jul-20 dup-wave: event-key canonicalisation + own-pushes tier -----------
# 15 editor dup flags in one batch; forensics found the SAME happening keyed
# differently on each side: brand aliases (avtovaz vs lada), model spacing
# ("07 l" vs "07l"), launch vs pricing for a sales-start-with-prices story.
# recent_event_keys already canonicalises the MAP side via brand_canonical —
# the fresh side arrived raw and never matched.

def test_event_hint_matches_across_brand_alias() -> None:
    from news_agent.core.dedup import recent_event_dup_hint
    recent = {"lada|iskra vesta|tech": ("2026-07-17T10:00:00+00:00", "https://a/1", "lada iskra vesta (tech)")}
    hint = recent_event_dup_hint("АвтоВАЗ", "iskra vesta", "tech", recent, "https://b/2")
    assert hint is not None


def test_event_hint_matches_across_model_spacing() -> None:
    from news_agent.core.dedup import recent_event_dup_hint
    recent = {"avatr|07l|launch": ("2026-07-18T10:00:00+00:00", "https://a/1", "avatr 07l (launch)")}
    hint = recent_event_dup_hint("avatr", "07 l", "launch", recent, "https://b/2")
    assert hint is not None


def test_event_hint_folds_launch_and_pricing() -> None:
    from news_agent.core.dedup import recent_event_dup_hint
    recent = {"avatr|07l|launch": ("2026-07-18T10:00:00+00:00", "https://a/1", "avatr 07l (launch)")}
    hint = recent_event_dup_hint("avatr", "07l", "pricing", recent, "https://b/2")
    assert hint is not None


def test_event_hint_still_separates_different_models() -> None:
    from news_agent.core.dedup import recent_event_dup_hint
    recent = {"byd|seal|launch": ("2026-07-18T10:00:00+00:00", "https://a/1", "byd seal (launch)")}
    assert recent_event_dup_hint("byd", "sealion", "launch", recent, "https://b/2") is None


def test_published_hint_brand_gate_matches_via_alias() -> None:
    """event_brand "avtovaz" must gate against a title that only says
    «Лада …» — the bare ``eb in pt`` gate missed every OTHER name of the
    same brand, muting the paraphrase tier. (Scripts are not the issue:
    normalise_title transliterates Cyrillic; ALIASES are.)"""
    from news_agent.core.dedup import published_dup_hint
    from news_agent.core.primary_source import normalise_title
    own = {normalise_title("Лада начала продажи Iskra")}
    pt = next(iter(own))
    # the raw event_brand occurs nowhere in the title — only the alias does
    assert "avtovaz" not in pt
    hint = published_dup_hint(
        "АвтоВАЗ расширил продажи Iskra в регионах",
        "avtovaz", "iskra", own, source_label="недавно уже отправляли в фид")
    assert hint is not None
    assert "отправляли в фид" in hint


# --- stale-hint gate (jul-27: editor «много отклоняем того, что нужно») ---


def test_event_hint_is_stale_reads_age_from_text() -> None:
    from news_agent.core.dedup import event_hint_is_stale
    assert event_hint_is_stale("(возможно дубль: «bmw x5 (reveal)» уже было ~12 дн. назад — проверьте)")
    assert event_hint_is_stale("(возможно дубль: «mazda 6 (facelift)» уже было ~30 дн. назад)")
    # within the trusted week — keeps diverting for free
    assert not event_hint_is_stale("(возможно дубль: «bmw ix3 (reveal)» уже было сегодня — проверьте)")
    assert not event_hint_is_stale("(возможно дубль: «voyah passion s» уже было ~7 дн. назад)")
    assert not event_hint_is_stale("(возможно дубль: «kia ev9» уже было ~3 дн. назад)")


def test_event_hint_is_stale_ignores_ageless_hints() -> None:
    # Archive-paraphrase / own-push tiers carry no age — calibrated separately.
    from news_agent.core.dedup import event_hint_is_stale
    assert not event_hint_is_stale("(возможно дубль: уже публиковали о «haval jolion max» — проверьте)")
    assert not event_hint_is_stale("(возможно дубль: недавно уже отправляли в фид — проверьте)")
    assert not event_hint_is_stale("")
    assert not event_hint_is_stale(None)  # type: ignore[arg-type]


def test_event_hint_threshold_is_configurable() -> None:
    from news_agent.core.dedup import event_hint_is_stale
    h = "(возможно дубль: «x» уже было ~10 дн. назад)"
    assert event_hint_is_stale(h, threshold_days=7)
    assert not event_hint_is_stale(h, threshold_days=14)


def test_archive_model_hint_recognised_as_weak() -> None:
    # jul-28: this tier has no notion of the event — 24% of its diverts were
    # a NEW happening for a model we had already covered.
    from news_agent.core.dedup import archive_model_hint_is_weak
    assert archive_model_hint_is_weak(
        "(возможно дубль: уже публиковали о «lamborghini urus» — проверьте)")
    assert archive_model_hint_is_weak(
        "(возможно дубль: уже публиковали о «bmw ix5 lwb x5 lwb» — проверьте)")


def test_strong_archive_title_hint_stays_automatic() -> None:
    # The ≥88 full-title branch fired 10 times with 1 error — keep diverting.
    from news_agent.core.dedup import archive_model_hint_is_weak
    assert not archive_model_hint_is_weak(
        "(возможно дубль: похожий заголовок уже публиковали — проверьте)")
    assert not archive_model_hint_is_weak(
        "(возможно дубль: «bmw ix3 (reveal)» уже было сегодня — проверьте)")
    assert not archive_model_hint_is_weak(
        "(возможно дубль: недавно уже отправляли в фид — проверьте)")
    assert not archive_model_hint_is_weak("")
