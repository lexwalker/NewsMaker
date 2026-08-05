"""The event-key clock must not be rewound by its own echoes (fix 7, aug-05).

recent_event_keys keeps ONE record per brand|model|type — the latest
sighting. Before the filter, a row that was itself hint-diverted still
refreshed last_seen_at, so a hot key stayed forever inside the 7-day trust
window while anyone kept writing about the model: new story → fresh hint →
diverted → its own row re-warms the key → repeat. Same disease as 7.1, in a
different store; the half-fix there never covered this one.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.adapters.storage import DedupStore  # noqa: E402


def _store(tmp_path, rows):
    """rows: list of (age_days, blob_dict)"""
    db = DedupStore(tmp_path / "t.sqlite")
    now = datetime.now(timezone.utc)
    with db._conn() as c:
        for i, (age, blob) in enumerate(rows):
            ts = (now - timedelta(days=age)).isoformat()
            c.execute(
                "INSERT INTO seen_articles (url_hash, canonical_url, title, "
                "first_seen_at, last_seen_at, source_domain, portal, "
                "cached_row_json) VALUES (?,?,?,?,?,?,?,?)",
                (f"h{i}", f"https://e.example/{i}", f"t{i}", ts, ts,
                 "e.example", "RU", json.dumps(blob, ensure_ascii=False)))
    return db


def _blob(reason="", dup_hint=None, verdict="Точно новость"):
    b = {"verdict": verdict, "llm_reason": reason,
         "event_brand": "Geely", "event_model": "monjaro", "event_type": "launch"}
    if dup_hint is not None:
        b["dup_hint"] = dup_hint
    return b


def test_clean_sighting_creates_the_key(tmp_path):
    db = _store(tmp_path, [(2, _blob())])
    keys = db.recent_event_keys("RU", days=30)
    assert "geely|monjaro|launch" in keys


def test_echo_with_legacy_baked_hint_does_not_rejuvenate(tmp_path):
    # Original clean sighting 10 days ago; a hint-carrying echo yesterday.
    # The key must report the ORIGINAL's age, not the echo's.
    db = _store(tmp_path, [
        (10, _blob()),
        (1, _blob(reason="(возможно дубль: «geely monjaro (launch)» уже "
                         "было ~9 дн. назад — проверьте) | Запуск продаж")),
    ])
    keys = db.recent_event_keys("RU", days=30)
    last_seen = keys["geely|monjaro|launch"][0]
    age = datetime.now(timezone.utc) - datetime.fromisoformat(last_seen)
    assert age >= timedelta(days=9)


def test_echo_with_dup_hint_field_does_not_rejuvenate(tmp_path):
    # Clean-persist format (aug-05): the hint lives in its own field and the
    # reason is clean — the filter must read the field too.
    db = _store(tmp_path, [
        (10, _blob()),
        (1, _blob(reason="Запуск продаж конкретной модели",
                  dup_hint="(возможно дубль: «geely monjaro (launch)» уже "
                           "было ~9 дн. назад — проверьте)")),
    ])
    keys = db.recent_event_keys("RU", days=30)
    last_seen = keys["geely|monjaro|launch"][0]
    age = datetime.now(timezone.utc) - datetime.fromisoformat(last_seen)
    assert age >= timedelta(days=9)


def test_all_sightings_hinted_means_no_key(tmp_path):
    # Only echoes inside the window (the clean original aged out): the event
    # is a month old, a new story about the model is likely a NEW happening.
    db = _store(tmp_path, [
        (3, _blob(dup_hint="(возможно дубль: … — проверьте)", reason="чисто")),
    ])
    assert db.recent_event_keys("RU", days=30) == {}


def test_rejected_rows_still_feed_the_key(tmp_path):
    # Deliberate long-standing behavior (see retry_failed_llm docstring): a
    # reject entry's value IS its event key. Only hint-carriers are echoes.
    db = _store(tmp_path, [(2, _blob(verdict="Отклонено LLM"))])
    assert "geely|monjaro|launch" in db.recent_event_keys("RU", days=30)
