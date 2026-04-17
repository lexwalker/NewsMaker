from datetime import datetime, timedelta, timezone

from news_agent.core.freshness import is_fresh


def test_fresh_article_passes() -> None:
    now = datetime.now(timezone.utc)
    published = now - timedelta(hours=5)
    assert is_fresh(published, hours=48, now=now)


def test_stale_article_rejected() -> None:
    now = datetime.now(timezone.utc)
    published = now - timedelta(hours=72)
    assert not is_fresh(published, hours=48, now=now)


def test_missing_time_is_considered_fresh() -> None:
    assert is_fresh(None, hours=48)


def test_naive_datetime_treated_as_utc() -> None:
    now = datetime.now(timezone.utc)
    naive = (now - timedelta(hours=1)).replace(tzinfo=None)
    assert is_fresh(naive, hours=48, now=now)
