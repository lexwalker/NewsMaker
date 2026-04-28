from datetime import datetime, timedelta, timezone

from news_agent.core.freshness import is_fresh, is_in_window


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


# --------------------------------------------------------- is_in_window tests

def test_window_published_inside_window_passes() -> None:
    now = datetime(2026, 4, 28, 13, 0, tzinfo=timezone.utc)
    since = now - timedelta(hours=4)
    published = now - timedelta(hours=2)
    assert is_in_window(published, since=since, now=now)


def test_window_published_before_since_rejected() -> None:
    now = datetime(2026, 4, 28, 13, 0, tzinfo=timezone.utc)
    since = now - timedelta(hours=4)
    published = now - timedelta(hours=8)
    assert not is_in_window(published, since=since, now=now)


def test_window_published_far_future_rejected() -> None:
    now = datetime(2026, 4, 28, 13, 0, tzinfo=timezone.utc)
    since = now - timedelta(hours=4)
    published = now + timedelta(hours=2)  # site clock badly skewed
    assert not is_in_window(published, since=since, now=now)


def test_window_small_future_skew_tolerated() -> None:
    now = datetime(2026, 4, 28, 13, 0, tzinfo=timezone.utc)
    since = now - timedelta(hours=4)
    # 2-minute clock drift — this happens with NTP-less servers
    published = now + timedelta(minutes=2)
    assert is_in_window(published, since=since, now=now)


def test_window_no_published_date_passes() -> None:
    now = datetime(2026, 4, 28, 13, 0, tzinfo=timezone.utc)
    since = now - timedelta(hours=4)
    assert is_in_window(None, since=since, now=now)


def test_window_naive_published_treated_as_utc() -> None:
    now = datetime(2026, 4, 28, 13, 0, tzinfo=timezone.utc)
    since = now - timedelta(hours=4)
    naive = (now - timedelta(hours=1)).replace(tzinfo=None)
    assert is_in_window(naive, since=since, now=now)
