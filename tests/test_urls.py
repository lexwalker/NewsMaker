from news_agent.core.urls import canonicalise, domain_of, url_hash


def test_canonicalise_strips_utm_and_fragment() -> None:
    raw = "HTTPS://Example.COM/path/?utm_source=x&id=1#frag"
    assert canonicalise(raw) == "https://example.com/path/?id=1"


def test_canonicalise_keeps_empty_path() -> None:
    assert canonicalise("https://Example.com") == "https://example.com/"


def test_url_hash_stable_across_tracking() -> None:
    a = url_hash("https://example.com/a?utm_source=fb")
    b = url_hash("https://Example.com/a")
    assert a == b


def test_domain_of_strips_www() -> None:
    assert domain_of("https://www.example.com/x") == "example.com"
