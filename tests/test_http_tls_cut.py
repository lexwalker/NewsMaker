"""A cut TLS handshake must be retried; a dead host must not be.

jul-29 incident: 20 sources died in one run with the identical
``ConnectError: [SSL: UNEXPECTED_EOF_WHILE_READING]`` — 18 of them had worked
in the run 7 hours earlier (GM, Cadillac, Chevrolet, Honda, Volvo, VW UK, BYD,
Stellantis, Autocar). httpx reports a handshake killed mid-flight with the same
exception TYPE as "host does not resolve", and _request treated the whole class
as permanent, so not one of them was retried.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import httpx  # noqa: E402
import pytest  # noqa: E402

from news_agent.adapters.fetchers.base import (  # noqa: E402
    RetryingHttpClient,
    _is_tls_cut,
)

_TLS_EOF = ("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation "
            "of protocol (_ssl.c:1010)")


def test_tls_eof_is_recognised() -> None:
    assert _is_tls_cut(httpx.ConnectError(_TLS_EOF))


def test_handshake_alerts_are_recognised() -> None:
    assert _is_tls_cut(httpx.ConnectError("[SSL] tlsv1 alert internal error"))
    assert _is_tls_cut(httpx.ConnectError("sslv3 alert handshake failure"))


def test_cert_verify_failure_is_not_a_cut() -> None:
    # Permanent, and already handled per-domain via ssl_insecure_domains —
    # retrying it would burn time and still fail.
    assert not _is_tls_cut(
        httpx.ConnectError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate "
                           "verify failed: unable to get local issuer"))


def test_dns_failure_is_not_a_cut() -> None:
    assert not _is_tls_cut(
        httpx.ConnectError("[Errno 11001] getaddrinfo failed"))


class _Stub:
    """Inner client that fails a set number of times, then answers 200."""

    def __init__(self, exc: Exception, fail_times: int) -> None:
        self.exc = exc
        self.left = fail_times
        self.calls = 0

    def request(self, method: str, url: str, **kwargs: object) -> httpx.Response:
        self.calls += 1
        if self.left > 0:
            self.left -= 1
            raise self.exc
        return httpx.Response(200, text="<html>ok</html>",
                              request=httpx.Request(method, url))


def _client(stub: _Stub) -> RetryingHttpClient:
    c = RetryingHttpClient(user_agent="test-agent", backoff_base=1.0)
    c._client = stub  # type: ignore[assignment]
    return c


def test_tls_cut_is_retried_and_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("news_agent.adapters.fetchers.base._TLS_CUT_PAUSE_S", 0)
    stub = _Stub(httpx.ConnectError(_TLS_EOF), fail_times=1)
    resp = _client(stub).get("https://pressroom.cadillac.com/")
    assert resp.status_code == 200
    assert stub.calls == 2  # the retry is what produced the 200


def test_tls_cut_still_raises_when_it_never_clears(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Verified live on siam.in and vwpress.co.uk: some hosts cut EVERY
    # handshake. The retry must give up, not loop.
    monkeypatch.setattr("news_agent.adapters.fetchers.base._TLS_CUT_PAUSE_S", 0)
    stub = _Stub(httpx.ConnectError(_TLS_EOF), fail_times=99)
    with pytest.raises(httpx.ConnectError):
        _client(stub).get("https://vwpress.co.uk/rss")
    assert stub.calls == 2  # max_attempts, not endless


def test_plain_connect_error_is_not_retried() -> None:
    stub = _Stub(httpx.ConnectError("[Errno 11001] getaddrinfo failed"),
                 fail_times=1)
    with pytest.raises(httpx.ConnectError):
        _client(stub).get("https://gone.example/")
    assert stub.calls == 1  # no time burned on a host that isn't there


def test_read_timeout_is_not_retried() -> None:
    stub = _Stub(httpx.ReadTimeout("timed out"), fail_times=1)
    with pytest.raises(httpx.ReadTimeout):
        _client(stub).get("https://slow.example/")
    assert stub.calls == 1
