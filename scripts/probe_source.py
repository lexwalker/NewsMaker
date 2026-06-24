"""Deterministic per-source coverage probe — one JSON line per URL.

Runs each URL through the SAME backends the prog uses, so the verdict reflects
real fetch behaviour (not WebFetch). For each URL it reports:

  plain    — the real make_client path (httpx + ssl/url quirks, _http_get routing)
  curl     — curl_cffi Chrome impersonation (TLS fingerprint bypass)
  sslfix   — httpx verify=False (detects cert-only SSL failures)
  links    — same-host vs same-registrable-domain article-link counts +
             the dominant cross-host targets + a few sample article hrefs
  cls/fix  — a heuristic classification + recommended lever (the agent refines)

Usage:  python scripts/probe_source.py URL [URL ...]
Output: JSONL on stdout (one object per URL); never raises (per-stage guarded).
"""

from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path
from urllib.parse import urldefrag, urljoin, urlparse

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src"))
from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

import httpx  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402

import batch_fetch_test as bf  # noqa: E402
from news_agent.adapters.fetchers.impersonate import ImpersonateFetcher  # noqa: E402

_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}


def _reg(host: str) -> str:
    p = host.split(".")
    return ".".join(p[-2:]) if len(p) >= 2 else host


def _link_analysis(url: str, html: str) -> dict:
    """Same-host vs same-registrable-domain article-link counts + cross-host map."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:  # noqa: BLE001
        return {"samehost": 0, "regdomain": 0, "cross": {}, "samples": []}
    host = urlparse(url).netloc
    rd = _reg(host)
    samehost = regdom = 0
    cross: dict[str, int] = {}
    samples: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("#", "mailto:", "javascript:")):
            continue
        absu = urldefrag(urljoin(url, href))[0]
        p = urlparse(absu)
        if not bf._looks_like_article(absu) or absu in seen:
            if p.netloc and p.netloc != host:
                cross[p.netloc] = cross.get(p.netloc, 0) + 1
            continue
        seen.add(absu)
        if p.netloc == host:
            samehost += 1
        elif _reg(p.netloc) == rd:
            regdom += 1
            if len(samples) < 5:
                samples.append(absu)
        else:
            cross[p.netloc] = cross.get(p.netloc, 0) + 1
    top_cross = dict(sorted(cross.items(), key=lambda kv: -kv[1])[:3])
    return {"samehost": samehost, "regdomain": regdom,
            "cross": top_cross, "samples": samples}


def _classify(plain: dict, curl: dict, sslfix: dict, la: dict) -> tuple[str, str, str]:
    """(classification, recommended_fix, confidence). Heuristic — agent refines."""
    ps, cs = plain.get("status"), curl.get("status")
    ok2 = lambda s: isinstance(s, int) and 200 <= s < 400  # noqa: E731
    # already yields same-host article links via some backend -> transient/ok
    if la["samehost"] > 0:
        if ok2(ps):
            return ("ok_or_transient", "none_or_monitor", "medium")
        return ("needs_impersonate", "add_impersonate", "high")
    # real sister-domain article links (e.g. drom.ru -> news.drom.ru) -> repoint.
    # Require regdomain>0: stray external links (social, partners) on an error
    # page must NOT masquerade as a cross-host article source.
    if la["regdomain"] > 0 and (ok2(cs) or ok2(ps)):
        return ("cross_host_links", "repoint_url", "medium")
    # reachable (200) but no article links + tiny body -> JS SPA shell
    big = max(curl.get("html_kb", 0), plain.get("html_kb", 0))
    if (ok2(cs) or ok2(ps)) and big < 15:
        return ("js_spa_shell", "playwright", "medium")
    if ok2(cs) or ok2(ps):
        return ("selector_or_empty", "investigate", "low")
    # ssl-only failure that verify=False fixes
    if ok2(sslfix.get("status")):
        return ("ssl_cert", "add_ssl_insecure", "high")
    # hard block
    if cs == 403 or ps == 403:
        return ("waf_hard_403", "playwright_or_proxy", "medium")
    if isinstance(ps, str) and ("404" in ps) or ps == 404 or cs == 404:
        return ("dead_404", "drop_or_fix_url", "high")
    return ("unreachable", "drop_or_proxy", "low")


def probe(url: str) -> dict:
    out: dict = {"url": url}
    # 1. real prog client (httpx + ssl/url quirks)
    plain: dict = {}
    try:
        client = bf.make_client()
        try:
            r = bf._http_get(client, url)
            html = r.text or ""
            plain = {"status": r.status_code, "html_kb": len(html) // 1000}
            la = _link_analysis(url, html)
        finally:
            if hasattr(client, "close"):
                client.close()
    except Exception as e:  # noqa: BLE001
        plain = {"status": f"{type(e).__name__}", "err": str(e)[:80]}
        la = {"samehost": 0, "regdomain": 0, "cross": {}, "samples": []}
    # 2. curl_cffi impersonation
    curl: dict = {}
    try:
        imp = ImpersonateFetcher()
        st, html = imp.fetch(url)
        curl = {"status": st, "html_kb": len(html) // 1000}
        cla = _link_analysis(url, html)
        # prefer the richer link analysis (curl usually fuller than blocked httpx)
        if cla["samehost"] + cla["regdomain"] >= la["samehost"] + la["regdomain"]:
            la = cla
    except Exception as e:  # noqa: BLE001
        curl = {"status": f"{type(e).__name__}", "err": str(e)[:80]}
    # 3. ssl verify=False (only meaningful if plain looked ssl-ish)
    sslfix: dict = {}
    if "ssl" in str(plain.get("status", "")).lower() or "ssl" in str(plain.get("err", "")).lower():
        try:
            r = httpx.get(url, verify=False, timeout=15, follow_redirects=True, headers=_UA)
            sslfix = {"status": r.status_code, "html_kb": len(r.text) // 1000}
        except Exception as e:  # noqa: BLE001
            sslfix = {"status": f"{type(e).__name__}"}
    cls, fix, conf = _classify(plain, curl, sslfix, la)
    out.update({"plain": plain, "curl": curl, "sslfix": sslfix, "links": la,
                "cls": cls, "fix": fix, "conf": conf})
    return out


def main() -> int:
    args = sys.argv[1:]
    if args and args[0] == "--slice":  # --slice A B -> probe data/dead_sources.json[A:B]
        a, b = int(args[1]), int(args[2])
        urls = json.loads((ROOT / "data" / "dead_sources.json").read_text(encoding="utf-8"))[a:b]
    else:
        urls = args
    for url in urls:
        try:
            print(json.dumps(probe(url), ensure_ascii=False))
        except Exception as e:  # noqa: BLE001
            print(json.dumps({"url": url, "cls": "probe_error", "err": str(e)[:120]},
                             ensure_ascii=False))
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
