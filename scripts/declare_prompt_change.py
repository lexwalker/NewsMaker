"""Declare the DIRECTION of a constitution edit for the next run.

Why: a prompt edit invalidates cached LLM verdicts, and only the person
who made the edit knows what it can do. A pure new-REJECT rule cannot
rescue anything (cached rejects stay valid); a pure rescue rule cannot
newly reject anything (cached accepts stay valid). Declaring it saves a
chunk of the post-edit re-classification — but the SCHEDULED run has no
env vars, so the declaration goes into data/prompt_change.json and the
next run picks it up, uses it once, and deletes it.

Stamped with the CURRENT prompt_ver: if the constitution is edited again
before the next run, the declaration no longer matches and is ignored
(safe default 'both'), so a stale file can never silently keep stale
verdicts alive.

Usage:
    python scripts/declare_prompt_change.py reject_only
    python scripts/declare_prompt_change.py publish_only
    python scripts/declare_prompt_change.py --clear
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from news_agent.adapters.llm.base import EDITORIAL_REVIEW_SYSTEM  # noqa: E402
from news_agent.core import heuristic_relevance as _heur  # noqa: E402
from news_agent.core.cache_version import (  # noqa: E402
    PROMPT_CHANGE_PUBLISH_ONLY,
    PROMPT_CHANGE_REJECT_ONLY,
    compute_split_versions,
)

PATH = ROOT / "data" / "prompt_change.json"
VALID = (PROMPT_CHANGE_REJECT_ONLY, PROMPT_CHANGE_PUBLISH_ONLY)


def main() -> int:
    args = [a for a in sys.argv[1:] if a.strip()]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    if args[0] == "--clear":
        PATH.unlink(missing_ok=True)
        print(f"declaration cleared ({PATH.name}) — next run uses 'both'.")
        return 0
    direction = args[0].strip()
    if direction not in VALID:
        print(f"! unknown direction {direction!r}; expected one of {VALID}")
        return 2
    try:
        heur_src = Path(_heur.__file__).read_bytes()
    except Exception:  # noqa: BLE001
        heur_src = b""
    prompt_ver, _ = compute_split_versions(EDITORIAL_REVIEW_SYSTEM, heur_src)
    PATH.write_text(json.dumps({
        "direction": direction,
        "prompt_ver": prompt_ver,
        "note": "consumed by the next healthy prog run; edit the prompt again "
                "and this is ignored (prompt_ver mismatch).",
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"declared {direction} for prompt_ver={prompt_ver} → {PATH}")
    print("The next prog (scheduled or manual) will use it once, then delete it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
