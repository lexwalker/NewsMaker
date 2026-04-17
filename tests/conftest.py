"""Shared pytest fixtures."""

from __future__ import annotations

import os

os.environ.setdefault("SPREADSHEET_ID", "test-sheet")
os.environ.setdefault("LLM_PROVIDER", "anthropic")
