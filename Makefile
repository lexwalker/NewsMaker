.PHONY: install lint format test run check compare sheets-check

install:
	python -m pip install -U pip
	python -m pip install -e ".[dev]"

lint:
	ruff check src tests scripts
	black --check src tests scripts
	mypy src

format:
	ruff check --fix src tests scripts
	black src tests scripts

test:
	pytest

check: lint test

run:
	python -m news_agent run --portal RU

sheets-check:
	python scripts/check_sheets.py

compare:
	python scripts/compare_llms.py
