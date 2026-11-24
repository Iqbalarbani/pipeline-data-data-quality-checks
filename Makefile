.PHONY: help install lint test check ci clean

help:
	@echo "Targets:"
	@echo "  make install  Install dependencies (uv, falls back to pip)"
	@echo "  make lint     Run ruff"
	@echo "  make test     Run pytest"
	@echo "  make check    Lint + test"
	@echo "  make ci       What CI runs"
	@echo "  make clean    Remove caches"

install:
	@if command -v uv >/dev/null 2>&1; then \
		uv sync --all-groups; \
	else \
		python -m pip install -e . && python -m pip install pytest ruff; \
	fi

lint:
	ruff check .
	ruff format --check .

test:
	pytest

check: lint test

ci: check

clean:
	rm -rf .pytest_cache .ruff_cache **/__pycache__ .venv 2>/dev/null || true
