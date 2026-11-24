"""Smoke test to keep CI green during scaffolding (Phase 1)."""

import src


def test_package_importable() -> None:
    """The top-level package should import without side effects."""
    assert src.__doc__ is not None
