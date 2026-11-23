# Source test semantics retained.
# ruff: noqa: TC002
"""Tests for lamindb_setup.evolve.console._progress."""

from __future__ import annotations

import pytest
from lamindb_setup.evolve.console._progress import progress_reporter, report_stage


def test_track_yields_every_item_in_order() -> None:
    with progress_reporter() as progress:
        result = list(progress.track([1, 2, 3], "items"))
    assert result == [1, 2, 3]


def test_track_handles_empty_sequence() -> None:
    with progress_reporter() as progress:
        result = list(progress.track([], "nothing"))
    assert result == []


def test_report_does_not_raise() -> None:
    with progress_reporter() as progress:
        progress.report("a milestone message")


def test_nested_reporter_joins_the_active_one() -> None:
    # rich allows one live display per console and every display here shares
    # one, so nesting must reuse the running display rather than raise.
    with progress_reporter() as outer:
        with progress_reporter() as inner:
            assert inner is outer
            assert list(inner.track([1, 2], "nested")) == [1, 2]


def test_reporter_is_released_after_the_display_closes() -> None:
    with progress_reporter() as first:
        pass
    with progress_reporter() as second:
        assert second is not first


def test_report_stage_writes_to_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    report_stage("a stage")

    captured = capsys.readouterr()
    assert "a stage" in captured.err
    assert captured.out == ""
