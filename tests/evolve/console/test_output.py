import json
from pathlib import Path

import pytest
from lamindb_setup.evolve.console import _querylog as querylog
from lamindb_setup.evolve.console._output import (
    OutputFormat,
    join_sections,
    write_and_echo,
)


def test_query_log_counts_queries_once_and_resets(instance, monkeypatch, capsys):
    from django.db import connections
    from django.db.backends import signals
    from django.dispatch import Signal

    # Isolate the signal and wrappers that normally live for a CLI process.
    monkeypatch.setattr(signals, "connection_created", Signal())
    monkeypatch.setattr(querylog, "_signal_connected", False)
    for db in connections.all():
        monkeypatch.setattr(db, "execute_wrappers", db.execute_wrappers.copy())
    querylog.configure_query_logging(True)
    try:
        querylog.install_query_logger()
        # Ignore a signal without a connection, and keep installation idempotent.
        signals.connection_created.send(sender=None)
        querylog.install_query_logger()
        with connections["default"].cursor() as cursor:
            cursor.execute("SELECT 42 /*" + "x" * 310 + "*/")
            assert cursor.fetchone() == (42,)
        querylog.echo_query_log_summary()
        logged = capsys.readouterr().err
        assert "SELECT 42" in logged and "…" in logged
        assert "1 statement(s)" in logged
        assert querylog.query_log_totals()[0] == 1
    finally:
        querylog.configure_query_logging(False)
    with connections["default"].cursor() as cursor:
        cursor.execute("SELECT 43")
    querylog.echo_query_log_summary()
    assert querylog.query_log_totals() == (0, 0)
    assert "SELECT 43" not in capsys.readouterr().err


def test_report_sections_echo_the_same_document_written_to_disk(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pieces = ['{"one": 1}', '{"two": 2}']
    assert join_sections(pieces, OutputFormat.markdown) == "\n\n".join(pieces)
    assert join_sections(pieces[:1], OutputFormat.json) == pieces[0]
    combined = join_sections(pieces, OutputFormat.json)
    output = tmp_path / "reports" / "apply.json"
    write_and_echo(combined, output)
    assert json.loads(capsys.readouterr().out) == [{"one": 1}, {"two": 2}]
    assert output.read_text() == combined
