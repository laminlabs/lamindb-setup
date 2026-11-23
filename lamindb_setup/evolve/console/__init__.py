"""Where this package writes: the stderr console, progress, query logging.

Leaf plumbing shared by every command, and a single front door for it.  The
modules underneath are implementation detail -- import from
`lamindb_setup.evolve.console` rather than reaching into them:

- :func:`stderr_console` is the one console this package writes human-facing
  output to, and :func:`echo` prints a plain line.
- :class:`OutputFormat` and friends decide how a command renders its summary
  and where that document is written.
- :func:`progress_reporter` and :func:`report_stage` drive the shared progress
  display.
- the query-log functions log every SQL statement a run issues.

The console loads `rich` on first use.
"""

from __future__ import annotations

from ._console import echo, stderr_console
from ._output import OutputFormat, join_sections, write_and_echo
from ._progress import ProgressReporter, progress_reporter, report_stage
from ._querylog import (
    configure_query_logging,
    echo_query_log_summary,
    install_query_logger,
    query_log_totals,
)

__all__ = [
    "OutputFormat",
    "ProgressReporter",
    "configure_query_logging",
    "echo",
    "echo_query_log_summary",
    "install_query_logger",
    "join_sections",
    "progress_reporter",
    "query_log_totals",
    "report_stage",
    "stderr_console",
    "write_and_echo",
]
