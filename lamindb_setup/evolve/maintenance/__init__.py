"""One-off repair commands for an instance that has drifted out of shape.

Each private module finds a class of problem and offers to fix it: duplicates,
dangling references, degenerate sheet schemas, stranded values, broken dtypes,
and uids that differ between instances.  They share one shape -- `compute_*`
finds, `execute_*` fixes, `print_*` and `render_*` report -- and the names
the command line needs are re-exported here.
"""

from __future__ import annotations

from ._align_uids import (
    collect_instance_locks,
    compute_alignment,
    execute_alignment,
    print_alignment,
    read_source_identity,
    render_alignment,
    rewrite_target_locks,
    with_prerequisites,
)
from ._dedup import (
    compute_content_duplicates,
    compute_duplicates,
    execute_dedup,
    print_dedup,
    render_dedup,
)
from ._prune import compute_prune, execute_prune, print_prune, render_prune
from ._sheet_schemas import (
    compute_sheet_schemas,
    execute_sheet_schemas,
    print_sheet_schemas,
    render_sheet_schemas,
)
from ._stranded_values import (
    compute_stranded_values,
    execute_stranded_values,
    print_stranded_values,
    render_stranded_values,
)
from ._verify_dtypes import (
    compute_verify_dtypes,
    execute_verify_dtypes,
    print_verify_dtypes,
    render_verify_dtypes,
)

__all__ = [
    "collect_instance_locks",
    "compute_alignment",
    "compute_content_duplicates",
    "compute_duplicates",
    "compute_prune",
    "compute_sheet_schemas",
    "compute_stranded_values",
    "compute_verify_dtypes",
    "execute_alignment",
    "execute_dedup",
    "execute_prune",
    "execute_sheet_schemas",
    "execute_stranded_values",
    "execute_verify_dtypes",
    "print_alignment",
    "print_dedup",
    "print_prune",
    "print_sheet_schemas",
    "print_stranded_values",
    "print_verify_dtypes",
    "read_source_identity",
    "render_alignment",
    "render_dedup",
    "render_prune",
    "render_sheet_schemas",
    "render_stranded_values",
    "render_verify_dtypes",
    "rewrite_target_locks",
    "with_prerequisites",
]
