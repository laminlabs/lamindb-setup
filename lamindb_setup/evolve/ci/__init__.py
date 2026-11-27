"""Continuous-integration helpers for managing schema plans in a separate repo.

The :data:`ci_app` Typer sub-application is mounted on the main CLI as
`lamin evolve ci` and exposes:

- `plan`: validate the configured plans and diff them against their instances
  (or, with `--preview-migrations`, rehearse them in a rolled-back
  transaction), emitting Markdown for a workflow to post on a pull request.
- `apply`: apply the configured plans to their instances (optionally filtered
  to the instances that have a given changed plan file).
- `drift`: per instance, write a plan of objects no configured plan tracks and
  report drift in the tracked objects, for scheduled drift detection.
- `missing-exports`: per instance, report the tracked objects plans must
  export so every untracked object can reference them.
- `verify-config`: check every committed plan is referenced by an instance or
  explicitly ignored, and that ignored plans carry no lock file.

All commands read a `lamin_schemas.yml` configuration (see :mod:`._config`).
`plan`, `apply`, `drift` and `missing-exports` can be restricted to some
of its instances (see :meth:`CIConfig.select`).
"""

from __future__ import annotations

from ._apply import CIApplyError, run_apply
from ._config import CIConfig, ConfigError, InstanceConfig, PlanRef, load_config
from ._drift import DriftResult, InstanceDriftResult, run_drift_detection
from ._markdown import (
    render_drift_report,
    render_missing_exports_report,
    render_report,
    render_verify_report,
)
from ._missing_exports import (
    BlockedObject,
    InstanceMissingExports,
    MissingExport,
    MissingExportsResult,
    PlanExportGroup,
    run_missing_exports,
)
from ._pr import PRReport, run_pr_check
from ._verify import VerifyResult, verify_config

__all__ = [
    "BlockedObject",
    "CIApplyError",
    "CIConfig",
    "ConfigError",
    "DriftResult",
    "InstanceConfig",
    "InstanceDriftResult",
    "InstanceMissingExports",
    "MissingExport",
    "MissingExportsResult",
    "PRReport",
    "PlanExportGroup",
    "PlanRef",
    "VerifyResult",
    "load_config",
    "render_drift_report",
    "render_missing_exports_report",
    "render_report",
    "render_verify_report",
    "run_apply",
    "run_drift_detection",
    "run_missing_exports",
    "run_pr_check",
    "verify_config",
]
