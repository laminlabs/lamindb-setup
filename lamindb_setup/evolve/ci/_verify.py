"""Verify the `lamin_schemas.yml` configuration against the committed plans.

Every committed `plans/plan_*.py` must either be referenced by at least one
instance in the configuration, or be listed in the global `ignored_plans`
glob list. A plan that is neither is an *orphan*: it would be applied nowhere
yet still ships in the repository, which is almost always a mistake. Ignored
plans, conversely, must *not* carry a lock file -- an ignored plan is never
applied, so a lock file means it was deployed despite being declared
unmanaged. :func:`verify_config` reports both as errors so a CI workflow can
fail and force a maintainer to make a deliberate choice.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..engine import sibling_lock_paths

if TYPE_CHECKING:
    from pathlib import Path

    from ._config import CIConfig

__all__ = ["VerifyResult", "verify_config"]


@dataclass
class VerifyResult:
    """The outcome of verifying the configuration against the plans directory."""

    orphans: list[Path] = field(default_factory=list)
    ignored: list[Path] = field(default_factory=list)
    ignored_with_locks: dict[Path, list[Path]] = field(default_factory=dict)
    referenced: list[Path] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Whether the configuration is valid (no orphans, no ignored-with-lock)."""
        return not self.orphans and not self.ignored_with_locks


def verify_config(config: CIConfig) -> VerifyResult:
    """Check that every committed plan is referenced or explicitly ignored.

    Discovers `plans/plan_*.py` next to the configuration file, partitions
    them into referenced, ignored and orphan, and flags ignored plans that have
    a lock file. The result is :attr:`~VerifyResult.ok` only when there are no
    orphans and no ignored plan carries a lock file.
    """
    base = config.config_path.parent
    referenced = {p.path.resolve() for inst in config.instances for p in inst.plans}
    ignored: set[Path] = set()
    for pattern in config.ignored_plans:
        ignored.update(p.resolve() for p in base.glob(pattern))

    result = VerifyResult()
    for plan_file in sorted((base / "plans").glob("plan_*.py")):
        resolved = plan_file.resolve()
        if resolved in referenced:
            result.referenced.append(plan_file)
        elif resolved in ignored:
            result.ignored.append(plan_file)
            locks = list(sibling_lock_paths(plan_file).values())
            if locks:
                result.ignored_with_locks[plan_file] = locks
        else:
            result.orphans.append(plan_file)
    return result
