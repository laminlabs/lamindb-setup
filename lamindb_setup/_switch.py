from __future__ import annotations

from typing import TYPE_CHECKING

from lamindb_setup.core._settings import settings

from ._logger import logger

if TYPE_CHECKING:
    from lamindb.models import Branch


def switch(target: str | Branch, *, space: bool = False, create: bool = False):
    """Switch to a branch or space, create if not exists.

    Args:
        target: Branch target or space target to switch to.
        space: If True, switch space; otherwise switch branch.
        create: If True and switching branch, create the branch if it does not exist.
    """
    target_name = target if isinstance(target, str) else target.name

    if space:
        settings.space = target
    else:
        resolved_target: str | Branch = target
        if not create and isinstance(target, str):
            from lamindb import Branch, Q
            from lamindb.errors import DoesNotExist

            existing = Branch.filter(Q(name=target) | Q(uid=target)).one_or_none()
            if existing is None:
                raise DoesNotExist(
                    f"Branch '{target}' does not exist. "
                    f"To create and switch, run: lamin switch -c {target}"
                )
            resolved_target = existing
            target_name = existing.name
        if create:
            from lamindb import Branch, Q
            from lamindb.errors import BranchAlreadyExists

            # Consistent with git switch -c: error if branch already exists.
            existing = Branch.filter(
                Q(name=target_name) | Q(uid=target_name)
            ).one_or_none()
            if existing is not None:
                raise BranchAlreadyExists(
                    f"Branch '{target_name}' already exists. Omit -c/--create to switch to it."
                )
            Branch(name=target_name).save()
            logger.important(f"created branch: {target_name}")
        settings.branch = resolved_target
    logger.important(f"switched to {target_name}")
