from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

from lamin_utils import logger

from lamindb_setup.core._settings import settings

if TYPE_CHECKING:
    from lamindb.models import Branch


def _navigation_command(instruction: str) -> str:
    prefix = "To switch, run: "
    return instruction[len(prefix) :] if instruction.startswith(prefix) else instruction


def missing_branch_create_and_navigate_message(
    target_name: str, instruction: str
) -> str:
    navigation_command = _navigation_command(instruction)
    return (
        f"Branch '{target_name}' does not exist.\n"
        "To create it and switch in worktree mode, run:\n"
        f"lamin create branch {target_name} && "
        f"{navigation_command} && "
        f"lamin switch {target_name}"
    )


def worktree_switch_instruction(
    target_name: str, *, create: bool = False
) -> str | None:
    """Return a navigation hint when switching from wrong worktree directory."""
    if "/" in target_name:
        raise ValueError(
            "Branch names containing '/' are not supported in worktree mode."
        )
    if not settings.worktree:
        return None

    dev_dir = settings.dev_dir
    if dev_dir is None:
        settings._resolve_active_worktree_root(raise_on_error=True)
        return None

    dev_dir = dev_dir.resolve()
    cwd = Path.cwd().resolve()

    # Worktree bootstrap from root is valid for `switch(..., create=True)`.
    if create and cwd == dev_dir:
        return None

    worktree_root = settings._resolve_active_worktree_root(raise_on_error=False)

    if cwd == dev_dir:
        branch_dir = dev_dir / target_name
        if branch_dir.is_dir():
            return f"To switch, run: cd {target_name}"
        return f"To switch, run: mkdir {target_name} && cd {target_name}"

    if worktree_root is not None and target_name != worktree_root.name:
        target_dir = dev_dir / target_name
        rel_target = os.path.relpath(target_dir, start=cwd)
        if target_dir.is_dir():
            return f"To switch, run: cd {rel_target}"
        return f"To switch, run: mkdir {rel_target} && cd {rel_target}"

    settings._resolve_active_worktree_root(raise_on_error=True)
    return None


def switch(target: str | Branch, *, space: bool = False, create: bool = False):
    """Switch to a branch or space, create if not exists.

    Args:
        target: Branch target or space target to switch to.
        space: If True, switch space; otherwise switch branch.
        create: If True and switching branch, create the branch if it does not exist.
    """
    is_worktree_bootstrap = False

    if space:
        settings.space = target
    else:
        target_name = target if isinstance(target, str) else target.name
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
        if settings.worktree:
            dev_dir = settings.dev_dir
            if dev_dir is not None:
                dev_dir = dev_dir.resolve()
                cwd = Path.cwd().resolve()
                if create and cwd == dev_dir:
                    child_dir = dev_dir / target_name
                    if child_dir.exists() and not child_dir.is_dir():
                        raise ValueError(
                            f"Cannot create worktree directory '{child_dir}': path exists and is not a directory."
                        )
                    child_dir.mkdir(parents=True, exist_ok=True)
                    original_cwd = cwd
                    previous_branch = settings._branch
                    try:
                        os.chdir(child_dir)
                        switch(target, space=False, create=True)
                    finally:
                        os.chdir(original_cwd)
                        # Root-level bootstrap should prepare the child worktree
                        # without mutating branch cache in the caller's context.
                        settings._branch = previous_branch
                    return
            instruction = worktree_switch_instruction(target_name, create=create)
            if instruction is not None:
                if create:
                    from lamindb import Branch, Q

                    existing = Branch.filter(
                        Q(name=target_name) | Q(uid=target_name)
                    ).one_or_none()
                    if existing is None:
                        raise ValueError(
                            missing_branch_create_and_navigate_message(
                                target_name, instruction
                            )
                        )
                raise ValueError(instruction)

        is_worktree_bootstrap = (
            create
            and settings.worktree
            and settings.dev_dir is not None
            and Path.cwd().resolve().parent == settings.dev_dir.resolve()
            and Path.cwd().resolve().name == target_name
            and not settings._branch_path.exists()
        )
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
    if is_worktree_bootstrap:
        logger.important_hint(f"to switch, cd into {target_name}")
    else:
        logger.important(f"switched to {target_name}")
