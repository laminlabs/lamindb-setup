from __future__ import annotations

from typing import TYPE_CHECKING

from lamin_utils import logger

from .core._settings import settings

if TYPE_CHECKING:
    from lamindb.models import Branch


def _resolve_branch(branch: str | Branch) -> Branch:
    """Resolve a branch by object, name, or uid."""
    from lamindb import Branch, Q
    from lamindb.errors import ObjectDoesNotExist

    if isinstance(branch, Branch):
        branch_record = branch
        if branch_record._state.adding:
            raise ObjectDoesNotExist("Branch must be saved.")
        return branch_record

    branch_record = Branch.filter(Q(name=branch) | Q(uid=branch)).one_or_none()
    if branch_record is None:
        raise ObjectDoesNotExist(f"Branch '{branch}' not found.")
    return branch_record


def merge(branch: str | Branch, *, target: str | Branch | None = None) -> None:
    """Merge a source branch into a target branch."""
    from django.apps import apps
    from django.db import DatabaseError, connection
    from lamindb.models import SQLRecord
    from lamindb.models._is_versioned import (
        IsVersioned,
        reconcile_is_latest_within_branch,
    )
    from lamindb.models.sqlrecord import BRANCH_SENSITIVE_BLOCK_MODEL_NAMES

    source = _resolve_branch(branch)
    target_branch: Branch = (
        settings.branch if target is None else _resolve_branch(target)
    )
    if target_branch.id == source.id:
        logger.important("source and target branch are identical, nothing to merge")
        return

    sqlrecord_models = [
        m
        for m in apps.get_models()
        if issubclass(m, SQLRecord) and not m._meta.abstract
    ]
    attached_block_models = [
        model
        for model_name in sorted(BRANCH_SENSITIVE_BLOCK_MODEL_NAMES)
        if (model := apps.get_model("lamindb", model_name)) is not None
    ]
    models = list(dict.fromkeys([*sqlrecord_models, *attached_block_models]))
    if not models:
        return

    vendor = connection.vendor
    quoted_tables = [connection.ops.quote_name(m._meta.db_table) for m in models]

    with connection.cursor() as cursor:
        if vendor == "postgresql":
            statements = [
                f"UPDATE {tbl} SET branch_id = %s WHERE branch_id = %s"
                for tbl in quoted_tables
            ]
            sql = "BEGIN; " + "; ".join(statements) + "; COMMIT;"
            params = [target_branch.id, source.id] * len(quoted_tables)
            try:
                cursor.execute(sql, params)
            except DatabaseError as e:
                logger.error(f"Merge failed: {e}")
                raise
        else:
            from django.db import transaction

            with transaction.atomic():
                for tbl in quoted_tables:
                    cursor.execute(
                        f"UPDATE {tbl} SET branch_id = %s WHERE branch_id = %s",
                        [target_branch.id, source.id],
                    )

    versioned_models = [m for m in models if issubclass(m, IsVersioned)]
    for model in versioned_models:
        reconcile_is_latest_within_branch(model, branch_id=target_branch.id)

    source._status_code = -1  # merged
    source.save(update_fields=["_status_code"])
    logger.important(f"merged branch '{source.name}' into '{target_branch.name}'")
