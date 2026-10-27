from __future__ import annotations

from typing import TYPE_CHECKING

from .core._logger import logger
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
    """Merge a source branch into a target branch atomically.

    Any failure rolls back all changes. PostgreSQL requires a database guard
    that rejects completion if any source records remain, including hidden ones.

    Raises:
        PermissionError: If the PostgreSQL database needs migration, the source
            branch cannot be updated, or records remain on SQLite.
        django.db.DatabaseError: If the database rejects an update or detects
            source records remaining before the branch is marked merged.
    """
    from django.apps import apps
    from django.db import connection, transaction
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

    with transaction.atomic():
        with connection.cursor() as cursor:
            statements = [
                f"UPDATE {tbl} SET branch_id = %s WHERE branch_id = %s"
                for tbl in quoted_tables
            ]
            params = [target_branch.id, source.id]
            if vendor == "postgresql":
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_trigger "
                    "WHERE tgrelid = %s::regclass "
                    "AND tgname = 'lamindb_require_empty_merged_branch' "
                    "AND tgenabled IN ('O', 'A'))",
                    [type(source)._meta.db_table],
                )
                if not cursor.fetchone()[0]:
                    raise PermissionError(
                        "Cannot merge safely until the database is updated. "
                        "Ask an administrator to upgrade LaminDB and migrate the instance."
                    )
                # Serialize merges from this branch until the transaction completes.
                cursor.execute(
                    f"SELECT id FROM {connection.ops.quote_name(type(source)._meta.db_table)} "
                    "WHERE id = %s FOR UPDATE",
                    [source.id],
                )
                if cursor.fetchone() is None:
                    raise PermissionError(
                        f"Cannot merge branch '{source.name}': cannot update the source branch."
                    )
                cursor.execute("; ".join(statements), params * len(statements))
            else:
                for statement in statements:
                    cursor.execute(statement, params)

                # SQLite has no database completion guard.
                remaining_records = " UNION ALL ".join(
                    f"SELECT 1 FROM {tbl} WHERE branch_id = %s" for tbl in quoted_tables
                )
                cursor.execute(
                    f"SELECT EXISTS({remaining_records})",
                    [source.id] * len(quoted_tables),
                )
                if cursor.fetchone()[0]:
                    raise PermissionError(
                        f"Cannot merge branch '{source.name}': some records could not be "
                        "moved. Check write access to all source records."
                    )

        versioned_models = [m for m in models if issubclass(m, IsVersioned)]
        for model in versioned_models:
            reconcile_is_latest_within_branch(model, branch_id=target_branch.id)

        source._status_code = -1  # merged
        source.save(update_fields=["_status_code"])
    logger.important(f"merged branch '{source.name}' into '{target_branch.name}'")
