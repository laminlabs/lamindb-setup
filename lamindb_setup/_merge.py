from __future__ import annotations

from typing import TYPE_CHECKING

from .core._logger import logger
from .core._settings import settings

if TYPE_CHECKING:
    from django.db.backends.utils import CursorWrapper
    from django.db.models import Model
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


def _check_merge_permissions(
    cursor: CursorWrapper, models: list[type[Model]], source: Branch
) -> None:
    """Check PostgreSQL update policies for every readable merge candidate."""
    from lamindb import Space

    # RLS can hide source records from both preflight and the residual check.
    # Restricted connections require the database's privileged completion guard.
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_trigger "
        "WHERE tgrelid = %s::regclass "
        "AND tgname = 'lamindb_require_empty_merged_branch' "
        "AND tgenabled IN ('O', 'A')) "
        "OR NOT EXISTS (SELECT 1 FROM unnest(%s::text[]) AS tables(name) "
        "WHERE row_security_active(name::regclass))",
        [type(source)._meta.db_table, [model._meta.db_table for model in models]],
    )
    if not cursor.fetchone()[0]:
        raise PermissionError(
            "Cannot merge safely with row-level security until the database is "
            "updated. Ask an administrator to upgrade LaminDB and migrate the instance."
        )

    checks = []
    params = []
    # The source branch must also be writable to mark it as merged.
    candidates = [(model, "branch_id") for model in models] + [(type(source), "id")]
    for model, column in candidates:
        table = cursor.db.ops.quote_name(model._meta.db_table)
        space = (
            "space_id"
            if any(field.column == "space_id" for field in model._meta.concrete_fields)
            else "NULL::smallint"
        )
        # FOR UPDATE applies the database's UPDATE USING policies, including
        # record grants, locks, and permissions inherited from parent records.
        checks.append(
            f"SELECT %s, {space}, COUNT(*) FROM {table} "
            f"WHERE {column} = %s AND id NOT IN "
            f"(SELECT id FROM {table} WHERE {column} = %s FOR UPDATE) GROUP BY 2"
        )
        params.extend([model._meta.label, source.id, source.id])
    cursor.execute(" UNION ALL ".join(checks), params)
    denied = cursor.fetchall()
    if not denied:
        return

    space_ids = {space_id for _, space_id, _ in denied if space_id is not None}
    space_names = dict(Space.objects.filter(id__in=space_ids).values_list("id", "name"))
    details = []
    for registry, space_id, count in denied:
        location = (
            f" in space '{space_names.get(space_id, space_id)}'"
            if space_id is not None
            else ""
        )
        details.append(
            f"{registry}: {count} record{'s' if count != 1 else ''}{location}"
        )
    raise PermissionError(
        f"Cannot merge branch '{source.name}': cannot update "
        + "; ".join(details)
        + ". Check write access and record locks."
    )


def merge(branch: str | Branch, *, target: str | Branch | None = None) -> None:
    """Merge a source branch into a target branch atomically.

    PostgreSQL update permissions are checked before moving readable source
    records. A database guard also rejects completion if hidden source records
    remain. Any failure during the merge rolls back all changes.

    Raises:
        PermissionError: If readable source records cannot be moved. All changes
            are rolled back and the source branch is left unmerged.
        django.db.IntegrityError: If the database detects hidden source records
            remaining before the branch is marked merged.
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
                _check_merge_permissions(cursor, models, source)
                cursor.execute("; ".join(statements), params * len(statements))
            else:
                for statement in statements:
                    cursor.execute(statement, params)

            # Permissions may change after preflight, and triggers can skip writes.
            # Check every participating table before committing any changes.
            remaining_records = " UNION ALL ".join(
                f"SELECT 1 FROM {tbl} WHERE branch_id = %s" for tbl in quoted_tables
            )
            cursor.execute(
                f"SELECT EXISTS({remaining_records})", [source.id] * len(quoted_tables)
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
