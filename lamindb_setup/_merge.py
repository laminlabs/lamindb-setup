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

    Any failure rolls back all changes. PostgreSQL requires READ COMMITTED
    semantics (also provided by READ UNCOMMITTED). REPEATABLE READ and SERIALIZABLE
    isolation raise `django.db.NotSupportedError`.
    PostgreSQL connections subject to row-level security require a hubmodule
    guard to detect hidden source records.
    Other connections check for remaining source records before completion.
    Database updates or the completion guard can raise `django.db.DatabaseError`.

    Raises:
        PermissionError: If active row-level security requires a hubmodule update,
            the source branch cannot be updated, or source records remain.
    """
    from django.apps import apps
    from django.db import NotSupportedError, connection, transaction
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
    check_tables = quoted_tables

    with transaction.atomic():
        with connection.cursor() as cursor:
            statements = [
                f"UPDATE {tbl} SET branch_id = %s WHERE branch_id = %s"
                for tbl in quoted_tables
            ]
            params = [target_branch.id, source.id]
            if vendor == "postgresql":
                cursor.execute("SHOW transaction_isolation")
                isolation = cursor.fetchone()[0]
                if isolation not in ("read committed", "read uncommitted"):
                    # An older snapshot can retain a removed guard or omit source
                    # records committed before the source lock was acquired.
                    raise NotSupportedError(
                        f"Cannot merge safely at {isolation.upper()} isolation. "
                        "Run merge() in a READ COMMITTED transaction."
                    )
                # Serialize source merges and prevent guard removal until commit.
                cursor.execute(
                    f"SELECT id FROM {connection.ops.quote_name(type(source)._meta.db_table)} "
                    "WHERE id = %s FOR UPDATE",
                    [source.id],
                )
                if cursor.fetchone() is None:
                    raise PermissionError(
                        f"Cannot merge branch '{source.name}': cannot update the source branch."
                    )
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_trigger "
                    "WHERE tgrelid = %s::regclass "
                    "AND tgname = 'lamindb_require_empty_merged_branch' "
                    "AND tgenabled IN ('O', 'A'))",
                    [type(source)._meta.db_table],
                )
                if cursor.fetchone()[0]:
                    check_tables = []
                else:
                    # Match the guard's membership scope, including module tables
                    # that are installed in the database but absent from this client.
                    cursor.execute(
                        "SELECT DISTINCT ns.nspname, rel.relname, "
                        "pg_catalog.row_security_active(rel.oid) "
                        "FROM pg_constraint fk "
                        "JOIN pg_class rel ON rel.oid = fk.conrelid "
                        "JOIN pg_namespace ns ON ns.oid = rel.relnamespace "
                        "JOIN pg_attribute col ON col.attrelid = rel.oid "
                        "AND col.attnum = fk.conkey[1] "
                        "WHERE fk.contype = 'f' AND fk.confrelid = %s::regclass "
                        "AND array_length(fk.conkey, 1) = 1 "
                        "AND col.attname = 'branch_id' "
                        "AND EXISTS (SELECT 1 FROM pg_attribute provenance "
                        "WHERE provenance.attrelid = rel.oid "
                        "AND provenance.attname = 'created_on_id' "
                        "AND NOT provenance.attisdropped)",
                        [type(source)._meta.db_table],
                    )
                    membership = cursor.fetchall()
                    if any(rls_active for _, _, rls_active in membership):
                        raise PermissionError(
                            "Cannot merge safely with active row-level security "
                            "until the database merge guard is installed. "
                            "For a hub-managed instance, ask an administrator to update "
                            "hubmodule and deploy it to this instance. Otherwise, ask a "
                            "database administrator to merge using a connection that is "
                            "not subject to row-level security on the source tables."
                        )
                    check_tables = [
                        f"{connection.ops.quote_name(schema)}.{connection.ops.quote_name(table)}"
                        for schema, table, _ in membership
                    ]
                cursor.execute("; ".join(statements), params * len(statements))
            else:
                for statement in statements:
                    cursor.execute(statement, params)

        versioned_models = [m for m in models if issubclass(m, IsVersioned)]
        for model in versioned_models:
            reconcile_is_latest_within_branch(model, branch_id=target_branch.id)

        if check_tables:
            with connection.cursor() as cursor:
                if vendor == "postgresql":
                    cursor.execute("SHOW row_security")
                    previous_row_security = cursor.fetchone()[0]
                    # Error rather than hide rows if RLS became active since preflight.
                    cursor.execute("SET LOCAL row_security = off")
                remaining_records = " UNION ALL ".join(
                    f"SELECT 1 FROM {table} WHERE branch_id = %s"
                    for table in check_tables
                )
                cursor.execute(
                    f"SELECT EXISTS({remaining_records})",
                    [source.id] * len(check_tables),
                )
                if cursor.fetchone()[0]:
                    raise PermissionError(
                        f"Cannot merge branch '{source.name}': some records could not be "
                        "moved. Check write access to all source records."
                    )
                if vendor == "postgresql":
                    # SET LOCAL otherwise persists in a caller-owned outer transaction.
                    cursor.execute(
                        "SELECT set_config('row_security', %s, true)",
                        [previous_row_security],
                    )

        previous_status = source._status_code
        try:
            source._status_code = -1  # merged
            source.save(update_fields=["_status_code"])
        finally:
            source._status_code = previous_status
    source._status_code = -1  # Publish the new status only after a successful merge.
    logger.important(f"merged branch '{source.name}' into '{target_branch.name}'")
