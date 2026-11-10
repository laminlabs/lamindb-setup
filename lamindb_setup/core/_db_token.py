from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import jwt

from ._logger import logger

if TYPE_CHECKING:
    from ._settings_instance import InstanceSettings


# Re-entry flag for the autocommit token transaction. Nested executes join it.
_LAMIN_TOKEN_TX = "_lamin_in_token_tx"


class _TokenAtomicBlock:
    """Stack marker so on_commit queues while an autocommit statement runs.

    Django still owns the real atomic blocks. This marker makes in_atomic_block
    true for the duration of one statement without calling set_autocommit(),
    which would fight the explicit BEGIN on the raw connection.
    """

    _from_testcase = False


def _in_atomic_block_is_property(connection) -> bool:
    # Django 5.2 and 6 store in_atomic_block as a plain attribute and assign it
    # in Atomic.__enter__/__exit__. This check is unused there. It covers an
    # older Django where in_atomic_block was a read-only property of
    # atomic_blocks; assigning it would raise, and appending the sentinel is
    # enough to make the property true.
    return isinstance(getattr(type(connection), "in_atomic_block", None), property)


def _set_in_atomic_block(connection, value: bool) -> None:
    if _in_atomic_block_is_property(connection):
        return
    connection.in_atomic_block = value


def _push_token_tx_view(connection):
    sentinel = _TokenAtomicBlock()
    previous = (connection.autocommit, connection.in_atomic_block)
    connection.atomic_blocks.append(sentinel)
    # Leave the adapter in autocommit. The explicit BEGIN below is the
    # transaction. Django's flag is what savepoints and on_commit consult.
    connection.autocommit = False
    _set_in_atomic_block(connection, True)
    return sentinel, previous


def _pop_token_tx_view(connection, sentinel, previous) -> None:
    previous_autocommit, previous_in_atomic = previous
    connection.atomic_blocks = [
        block for block in connection.atomic_blocks if block is not sentinel
    ]
    connection.autocommit = previous_autocommit
    if not _in_atomic_block_is_property(connection):
        connection.in_atomic_block = previous_in_atomic or bool(
            connection.atomic_blocks
        )


def _require_psycopg3(raw) -> None:
    if type(raw).__module__.split(".", 1)[0] != "psycopg":
        raise RuntimeError(
            "Fine-grained database access requires the psycopg 3 connection "
            "so the database token is sent as a bind parameter."
        )


def _transaction_status(raw):
    from psycopg.pq import TransactionStatus

    return raw.info.transaction_status, TransactionStatus


def _discard_connection(connection, raw) -> None:
    # Django's close() keeps a dead connection when in_atomic_block is set.
    # Drop the adapter connection directly so the next query reconnects.
    try:
        if raw is not None and not getattr(raw, "closed", False):
            raw.close()
    except Exception:
        pass
    connection.connection = None
    connection.closed_in_transaction = False
    connection.needs_rollback = False
    connection.rollback_exc = None


def _rollback_or_close(connection, raw) -> None:
    try:
        if raw is None or getattr(raw, "closed", False):
            _discard_connection(connection, raw)
            return
        status, TransactionStatus = _transaction_status(raw)
        if status != TransactionStatus.IDLE:
            raw.rollback()
    except Exception:
        _discard_connection(connection, raw)


def _commit_if_open(connection, raw) -> None:
    from django.db.transaction import TransactionManagementError

    try:
        status, TransactionStatus = _transaction_status(raw)
    except Exception:
        _discard_connection(connection, raw)
        raise
    if status == TransactionStatus.IDLE:
        return
    if status == TransactionStatus.INTRANS:
        try:
            # Deferred constraints fail here, outside the cursor that ran the SQL.
            # Translate that into Django's IntegrityError or DatabaseError.
            with connection.wrap_database_errors:
                raw.commit()
        except Exception:
            _discard_connection(connection, raw)
            raise
        return
    if status == TransactionStatus.INERROR:
        _rollback_or_close(connection, raw)
        raise TransactionManagementError(
            "An error occurred in the current transaction. "
            "The transaction was rolled back."
        )
    _discard_connection(connection, raw)
    raise RuntimeError(
        "Database connection was left in an unexpected transaction state."
    )


def _begin(connection, raw) -> None:
    # Raw cursor: Django's cursor would re-enter the token wrapper.
    with connection.wrap_database_errors:
        with raw.cursor() as cur:
            cur.execute("BEGIN")


def _execute_set_token(connection, token_value: str) -> None:
    """Send the token as a server-side bind parameter.

    psycopg.Cursor uses the extended query protocol. Django's cursor is a
    client-side cursor, which would paste the token into the query text.
    """
    raw = connection.connection
    _require_psycopg3(raw)
    from psycopg import Cursor

    with connection.wrap_database_errors:
        with Cursor(raw) as cur:
            cur.execute("SELECT set_token(%s, true)", (token_value,))
            cur.fetchone()


def _set_dbapi_rowcount(cursor, value: int) -> None:
    dbapi_cursor = getattr(cursor, "cursor", cursor)
    if hasattr(dbapi_cursor, "_rowcount"):
        dbapi_cursor._rowcount = value


def _execute_many_with_token(cursor, sql, params) -> None:
    """Run one autocommit transaction per parameter set and sum the row counts.

    psycopg's executemany() starts at 0 and adds each statement's affected rows,
    including an empty parameter sequence. A plain execute() loop would leave
    only the last statement's count.
    """
    total = 0
    for param_set in params:
        cursor.execute(sql, param_set)
        rowcount = cursor.rowcount
        if rowcount is not None and rowcount > 0:
            total += rowcount
    _set_dbapi_rowcount(cursor, total)


def _run_commit_hooks(hooks) -> None:
    import logging

    # Lamin's logger.error() does not accept exc_info, which Logger.exception() passes.
    commit_logger = logging.getLogger("django.db.backends")
    for _savepoint_ids, func, robust in hooks:
        if robust:
            try:
                func()
            except Exception as exc:
                commit_logger.error(
                    "Error calling %s in on_commit() during the database "
                    "token transaction (%s).",
                    getattr(func, "__qualname__", repr(func)),
                    exc,
                    exc_info=True,
                )
        else:
            func()


def _discard_token_tx_hooks(connection, hook_start: int) -> None:
    del connection.run_on_commit[hook_start:]
    connection.needs_rollback = False
    connection.rollback_exc = None


def _run_autocommit_with_token(connection, token_value: str, operation):
    """Run one autocommit statement in a server transaction with set_token.

    The transaction is finished before this function returns, so Django still
    observes autocommit: the next atomic() sees an idle connection, and
    WITH HOLD cursors can be fetched after execute().
    """
    from django.db.transaction import TransactionManagementError

    raw = connection.connection
    sentinel, previous = _push_token_tx_view(connection)
    hook_start = len(connection.run_on_commit)
    setattr(connection, _LAMIN_TOKEN_TX, True)
    hooks = []
    try:
        _begin(connection, raw)
        try:
            _execute_set_token(connection, token_value)
            result = operation()
        except BaseException:
            _rollback_or_close(connection, raw)
            _discard_token_tx_hooks(connection, hook_start)
            raise
        else:
            if connection.needs_rollback:
                _rollback_or_close(connection, raw)
                _discard_token_tx_hooks(connection, hook_start)
                raise TransactionManagementError(
                    "An error occurred in the current transaction. "
                    "The transaction was rolled back."
                )
            _commit_if_open(connection, raw)
            hooks = list(connection.run_on_commit[hook_start:])
            del connection.run_on_commit[hook_start:]
    finally:
        setattr(connection, _LAMIN_TOKEN_TX, False)
        _pop_token_tx_view(connection, sentinel, previous)
    _run_commit_hooks(hooks)
    return result


# db token that refreshes on access if needed
class DBToken:
    def __init__(
        self, instance: InstanceSettings | dict, access_token: str | None = None
    ):
        self.instance = instance
        self.access_token = access_token
        self._token: str | None = None
        self._expiration: float | None = None
        self._type: str | None = None
        self._refresh_lock = threading.Lock()

    def _refresh_token(self):
        from ._hub_core import access_db

        self._token = access_db(self.instance, self.access_token)
        token_decoded = jwt.decode(self._token, options={"verify_signature": False})
        self._expiration = token_decoded["exp"]
        self._type = token_decoded["type"]

    def _needs_refresh(self) -> bool:
        return self._token is None or time.time() >= self._expiration  # type: ignore

    @property
    def token(self) -> str:
        # One refresh at a time. The second check skips the hub when the
        # waiter arrives after another thread has already stored a new token.
        if self._needs_refresh():
            with self._refresh_lock:
                if self._needs_refresh():
                    self._refresh_token()

        return self._token  # type: ignore


# a class to manage jwt in dbs
class DBTokenManager:
    def __init__(self):
        from django.db.transaction import Atomic

        self.original_atomic_enter = Atomic.__enter__
        self.atomic_is_patched = False

        self.tokens: dict[str, DBToken] = {}

    def set(self, token: DBToken, connection_name: str = "default"):
        if connection_name in self.tokens:
            return

        from django.db.backends.signals import connection_created
        from django.db.transaction import Atomic

        from .django import get_connection

        def set_token_wrapper(execute, sql, params, many, context):
            connection = context["connection"]
            # Nested statement of the token transaction, or a real atomic block.
            # Atomic.__enter__ already ran set_token for the outermost block.
            in_token_tx = getattr(connection, _LAMIN_TOKEN_TX, False)
            if in_token_tx or connection.in_atomic_block:
                return execute(sql, params, many, context)
            # set_autocommit(False) outside atomic() is a caller-managed
            # transaction. Set the token inside it and leave commit to the caller.
            if not connection.get_autocommit():
                _execute_set_token(connection, token.token)
                return execute(sql, params, many, context)
            # Keep one autocommit transaction per parameter set.
            if many:
                _execute_many_with_token(context["cursor"], sql, params)
                return None

            # Resolve before BEGIN so a token refresh does not hold the transaction.
            token_value = token.token

            def operation():
                return execute(sql, params, many, context)

            return _run_autocommit_with_token(connection, token_value, operation)

        get_connection(connection_name).execute_wrappers.append(set_token_wrapper)

        def connection_callback(sender, connection, **kwargs):
            if (
                connection.alias == connection_name
                and set_token_wrapper not in connection.execute_wrappers
            ):
                connection.execute_wrappers.append(set_token_wrapper)

        dispatch_uid = f"dbtokenmanager:{id(self)}:{connection_name}"
        # emitted when a database connection is established
        # not when a database wrapper is created
        connection_created.connect(
            connection_callback, dispatch_uid=dispatch_uid, weak=False
        )

        self.tokens[connection_name] = token

        if not self.atomic_is_patched:
            # ensure we set the token only once for an outer atomic block
            def __enter__(atomic):
                connection_name = "default" if atomic.using is None else atomic.using
                # Refresh before entering so the hub request does not sit inside the block.
                token_value = None
                if connection_name in self.tokens:
                    token_value = self.tokens[connection_name].token
                self.original_atomic_enter(atomic)
                if token_value is None:
                    return
                # Atomic manages every connection. Do not close over one of them.
                connection = get_connection(connection_name)
                if len(connection.atomic_blocks) != 1:
                    return
                try:
                    # Outermost block already opened the transaction.
                    _execute_set_token(connection, token_value)
                except BaseException:
                    # __exit__ does not run when __enter__ raises.
                    connection.needs_rollback = True
                    try:
                        atomic.__exit__(None, None, None)
                    except Exception:
                        _discard_connection(connection, connection.connection)
                    raise

            Atomic.__enter__ = __enter__

            self.atomic_is_patched = True
            logger.debug("django.db.transaction.Atomic.__enter__ has been patched")

    def reset(self, connection_name: str = "default"):
        if connection_name not in self.tokens:
            return

        from django.db.backends.signals import connection_created

        from .django import get_connection

        connection = get_connection(connection_name)

        connection.execute_wrappers = [
            w
            for w in connection.execute_wrappers
            if getattr(w, "__name__", None) != "set_token_wrapper"
        ]

        dispatch_uid = f"dbtokenmanager:{id(self)}:{connection_name}"
        connection_created.disconnect(dispatch_uid=dispatch_uid)

        self.tokens.pop(connection_name, None)

        if not self.tokens:
            from django.db.transaction import Atomic

            Atomic.__enter__ = self.original_atomic_enter
            self.atomic_is_patched = False


db_token_manager = DBTokenManager()
