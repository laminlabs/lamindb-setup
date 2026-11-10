from __future__ import annotations

import threading
import time

import jwt
import pytest
from django.db.transaction import TransactionManagementError
from django.db.utils import OperationalError
from lamindb_setup.core import _db_token
from lamindb_setup.core._db_token import (
    DBToken,
    DBTokenManager,
    _begin,
    _commit_if_open,
    _discard_connection,
    _execute_many_with_token,
    _execute_set_token,
    _in_atomic_block_is_property,
    _pop_token_tx_view,
    _push_token_tx_view,
    _require_psycopg3,
    _rollback_or_close,
    _run_autocommit_with_token,
    _run_commit_hooks,
    _set_in_atomic_block,
)
from psycopg.pq import TransactionStatus


class _Conn:
    def __init__(self):
        self.autocommit = True
        self.in_atomic_block = False
        self.atomic_blocks = []
        self.run_on_commit = []
        self.needs_rollback = False
        self.rollback_exc = None
        self.closed_in_transaction = False
        self.connection = _Raw()
        self.wrap_database_errors = _PassThrough()
        self.execute_wrappers = []
        self.alias = "default"

    def get_autocommit(self):
        return self.autocommit


class _Raw:
    __module__ = "psycopg.connection"

    def __init__(self, status=TransactionStatus.INTRANS):
        self.closed = False
        self.info = _Info(status)
        self.begun = []
        self.commits = 0
        self.rollbacks = 0
        self.fail_commit = None
        self.fail_rollback = None
        self.fail_status = None

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        if self.fail_commit is not None:
            raise self.fail_commit
        self.commits += 1
        self.info.transaction_status = TransactionStatus.IDLE

    def rollback(self):
        if self.fail_rollback is not None:
            raise self.fail_rollback
        self.rollbacks += 1
        self.info.transaction_status = TransactionStatus.IDLE

    def close(self):
        self.closed = True


class _Info:
    def __init__(self, status):
        self.transaction_status = status


class _StatusInfo:
    def __init__(self, raw):
        self.raw = raw

    @property
    def transaction_status(self):
        if self.raw.fail_status is not None:
            raise self.raw.fail_status
        return self.raw.info.transaction_status


class _Cursor:
    def __init__(self, raw):
        self.raw = raw

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, params=None):
        self.raw.begun.append((sql, params))


class _ServerCursor:
    def __init__(self, raw):
        self.raw = raw
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        self.raw.server_statements = self.statements

    def fetchone(self):
        return None


@pytest.fixture(autouse=True)
def _server_side_cursor(monkeypatch):
    monkeypatch.setattr("psycopg.Cursor", _ServerCursor)


class _PassThrough:
    def __enter__(self):
        return None

    def __exit__(self, *_args):
        return None


class _Translate:
    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc, _tb):
        if exc_type is None:
            return False
        raise OperationalError(str(exc)) from exc


def _context(connection, cursor=None):
    return {"connection": connection, "cursor": cursor}


def _jwt(exp_offset: float, token_type: str = "read-write") -> str:
    return jwt.encode(
        {"exp": time.time() + exp_offset, "type": token_type},
        "secret",
        algorithm="HS256",
    )


def test_property_atomic_flag_is_driven_by_the_sentinel():
    class _Legacy:
        def __init__(self):
            self.autocommit = True
            self.atomic_blocks = []

        @property
        def in_atomic_block(self):
            return bool(self.atomic_blocks)

    connection = _Legacy()
    assert _in_atomic_block_is_property(connection)
    _set_in_atomic_block(connection, True)
    sentinel, previous = _push_token_tx_view(connection)
    assert connection.in_atomic_block is True
    assert connection.autocommit is False
    _pop_token_tx_view(connection, sentinel, previous)
    assert connection.in_atomic_block is False
    assert connection.autocommit is True


def test_pop_keeps_atomic_flag_when_another_block_remains():
    connection = _Conn()
    connection.atomic_blocks.append(object())
    sentinel, previous = _push_token_tx_view(connection)
    _pop_token_tx_view(connection, sentinel, previous)
    assert connection.in_atomic_block is True
    assert sentinel not in connection.atomic_blocks

    already_atomic = _Conn()
    already_atomic.in_atomic_block = True
    sentinel, previous = _push_token_tx_view(already_atomic)
    _pop_token_tx_view(already_atomic, sentinel, previous)
    assert already_atomic.in_atomic_block is True
    assert already_atomic.atomic_blocks == []


def test_require_psycopg3_rejects_other_drivers():
    class _OtherDriver:
        __module__ = "psycopg2.extensions"

    with pytest.raises(RuntimeError, match="psycopg 3") as exc:
        _require_psycopg3(_OtherDriver())
    message = str(exc.value)
    assert "psycopg2" in message
    assert 'pip install "psycopg[binary]>=3.1.12,<4"' in message


def test_discard_connection_closes_and_survives_close_errors():
    connection = _Conn()
    connection.closed_in_transaction = True
    connection.needs_rollback = True
    connection.rollback_exc = RuntimeError("marked")
    raw = connection.connection
    _discard_connection(connection, raw)
    assert raw.closed is True
    assert connection.connection is None
    assert connection.closed_in_transaction is False
    assert connection.needs_rollback is False
    assert connection.rollback_exc is None

    connection = _Conn()
    raw = connection.connection
    raw.close = lambda: (_ for _ in ()).throw(RuntimeError("close failed"))
    _discard_connection(connection, raw)
    assert connection.connection is None
    assert connection.needs_rollback is False

    connection = _Conn()
    _discard_connection(connection, None)
    assert connection.connection is None

    already_closed = _Conn()
    raw = already_closed.connection
    raw.closed = True

    def close_again():
        raise AssertionError("closed connection should not be closed again")

    raw.close = close_again
    _discard_connection(already_closed, raw)
    assert already_closed.connection is None


def test_rollback_or_close_branches():
    idle = _Conn()
    idle.connection.info.transaction_status = TransactionStatus.IDLE
    _rollback_or_close(idle, idle.connection)
    assert idle.connection.rollbacks == 0
    assert idle.connection is not None

    open_tx = _Conn()
    _rollback_or_close(open_tx, open_tx.connection)
    assert open_tx.connection.rollbacks == 1

    closed = _Conn()
    closed.connection.closed = True
    _rollback_or_close(closed, closed.connection)
    assert closed.connection is None

    missing = _Conn()
    _rollback_or_close(missing, None)
    assert missing.connection is None

    broken = _Conn()
    broken.connection.fail_rollback = RuntimeError("rollback failed")
    _rollback_or_close(broken, broken.connection)
    assert broken.connection is None

    unreadable = _Conn()
    unreadable.connection.fail_status = RuntimeError("dead")
    unreadable.connection.info = _StatusInfo(unreadable.connection)
    _rollback_or_close(unreadable, unreadable.connection)
    assert unreadable.connection is None


def test_commit_if_open_branches():
    idle = _Conn()
    idle.connection.info.transaction_status = TransactionStatus.IDLE
    _commit_if_open(idle, idle.connection)
    assert idle.connection.commits == 0

    ready = _Conn()
    _commit_if_open(ready, ready.connection)
    assert ready.connection.commits == 1

    failed = _Conn()
    failed.connection.fail_commit = RuntimeError("constraint")
    with pytest.raises(RuntimeError, match="constraint"):
        _commit_if_open(failed, failed.connection)
    assert failed.connection is None

    inerror = _Conn()
    inerror.connection.info.transaction_status = TransactionStatus.INERROR
    with pytest.raises(TransactionManagementError):
        _commit_if_open(inerror, inerror.connection)
    assert inerror.connection.rollbacks == 1

    inerror_dead = _Conn()
    inerror_dead.connection.info.transaction_status = TransactionStatus.INERROR
    inerror_dead.connection.fail_rollback = RuntimeError("rollback failed")
    with pytest.raises(TransactionManagementError):
        _commit_if_open(inerror_dead, inerror_dead.connection)
    assert inerror_dead.connection is None

    unknown = _Conn()
    unknown.connection.info.transaction_status = TransactionStatus.ACTIVE
    with pytest.raises(RuntimeError, match="unexpected transaction state"):
        _commit_if_open(unknown, unknown.connection)
    assert unknown.connection is None

    unreadable = _Conn()
    unreadable.connection.fail_status = RuntimeError("dead")
    unreadable.connection.info = _StatusInfo(unreadable.connection)
    with pytest.raises(RuntimeError, match="dead"):
        _commit_if_open(unreadable, unreadable.connection)
    assert unreadable.connection is None


def test_begin_translates_driver_errors():
    connection = _Conn()
    connection.wrap_database_errors = _Translate()

    def execute(_sql, _params=None):
        raise RuntimeError("connection lost")

    connection.connection.cursor = lambda: _FailCursor(execute)
    with pytest.raises(OperationalError, match="connection lost"):
        _begin(connection, connection.connection)


class _FailCursor:
    def __init__(self, execute):
        self._execute = execute

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, params=None):
        self._execute(sql, params)


def test_execute_set_token_uses_server_cursor(monkeypatch):
    connection = _Conn()
    monkeypatch.setattr("psycopg.Cursor", _ServerCursor)
    _execute_set_token(connection, "jwt-token")
    assert connection.connection.server_statements == [
        ("SELECT set_token(%s, true)", ("jwt-token",))
    ]


def test_execute_set_token_translates_driver_errors(monkeypatch):
    connection = _Conn()
    connection.wrap_database_errors = _Translate()

    class _BrokenCursor(_ServerCursor):
        def execute(self, _sql, _params=None):
            raise RuntimeError("token rejected")

    monkeypatch.setattr("psycopg.Cursor", _BrokenCursor)
    with pytest.raises(OperationalError, match="token rejected"):
        _execute_set_token(connection, "jwt-token")


def test_execute_many_sums_rowcounts_and_clears_empty_batches():
    class _BatchCursor:
        def __init__(self):
            self.rowcount = -1
            self._rowcount = -1
            self.calls = []

        def execute(self, sql, params):
            self.calls.append((sql, params))
            self.rowcount = params[0]
            self._rowcount = params[0]

    cursor = _BatchCursor()
    _execute_many_with_token(
        cursor, "INSERT INTO t VALUES (%s)", [(2,), (0,), (None,), (-1,), (3,)]
    )
    assert [params for _sql, params in cursor.calls] == [
        (2,),
        (0,),
        (None,),
        (-1,),
        (3,),
    ]
    assert cursor._rowcount == 5

    empty = _BatchCursor()
    _execute_many_with_token(empty, "INSERT INTO t VALUES (%s)", [])
    assert empty._rowcount == 0

    class _Inner:
        def __init__(self):
            self._rowcount = -1

    class _Outer:
        def __init__(self):
            self.cursor = _Inner()
            self.rowcount = 4

        def execute(self, _sql, _params):
            return None

    outer = _Outer()
    _execute_many_with_token(outer, "UPDATE t SET x = %s", [(1,)])
    assert outer.cursor._rowcount == 4

    plain = object()
    _db_token._set_dbapi_rowcount(plain, 4)
    assert not hasattr(plain, "_rowcount")


def test_commit_hooks_continue_after_robust_failures(caplog):
    def ok():
        ok.ran = True

    def bad():
        raise RuntimeError("hook failed")

    ok.ran = False
    with caplog.at_level("ERROR", logger="django.db.backends"):
        _run_commit_hooks([(set(), bad, True), (set(), ok, False)])
    assert ok.ran is True
    assert "hook failed" in caplog.text

    def stop():
        raise RuntimeError("stop")

    with pytest.raises(RuntimeError, match="stop"):
        _run_commit_hooks([(set(), stop, False)])


def test_autocommit_statement_commits_and_runs_hooks():
    connection = _Conn()
    ran = {}

    def earlier():
        ran["earlier"] = True

    def hook():
        ran["hook"] = True

    connection.run_on_commit.append((set(), earlier, False))

    def operation():
        connection.run_on_commit.append((set(), hook, False))
        return "done"

    assert _run_autocommit_with_token(connection, "jwt", operation) == "done"
    assert connection.connection.begun[0][0] == "BEGIN"
    assert connection.connection.server_statements == [
        ("SELECT set_token(%s, true)", ("jwt",))
    ]
    assert connection.connection.commits == 1
    assert ran == {"hook": True}
    assert len(connection.run_on_commit) == 1
    assert connection.run_on_commit[0][1] is earlier
    assert connection.autocommit is True
    assert connection.in_atomic_block is False
    assert connection._lamin_in_token_tx is False


def test_autocommit_statement_rolls_back_and_drops_hooks():
    connection = _Conn()

    def earlier():
        return None

    connection.run_on_commit.append((set(), earlier, False))

    def operation():
        connection.run_on_commit.append((set(), lambda: None, False))
        connection.rollback_exc = RuntimeError("marked")
        raise RuntimeError("query failed")

    with pytest.raises(RuntimeError, match="query failed"):
        _run_autocommit_with_token(connection, "jwt", operation)
    assert connection.connection.rollbacks == 1
    assert len(connection.run_on_commit) == 1
    assert connection.run_on_commit[0][1] is earlier
    assert connection.needs_rollback is False
    assert connection.rollback_exc is None
    assert connection.autocommit is True
    assert connection._lamin_in_token_tx is False


def test_autocommit_statement_honors_needs_rollback():
    connection = _Conn()
    connection.rollback_exc = RuntimeError("marked")

    def operation():
        connection.needs_rollback = True
        return "ignored"

    with pytest.raises(TransactionManagementError):
        _run_autocommit_with_token(connection, "jwt", operation)
    assert connection.connection.rollbacks == 1
    assert connection.run_on_commit == []
    assert connection.needs_rollback is False
    assert connection.rollback_exc is None


def test_autocommit_statement_restores_flags_when_begin_fails():
    connection = _Conn()

    def earlier():
        return None

    connection.run_on_commit.append((set(), earlier, False))

    def execute(_sql, _params=None):
        raise RuntimeError("begin failed")

    connection.connection.cursor = lambda: _FailCursor(execute)
    with pytest.raises(RuntimeError, match="begin failed"):
        _run_autocommit_with_token(connection, "jwt", lambda: "nope")
    assert connection.connection.rollbacks == 0
    assert len(connection.run_on_commit) == 1
    assert connection.autocommit is True
    assert connection.in_atomic_block is False
    assert connection._lamin_in_token_tx is False


def test_autocommit_statement_discards_connection_when_commit_fails():
    connection = _Conn()
    connection.connection.fail_commit = RuntimeError("constraint")
    with pytest.raises(RuntimeError, match="constraint"):
        _run_autocommit_with_token(connection, "jwt", lambda: "done")
    assert connection.connection is None
    assert connection.autocommit is True
    assert connection.in_atomic_block is False


def test_db_token_refreshes_when_missing_or_expired(monkeypatch):
    encoded = []

    def access_db(_instance, _access_token):
        encoded.append(_jwt(60 if len(encoded) == 0 else 120, "read-only"))
        return encoded[-1]

    monkeypatch.setattr("lamindb_setup.core._hub_core.access_db", access_db)
    token = DBToken({"id": "instance"}, access_token="access")
    assert token.token == encoded[0]
    assert token._type == "read-only"
    assert token.token == encoded[0]
    token._expiration = time.time() - 1
    assert token.token == encoded[1]


def test_db_token_refresh_is_single_flight(monkeypatch):
    first_inside = threading.Event()
    release_refresh = threading.Event()
    calls = {"n": 0}

    def access_db(_instance, _access_token):
        calls["n"] += 1
        first_inside.set()
        assert release_refresh.wait(timeout=2)
        return _jwt(60)

    monkeypatch.setattr("lamindb_setup.core._hub_core.access_db", access_db)
    token = DBToken({"id": "instance"})
    result: dict[str, str] = {}

    def read(slot: str) -> None:
        result[slot] = token.token

    first = threading.Thread(target=read, args=("a",))
    second = threading.Thread(target=read, args=("b",))
    first.start()
    assert first_inside.wait(timeout=2)
    second.start()
    second.join(timeout=0.2)
    assert second.is_alive()
    assert calls["n"] == 1
    release_refresh.set()
    first.join(timeout=2)
    second.join(timeout=2)
    assert not first.is_alive()
    assert not second.is_alive()
    assert calls["n"] == 1
    assert result["a"] == result["b"]


def test_manager_registers_resets_and_sets_token_once(monkeypatch):
    from django.db.backends.signals import connection_created
    from django.db.transaction import Atomic as live_atomic

    original_enter = live_atomic.__enter__
    connections = {}
    receivers = {}

    def get_connection(name):
        return connections[name]

    def connect(callback, **kwargs):
        receivers[kwargs["dispatch_uid"]] = callback

    def disconnect(**_kwargs):
        return None

    monkeypatch.setattr("lamindb_setup.core.django.get_connection", get_connection)
    monkeypatch.setattr(connection_created, "connect", connect)
    monkeypatch.setattr(connection_created, "disconnect", disconnect)
    manager = DBTokenManager()

    def remember_enter(atomic):
        name = "default" if atomic.using is None else atomic.using
        connections[name].atomic_blocks.append(atomic)
        connections[name].in_atomic_block = True

    manager.original_atomic_enter = remember_enter
    default = _Conn()
    other = _Conn()
    other.alias = "other"
    connections["default"] = default
    connections["other"] = other
    db_token = DBToken({})
    db_token._token = "jwt"
    db_token._expiration = time.time() + 3600

    try:
        manager.set(db_token, "default")
        manager.set(db_token, "default")
        assert len(default.execute_wrappers) == 1
        uid = f"dbtokenmanager:{id(manager)}:default"
        fresh = _Conn()
        receivers[uid](sender=None, connection=fresh)
        assert fresh.execute_wrappers == default.execute_wrappers
        receivers[uid](sender=None, connection=fresh)
        assert len(fresh.execute_wrappers) == 1
        skipped = _Conn()
        skipped.alias = "other"
        receivers[uid](sender=None, connection=skipped)
        assert skipped.execute_wrappers == []

        wrapper = default.execute_wrappers[0]

        def run(result, sql="SELECT 1", params=None, many=False, cursor=None):
            return wrapper(
                lambda *_args: result,
                sql,
                params,
                many,
                _context(default, cursor),
            )

        default.in_atomic_block = True
        assert run("passed") == "passed"

        default.in_atomic_block = False
        default._lamin_in_token_tx = True
        assert run("joined") == "joined"
        default._lamin_in_token_tx = False
        default.autocommit = False
        assert run("manual") == "manual"
        assert default.connection.server_statements == [
            ("SELECT set_token(%s, true)", ("jwt",))
        ]

        default.autocommit = True

        class _Batch:
            def __init__(self):
                self.rowcount = 1
                self._rowcount = 1
                self.calls = 0

            def execute(self, _sql, _params):
                self.calls += 1

        batch = _Batch()
        assert (
            run(None, sql="INSERT", params=[(1,), (1,)], many=True, cursor=batch)
            is None
        )
        assert batch.calls == 2
        assert batch._rowcount == 2
        assert run("auto") == "auto"
        assert default.connection.commits == 1
        assert default.autocommit is True
        assert default.in_atomic_block is False

        live_atomic.__enter__(live_atomic(None, savepoint=True, durable=False))
        assert ("SELECT set_token(%s, true)", ("jwt",)) in (
            default.connection.server_statements
        )

        nested = _Conn()
        connections["default"] = nested
        nested.atomic_blocks.append(object())
        live_atomic.__enter__(live_atomic("default", savepoint=True, durable=False))
        assert not hasattr(nested.connection, "server_statements")

        manager.set(db_token, "other")
        live_atomic.__enter__(live_atomic("other", savepoint=True, durable=False))
        assert other.connection.server_statements == [
            ("SELECT set_token(%s, true)", ("jwt",))
        ]
        plain = _Conn()
        connections["plain"] = plain
        live_atomic.__enter__(live_atomic("plain", savepoint=True, durable=False))
        assert plain.atomic_blocks
        assert not hasattr(plain.connection, "server_statements")

        def fail_set(_connection, _token_value):
            raise RuntimeError("bad token")

        monkeypatch.setattr(_db_token, "_execute_set_token", fail_set)
        quiet = _Conn()
        connections["default"] = quiet
        atomic_quiet = live_atomic(None, savepoint=True, durable=False)
        atomic_quiet.__exit__ = lambda *_args: None
        with pytest.raises(RuntimeError, match="bad token"):
            live_atomic.__enter__(atomic_quiet)
        assert quiet.needs_rollback is True
        assert quiet.connection is not None

        exploding = _Conn()
        connections["default"] = exploding

        def explode_exit(_exc_type, _exc, _tb):
            raise RuntimeError("exit failed")

        atomic_fail = live_atomic(None, savepoint=True, durable=False)
        atomic_fail.__exit__ = explode_exit
        with pytest.raises(RuntimeError, match="bad token"):
            live_atomic.__enter__(atomic_fail)
        assert exploding.connection is None

        connections["default"] = default
        manager.original_atomic_enter = original_enter
        manager.reset("missing")
        assert "default" in manager.tokens
        manager.reset("default")
        assert default.execute_wrappers == []
        assert manager.atomic_is_patched is True
        manager.reset("other")
        assert other.execute_wrappers == []
        assert manager.tokens == {}
        assert manager.atomic_is_patched is False
        assert live_atomic.__enter__ is original_enter
    finally:
        manager.original_atomic_enter = original_enter
        for name in ("default", "other"):
            if name not in manager.tokens:
                continue
            connections.setdefault(name, _Conn())
            manager.reset(name)
        live_atomic.__enter__ = original_enter
