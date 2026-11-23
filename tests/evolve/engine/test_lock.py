# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="call-overload,misc,var-annotated"
# Source test semantics retained.
# ruff: noqa: TC003
"""Tests for lamindb_setup.evolve.engine._lock."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from lamindb_setup.evolve import Feature, FeatureType, LaminDType, Schema
from lamindb_setup.evolve.authoring._dtypes import DType, get_dtype
from lamindb_setup.evolve.engine import _lock as lock_mod
from lamindb_setup.evolve.engine._lock import (
    LOCK_VERSION,
    LockEntry,
    LockFile,
    natural_key,
    remote_state_hash,
    serialize_config,
)


def test_natural_key_is_stable_across_equal_objects() -> None:
    ft = FeatureType("ft")
    f1 = Feature("col", dtype=LaminDType(str), type=ft)
    f2 = Feature("col", dtype=LaminDType(str), type=ft)
    assert natural_key(f1) == natural_key(f2) == "FeatureType:ft/Feature:col"


def test_natural_key_includes_parent_chain() -> None:
    ft = FeatureType("ft")
    f = Feature("col", dtype=LaminDType(str), type=ft)
    assert natural_key(ft) == "FeatureType:ft"
    assert natural_key(f) == "FeatureType:ft/Feature:col"


def test_unnamed_schema_natural_key_uses_config_hash() -> None:
    f = Feature("col", dtype=LaminDType(str))
    schema = Schema(features=[f])
    key = natural_key(schema)
    assert key.startswith("Schema:#")
    # The hash is stable for an identical configuration.
    f2 = Feature("col", dtype=LaminDType(str))
    assert natural_key(Schema(features=[f2])) == key


def test_serialize_config_feature() -> None:
    ft = FeatureType("ft")
    f = Feature("col", dtype=LaminDType(str), type=ft, nullable=True, unit="mg")
    cfg = serialize_config(f)
    assert cfg["kind"] == "Feature"
    assert cfg["name"] == "col"
    assert cfg["dtype"] == "builtins.str"
    assert cfg["type"] == "FeatureType:ft"
    assert cfg["nullable"] is True
    assert cfg["unit"] == "mg"


def test_serialize_config_resolved_dtype_is_not_double_quoted() -> None:
    # A declarative feature carries a ResolvedDType whose `.dtype` is already
    # the canonical lamindb string. serialize_config must record that string
    # verbatim, not wrap it in `repr()` -- otherwise the lock stores a quoted
    # `'int'` that never equals the instance's clean `int` and defeats the
    # stale-lock reconciliation, surfacing as spurious configuration drift.
    f = Feature("col", dtype=get_dtype(DType[int]))
    cfg = serialize_config(f)
    assert cfg["dtype"] == "int"


@pytest.mark.parametrize(
    "dtype,expected",
    [(list[str], "list[str]"), (list[int], "list[int]"), (list[float], "list[float]")],
)
def test_serialize_config_records_a_list_element_type(
    dtype: object, expected: str
) -> None:
    # `list[str]` is a GenericAlias whose `__name__` is a bare `list`, so
    # every list dtype used to be recorded as just that.
    assert serialize_config(Feature("col", dtype=LaminDType(dtype)))["dtype"] == (
        expected
    )


def test_serialize_config_schema_references_features_by_key() -> None:
    ft = FeatureType("ft")
    f = Feature("col", dtype=LaminDType(str), type=ft)
    schema = Schema(name="S", features=[f])
    cfg = serialize_config(schema)
    assert cfg["kind"] == "Schema"
    assert cfg["features"] == ["FeatureType:ft/Feature:col"]
    assert "optionals" not in cfg


def test_serialize_config_schema_records_optionals() -> None:
    ft = FeatureType("ft")
    required = Feature("req", dtype=LaminDType(str), type=ft)
    optional = Feature("opt", dtype=LaminDType(str), type=ft)
    schema = Schema(name="S", features=[required, optional], optionals=[optional])
    cfg = serialize_config(schema)
    assert cfg["optionals"] == ["FeatureType:ft/Feature:opt"]


def test_lockfile_roundtrip(tmp_path: Path) -> None:
    lock = LockFile(plan="p")
    lock.add(
        LockEntry(
            key="FeatureType:ft",
            kind="FeatureType",
            name="ft",
            uid="abc123456789",
            config={"kind": "FeatureType", "name": "ft"},
        )
    )
    path = tmp_path / "p.lock"
    lock.write(path)

    loaded = LockFile.read(path)
    assert loaded is not None
    assert loaded.plan == "p"
    assert loaded.uid_for("FeatureType:ft") == "abc123456789"
    entry = loaded.entry_for("FeatureType:ft")
    assert entry is not None
    assert entry.config["name"] == "ft"


def test_lockfile_read_missing_returns_none(tmp_path: Path) -> None:
    assert LockFile.read(tmp_path / "does_not_exist.lock") is None


def test_lockfile_remove_drops_entry_and_reports_presence() -> None:
    lock = LockFile(plan="p")
    lock.add(
        LockEntry(
            key="FeatureType:ft",
            kind="FeatureType",
            name="ft",
            uid="abc123456789",
        )
    )

    assert lock.remove("FeatureType:ft") is True
    assert lock.entry_for("FeatureType:ft") is None
    # Removing an absent key is a no-op that reports nothing was present.
    assert lock.remove("FeatureType:ft") is False


def test_lockentry_roundtrips_remote_snapshot_and_hash(tmp_path: Path) -> None:
    snapshot = {"kind": "Feature", "name": "col", "dtype": "str"}
    lock = LockFile(plan="p")
    lock.add(
        LockEntry(
            key="FeatureType:ft/Feature:col",
            kind="Feature",
            name="col",
            uid="abc123456789",
            config={"kind": "Feature", "name": "col"},
            remote_snapshot=snapshot,
            remote_hash=remote_state_hash(snapshot),
        )
    )
    path = tmp_path / "p.lock"
    lock.write(path)

    loaded = LockFile.read(path)
    assert loaded is not None
    assert loaded.version == LOCK_VERSION
    entry = loaded.entry_for("FeatureType:ft/Feature:col")
    assert entry is not None
    assert entry.remote_snapshot == snapshot
    assert entry.remote_hash == remote_state_hash(snapshot)


@pytest.mark.parametrize("fields", [{}, {"version": 5}, {"version": LOCK_VERSION + 1}])
def test_read_rejects_unsupported_or_missing_version(
    tmp_path: Path, fields: dict
) -> None:
    from lamindb_setup.evolve.engine._lock import LockFileError

    path = tmp_path / "plan.lock"
    path.write_text(json.dumps({"plan": "p", "objects": {}, **fields}))
    with pytest.raises(LockFileError, match="Unsupported lock version"):
        LockFile.read(path)


def test_remote_state_hash_is_order_independent() -> None:
    a = {"kind": "Feature", "name": "col", "dtype": "str"}
    b = {"dtype": "str", "name": "col", "kind": "Feature"}
    assert remote_state_hash(a) == remote_state_hash(b)


def test_lockfile_records_package_version(tmp_path: Path) -> None:
    from importlib.metadata import version

    lock = LockFile(plan="p")
    assert lock.lamindb_version == version("lamindb-core")

    path = tmp_path / "p.lock"
    lock.write(path)

    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["lamindb_version"] == version("lamindb-core")

    loaded = LockFile.read(path)
    assert loaded is not None
    assert loaded.lamindb_version == version("lamindb-core")


def test_write_creates_parent_directory_and_leaves_no_temp_file(
    tmp_path: Path,
) -> None:
    path = tmp_path / "nested" / "p.lock"
    LockFile(plan="p").write(path)

    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8"))["plan"] == "p"
    # The atomic write must not leave its temporary file behind.
    siblings = [p.name for p in path.parent.iterdir() if p.name != path.name]
    assert siblings == []


def test_write_failure_preserves_existing_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "p.lock"
    LockFile(plan="original").write(path)
    original = path.read_text(encoding="utf-8")

    def _boom(src: object, dst: object) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(lock_mod.os, "replace", _boom)

    with pytest.raises(OSError, match="replace failed"):
        LockFile(plan="updated").write(path)

    # The original lock is untouched and no temp file is left behind.
    assert path.read_text(encoding="utf-8") == original
    siblings = [p.name for p in path.parent.iterdir() if p.name != path.name]
    assert siblings == []


def test_read_reports_a_corrupt_lock_instead_of_a_json_error(tmp_path: Path) -> None:
    from lamindb_setup.evolve.engine._lock import LockFileError

    path = tmp_path / "plan.lock"
    path.write_text('{"plan": "p", "version": 5, "objec')

    with pytest.raises(LockFileError, match="not readable as a lock file"):
        LockFile.read(path)


def test_read_reports_a_lock_that_is_not_an_object(tmp_path: Path) -> None:
    from lamindb_setup.evolve.engine._lock import LockFileError

    path = tmp_path / "plan.lock"
    path.write_text("[1, 2, 3]")

    with pytest.raises(LockFileError, match="does not contain a lock object"):
        LockFile.read(path)


def test_read_reports_a_malformed_entry_by_file_name(tmp_path: Path) -> None:
    from lamindb_setup.evolve.engine._lock import LOCK_VERSION, LockFileError

    path = tmp_path / "plan.lock"
    path.write_text(
        json.dumps(
            {"plan": "p", "version": LOCK_VERSION, "objects": {"Feature:x": {"n": 1}}}
        )
    )

    with pytest.raises(LockFileError, match="malformed entry"):
        LockFile.read(path)


def test_a_current_lock_still_reads(tmp_path: Path) -> None:
    from lamindb_setup.evolve.engine._lock import LOCK_VERSION

    path = tmp_path / "plan.lock"
    path.write_text(json.dumps({"plan": "p", "version": LOCK_VERSION, "objects": {}}))

    lock = LockFile.read(path)

    assert lock is not None
    assert lock.version == LOCK_VERSION
    assert LockFile.read(tmp_path / "absent.lock") is None


@pytest.mark.parametrize("kind", ["RecordType", "Sheet"])
def test_a_record_types_schema_is_snapshotted_whatever_the_plan_calls_it(
    kind: str,
) -> None:
    # The plan's kind says nothing about whether the record has a schema;
    # `None` is recorded too, telling "none" apart from a legacy snapshot.
    from types import SimpleNamespace

    from lamindb_setup.evolve.engine._lock import serialize_existing

    def record(schema: object) -> object:
        return SimpleNamespace(
            name="X", type=None, space=None, description=None, _aux={}, schema=schema
        )

    with_schema = record(SimpleNamespace(uid="schemaUID0000001"))
    assert serialize_existing(with_schema, kind)["schema"] == "schemaUID0000001"
    assert serialize_existing(record(None), kind)["schema"] is None
