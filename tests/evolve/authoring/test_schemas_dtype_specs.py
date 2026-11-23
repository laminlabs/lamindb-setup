"""Tests for FQN-based, reconnect-safe dtype/itype storage on Schema."""

from __future__ import annotations

from typing import TYPE_CHECKING

from lamindb_setup.evolve import Feature, LaminDType, Schema

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType


def test_schema_itype_is_stored_as_spec_not_class(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    """A dynamic schema keeps its `itype` as an FQN spec, not the live class."""
    schema = Schema(features=[], itype=ln.Feature, flexible=True)
    assert "itype" not in vars(schema)
    assert schema._itype_spec == ("class", ("lamindb.models.feature", "Feature"))
    # The property re-resolves the class against the connected instance.
    assert schema.itype is ln.Feature


def test_schema_itype_resolves_current_class_not_stale(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    """Building from a stale itype class still resolves to the live class."""
    stale = type("Feature", (), {"__module__": "lamindb.models.feature"})
    assert stale is not ln.Feature

    schema = Schema(features=[], itype=stale, flexible=True)
    assert schema._itype_spec == ("class", ("lamindb.models.feature", "Feature"))
    assert schema.itype is ln.Feature
    assert schema.itype is not stale


def test_schema_dtype_resolves_via_lamindtype(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    """`Schema.dtype` defers to its `LaminDType`, which re-resolves on access."""
    schema = Schema(features=[], itype=ln.Feature, dtype=LaminDType(ln.ULabel))
    assert "dtype" not in vars(schema)
    assert schema.dtype is ln.ULabel


def test_schema_without_itype_or_dtype(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    """A feature-based schema exposes `None` for itype and dtype."""
    feature = Feature("sample_id", dtype=LaminDType(str))
    schema = Schema("s", features=[feature])
    assert schema._itype_spec is None
    assert schema.itype is None
    assert schema.dtype is None
