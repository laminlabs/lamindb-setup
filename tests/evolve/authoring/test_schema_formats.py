"""`Schema(formats=...)`: lamindb's format constraints, checked on curation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from lamindb_setup.evolve import Feature, LaminDType, Schema
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
from lamindb_setup.evolve.engine._apply import (
    Action,
    ApplyMode,
    compute_changes,
    write_lock,
)
from lamindb_setup.evolve.engine._lock import LockFile
from lamindb_setup.evolve.introspect import introspect_instance

if TYPE_CHECKING:
    from pathlib import Path
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType

_ZARR: dict[str, Any] = {
    "zarr_format": 3,
    "chunk_shape": {"y": (256, 512), "x": (256, 512)},
    "multiscales": {"scale": 2, "axes": ("y", "x")},
    "layers": {"csc": "csc_matrix"},
}
# as lamindb reads it back from `_aux`: tuples become lists
_ZARR_JSON: dict[str, Any] = {
    "zarr_format": 3,
    "chunk_shape": {"y": [256, 512], "x": [256, 512]},
    "multiscales": {"scale": 2, "axes": ["y", "x"]},
    "layers": {"csc": "csc_matrix"},
}


def _spatial(formats: Any, otype: Any = "SpatialData") -> Schema:
    obs = Schema(name="FmtObs", features=[Feature("fmt_x", dtype=LaminDType(str))])
    slots = {"tables:table:obs": obs} if otype == "SpatialData" else {"obs": obs}
    return Schema(
        name="FmtSpatial", features=[], otype=otype, slots=slots, formats=formats
    )


@create_plan
def _plan(ctx: PlanContext, exports: None) -> None:
    _spatial({"zarr": _ZARR})


@create_plan
def _scale4_plan(ctx: PlanContext, exports: None) -> None:
    _spatial({"zarr": {**_ZARR, "multiscales": {"scale": 4}}})


@create_plan
def _no_formats_plan(ctx: PlanContext, exports: None) -> None:
    _spatial(None)


def _lock(ctx: Any, plan: Any, path: Path) -> LockFile:
    write_lock(ctx, plan, path)
    lock = LockFile.read(path)
    assert lock is not None
    return lock


def _action(ctx: Any) -> Action:
    return next(c for c in ctx.changes if c.name == "FmtSpatial").action


def test_formats_are_normalized_to_json() -> None:
    assert _spatial({"zarr": _ZARR}).formats == {"zarr": _ZARR_JSON}


@pytest.mark.parametrize(
    ("formats", "otype", "message"),
    [
        ({"h5ad": {}}, "SpatialData", "unknown format 'h5ad'"),
        ({"zarr": {"zarr_fmt": 3}}, "SpatialData", "unknown check(s) ['zarr_fmt']"),
        ({"zarr": {"zarr_format": 3}}, "AnnData", "only enforced for"),
    ],
)
def test_invalid_formats_are_rejected(formats: Any, otype: str, message: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        _spatial(formats, otype)
    assert message in str(excinfo.value)


def test_apply_creates_updates_and_clears_formats(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    created = compute_changes(_plan, mode=ApplyMode.APPLY, lock=None)
    assert _action(created) is Action.CREATE
    assert ln.Schema.get(name="FmtSpatial").formats.zarr == _ZARR_JSON
    lock = _lock(created, _plan, tmp_path / "a.lock")
    assert lock.entry_for("Schema:FmtSpatial").config["formats"] == {  # type: ignore[union-attr]
        "zarr": _ZARR_JSON
    }
    again = compute_changes(_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in again.changes} == {Action.NO_CHANGE}

    before = ln.Schema.get(name="FmtSpatial")
    edited = compute_changes(_scale4_plan, mode=ApplyMode.APPLY, lock=lock)
    assert _action(edited) is Action.UPDATE
    after = ln.Schema.get(name="FmtSpatial")
    assert after.formats.zarr["multiscales"] == {"scale": 4}
    # not part of the content hash: the same record, updated in place
    assert (after.uid, after.hash) == (before.uid, before.hash)

    lock = _lock(edited, _scale4_plan, tmp_path / "b.lock")
    cleared = compute_changes(_no_formats_plan, mode=ApplyMode.APPLY, lock=lock)
    assert _action(cleared) is Action.UPDATE
    assert ln.Schema.get(name="FmtSpatial").formats.zarr is None


def test_an_edit_on_the_instance_is_flagged_and_reverted(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _lock(applied, _plan, tmp_path / "a.lock")
    schema = ln.Schema.get(name="FmtSpatial")
    schema.formats.zarr = {"zarr_format": 2}
    schema.save()

    ctx = compute_changes(_plan, mode=ApplyMode.APPLY, lock=lock)

    [change] = [c for c in ctx.changes if c.name == "FmtSpatial"]
    assert change.action is Action.UPDATE
    assert any(
        "remote changed outside the plan" in w and "formats" in w
        for w in change.warnings
    )
    assert ln.Schema.get(name="FmtSpatial").formats.zarr == _ZARR_JSON


def test_introspect_roundtrips_formats(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    compute_changes(_plan, mode=ApplyMode.APPLY, lock=None)

    output = tmp_path / "introspected.py"
    result = introspect_instance(instance.slug, output, name="introspected")

    assert result.ok, (result.errors, result.verification)
    assert result.actionable_changes == 0
    assert f"formats={ {'zarr': _ZARR_JSON}!r}" in output.read_text("utf-8")


def test_spatialdata_curator_enforces_the_applied_formats(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    pytest.importorskip("spatialdata")
    import anndata as ad
    import numpy as np
    from scipy.sparse import csc_matrix
    from spatialdata import SpatialData
    from spatialdata.models import Image2DModel, TableModel

    compute_changes(_plan, mode=ApplyMode.APPLY, lock=None)
    schema = ln.Schema.get(name="FmtSpatial")

    def sdata(*, conforming: bool) -> SpatialData:
        image = Image2DModel.parse(
            np.zeros((1, 1024, 1024), dtype=np.uint8),
            dims=("c", "y", "x"),
            chunks=(1, 256, 256),
            scale_factors=[2, 2] if conforming else None,
        )
        table = ad.AnnData(np.ones((3, 2), dtype=np.float32))
        table.obs["fmt_x"] = ["a", "b", "c"]
        if conforming:
            table.layers["csc"] = csc_matrix(table.X)
        return SpatialData(
            images={"image": image}, tables={"table": TableModel.parse(table)}
        )

    with pytest.raises(ln.errors.ValidationError) as excinfo:
        ln.curators.SpatialDataCurator(sdata(conforming=False), schema).validate()
    assert "is single-scale" in str(excinfo.value)
    assert "has no layer 'csc'" in str(excinfo.value)

    ln.curators.SpatialDataCurator(sdata(conforming=True), schema).validate()
