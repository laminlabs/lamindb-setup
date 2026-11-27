"""`Schema(slots={"__external__": ...})`: validate external metadata on any artifact.

files with no in-memory curator (FASTQ, BAM, images,
folders, ...) should still be uploaded against a schema that validates the
metadata passed as `features=`.  Since lamindb 2.12 a schema whose only slot is
`__external__` needs no `otype` and applies to any artifact, and the scverse
curators accept an `__external__` slot next to their content slots.
"""

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

_VALUES = {"si_sample_id": "S1", "si_tissue": "liver"}


def _external() -> Schema:
    return Schema(
        name="SIExternal",
        features=[
            Feature("si_sample_id", dtype=LaminDType(str)),
            Feature("si_tissue", dtype=LaminDType(str)),
        ],
        # reject metadata the external schema does not declare
        maximal_set=True,
    )


@create_plan
def _plan(ctx: PlanContext, exports: None) -> None:
    Schema(name="SIToBeCurated", features=[], slots={"__external__": _external()})


@create_plan
def _anndata_plan(ctx: PlanContext, exports: None) -> None:
    obs = Schema(name="SIObs", features=[Feature("si_obs", dtype=LaminDType(str))])
    Schema(
        name="SIAnnData",
        features=[],
        otype="AnnData",
        slots={"obs": obs, "__external__": _external()},
    )


def test_external_only_schema_needs_no_otype() -> None:
    schema = Schema(
        name="SIToBeCurated", features=[], slots={"__external__": _external()}
    )
    assert schema.otype is None
    assert set(schema.slots or {}) == {"__external__"}


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param(
            {"features": [Feature("si_x", dtype=LaminDType(str))]},
            id="members-next-to-external",
        ),
        pytest.param(
            {"features": [], "extra_slot": "obs"}, id="content-slot-next-to-external"
        ),
        pytest.param(
            {"features": [], "index": Feature("si_idx", dtype=LaminDType(str))},
            id="index-next-to-external",
        ),
    ],
)
def test_otype_is_still_required_when_the_schema_has_content(
    kwargs: dict[str, Any],
) -> None:
    slots = {"__external__": _external()}
    if extra := kwargs.pop("extra_slot", None):
        slots[extra] = _external()
    with pytest.raises(ValueError, match="otype"):
        Schema(name="SIBad", slots=slots, **kwargs)


def test_otype_is_still_required_next_to_an_itype(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    with pytest.raises(ValueError, match="otype"):
        Schema(
            name="SIBad",
            features=[],
            itype=ln.Feature,
            slots={"__external__": _external()},
        )


def test_apply_external_only_schema_validates_any_artifact(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    created = compute_changes(_plan, mode=ApplyMode.APPLY, lock=None)
    assert {c.action for c in created.changes} == {Action.CREATE}
    schema = ln.Schema.get(name="SIToBeCurated")
    assert schema.otype is None
    assert set(schema.slots) == {"__external__"}
    assert schema.slots["__external__"] == ln.Schema.get(name="SIExternal")

    lock_path = tmp_path / "plan.lock"
    write_lock(created, _plan, lock_path)
    lock = LockFile.read(lock_path)
    again = compute_changes(_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in again.changes} == {Action.NO_CHANGE}

    fastq = tmp_path / "reads.fastq.gz"
    fastq.write_bytes(b"@read1\nACGT\n+\n!!!!\n")
    key = "si/reads.fastq.gz"

    with pytest.raises(ln.errors.ValidationError, match="no external features"):
        ln.Artifact(fastq, key=key, schema=schema)
    with pytest.raises(ln.errors.ValidationError):
        ln.Artifact(fastq, key=key, features={**_VALUES, "si_tissue": 1}, schema=schema)
    ln.Feature(name="si_undeclared", dtype=str).save()
    with pytest.raises(ln.errors.ValidationError):
        ln.Artifact(
            fastq, key=key, features={**_VALUES, "si_undeclared": "x"}, schema=schema
        )

    artifact = ln.Artifact(fastq, key=key, features=_VALUES, schema=schema).save()
    assert artifact.schema == schema
    assert artifact.features.get_values() == _VALUES

    folder = tmp_path / "sample_folder"
    folder.mkdir()
    (folder / "image.tiff").write_bytes(b"not really a tiff")
    folder_artifact = ln.Artifact(
        folder, key="si/sample_folder", features=_VALUES, schema=schema
    ).save()
    assert folder_artifact.schema == schema
    assert folder_artifact.features.get_values() == _VALUES


def test_introspect_roundtrips_external_only_schema(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    compute_changes(_plan, mode=ApplyMode.APPLY, lock=None)

    output = tmp_path / "introspected.py"
    result = introspect_instance(instance.slug, output, name="introspected")

    assert result.ok, (result.errors, result.verification)
    assert result.actionable_changes == 0
    source = output.read_text("utf-8")
    [line] = [ln_ for ln_ in source.splitlines() if '"SIToBeCurated"' in ln_]
    assert 'slots={"__external__": ' in line
    assert "otype=" not in line


def test_anndata_schema_with_external_slot(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    pytest.importorskip("anndata")
    import anndata as ad
    import numpy as np
    import pandas as pd

    compute_changes(_anndata_plan, mode=ApplyMode.APPLY, lock=None)
    schema = ln.Schema.get(name="SIAnnData")
    assert set(schema.slots) == {"obs", "__external__"}

    adata = ad.AnnData(
        np.zeros((2, 2), dtype="float32"),
        obs=pd.DataFrame({"si_obs": ["a", "b"]}, index=["c1", "c2"]),
    )
    with pytest.raises(ln.errors.ValidationError, match="no external features"):
        ln.Artifact.from_anndata(adata, key="si/adata.h5ad", schema=schema)

    artifact = ln.Artifact.from_anndata(
        adata, key="si/adata.h5ad", schema=schema, features=_VALUES
    ).save()
    assert artifact.schema == schema
    assert artifact.features.get_values() == _VALUES
