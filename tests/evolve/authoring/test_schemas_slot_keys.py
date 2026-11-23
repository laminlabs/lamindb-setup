# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="arg-type"
"""Slot-key rules for composite schemas.

:func:`~lamindb_setup.evolve.authoring._schemas.validate_slot_keys` is a copy of rules that
really live in `lamindb.curators.core`, so it goes stale whenever lamindb moves.
The table below is the single source of truth for both halves of that: the unit
tests assert what our validator does, and the parity tests replay the same keys
through the real curators.  A dependabot bump of lamindb therefore fails here as
soon as the two disagree, instead of failing in a user's plan.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from lamindb_setup.evolve.authoring._schemas import validate_slot_keys

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any
    from typing import Any as LamindbModuleType


# (otype, slot key, accepted by validate_slot_keys)
SLOT_CASES: list[tuple[str, str, bool]] = [
    # -- DataFrame ----------------------------------------------------------
    ("DataFrame", "attrs", True),
    ("DataFrame", "attrs:study", True),
    ("DataFrame", "attrs:study:nested", True),
    ("DataFrame", "__external__", True),
    ("DataFrame", "obs", False),
    ("DataFrame", "attrs:", False),
    ("DataFrame", "attrsstudy", False),
    # -- AnnData ------------------------------------------------------------
    ("AnnData", "obs", True),
    ("AnnData", "var", True),
    ("AnnData", "var.T", True),
    ("AnnData", "uns", True),
    ("AnnData", "uns:study", True),
    ("AnnData", "uns:study:nested", True),
    ("AnnData", "attrs", False),
    ("AnnData", "unsstudy", False),
    ("AnnData", "rna:obs", False),
    ("AnnData", "__external__", True),
    # -- MuData -------------------------------------------------------------
    # MuData top-level slots.
    ("MuData", "obs", True),
    ("MuData", "var", True),
    ("MuData", "var.T", True),
    ("MuData", "uns", True),
    ("MuData", "uns:study", True),
    ("MuData", "uns:study:nested", True),
    ("MuData", "rna:obs", True),
    ("MuData", "rna:var", True),
    ("MuData", "rna:var.T", True),
    ("MuData", "rna:uns", True),
    ("MuData", "rna:uns:study", True),
    ("MuData", "unstim:obs", True),
    ("MuData", "__external__", True),
    ("MuData", ":obs", False),
    ("MuData", "rna:obsm", False),
    ("MuData", "rna", False),
    # -- SpatialData --------------------------------------------------------
    ("SpatialData", "attrs", True),
    ("SpatialData", "attrs:study", True),
    ("SpatialData", "attrs:study:nested", True),
    ("SpatialData", "tables:table1:obs", True),
    ("SpatialData", "tables:table1:var", True),
    ("SpatialData", "tables:table1:var.T", True),
    ("SpatialData", "__external__", True),
    ("SpatialData", "tables::obs", False),
    ("SpatialData", "tables:table1:uns", False),
    ("SpatialData", "table1:obs", False),
    ("SpatialData", "study", False),
]

# Keys where we knowingly disagree with the curator.  Each entry says what the
# curator does with the key and why we do something else; a bump that removes
# the disagreement fails the parity test, which is the point.
KNOWN_DIVERGENCES: dict[tuple[str, str], str] = {
    ("DataFrame", "attrsstudy"): (
        "DataFrameCurator routes on `slot.startswith('attrs')`, so a key that "
        "merely begins with 'attrs' silently curates the whole attrs dict. We "
        "reject it -- it is a typo, not a slot."
    ),
    ("MuData", "unstim:obs"): (
        "MuDataCurator routes on `'uns' in slot`, so a modality whose *name* "
        "contains 'uns' is mistaken for a uns dict path and rejected upstream. "
        "We accept it rather than mirror that bug."
    ),
    ("SpatialData", "study"): (
        "SpatialDataCurator still reads a bare key off `sdata.attrs`, but warns "
        "to prefix it with 'attrs:'. We reject it so a typo'd slot cannot pass "
        "as an attrs lookup."
    ),
}


def _slots(*keys: str) -> dict[str, Any]:
    """Slot mapping with stand-in values -- `validate_slot_keys` reads only keys."""
    return {key: object() for key in keys}


@pytest.mark.parametrize(("otype", "key", "accepted"), SLOT_CASES)
def test_validate_slot_keys(otype: str, key: str, accepted: bool) -> None:
    slots = _slots(key)
    if accepted:
        validate_slot_keys(otype, slots)
    else:
        with pytest.raises(ValueError, match="is not valid for otype"):
            validate_slot_keys(otype, slots)


def test_validate_slot_keys_rejects_non_dict() -> None:
    not_a_dict = cast("dict[str, Any]", ["obs"])
    with pytest.raises(ValueError, match="Slots must be a dictionary"):
        validate_slot_keys("DataFrame", not_a_dict)


def test_error_message_lists_the_accepted_forms() -> None:
    slots = _slots("nope")
    with pytest.raises(ValueError, match=r"'<modality>:uns:<key>\[:<key>\.\.\.\]'"):
        validate_slot_keys("MuData", slots)


def test_schema_accepts_top_level_mudata_slots() -> None:
    """The issue's reproducer: a global `obs` next to per-modality slots."""
    from lamindb_setup.evolve import Feature, LaminDType, Schema

    sample = Schema(
        name="Sample", features=[Feature(name="sample", dtype=LaminDType(str))]
    )
    composite = Schema(
        name="MuDataComposite",
        features=[],
        otype="MuData",
        slots={
            "obs": sample,
            "rna:obs": sample,
            "rna:var.T": sample,
            "uns:experiment": sample,
        },
    )
    assert set(composite.slots or {}) == {
        "obs",
        "rna:obs",
        "rna:var.T",
        "uns:experiment",
    }


# --- parity with the real curators ------------------------------------------


def _nested() -> dict[str, Any]:
    return {"nested": {"sample": "a"}, "sample": "a"}


def _dataframe() -> Any:
    import pandas as pd

    df = pd.DataFrame({"sample": ["a", "b"]})
    df.attrs["study"] = _nested()
    return df


def _anndata(prefix: str = "g") -> Any:
    import anndata as ad
    import numpy as np
    import pandas as pd

    adata = ad.AnnData(
        X=np.zeros((2, 2), dtype="float32"),
        obs=pd.DataFrame({"sample": ["a", "b"]}, index=["c1", "c2"]),
        var=pd.DataFrame({"sample": ["a", "b"]}, index=[f"{prefix}1", f"{prefix}2"]),
    )
    adata.uns["study"] = _nested()
    adata.uns["sample"] = "a"
    return adata


def _mudata() -> Any:
    import mudata as md

    mdata = md.MuData({"rna": _anndata("rna"), "unstim": _anndata("unstim")})
    mdata.uns["study"] = _nested()
    mdata.uns["sample"] = "a"
    return mdata


def _spatialdata() -> Any:
    from spatialdata import SpatialData
    from spatialdata.models import TableModel

    sdata = SpatialData(tables={"table1": TableModel.parse(_anndata())})
    sdata.attrs["study"] = _nested()
    sdata.attrs["sample"] = "a"
    return sdata


def _curator_accepts(
    ln: LamindbModuleType,
    otype: str,
    key: str,
    dataset: Any,
    component: Any,
    features: list[Any],
) -> str | None:
    """Return `None` if the curator resolves `key`, else the error it raised."""
    curators = {
        "DataFrame": ln.curators.DataFrameCurator,
        "AnnData": ln.curators.AnnDataCurator,
        "MuData": ln.curators.MuDataCurator,
        "SpatialData": ln.curators.SpatialDataCurator,
    }
    # DataFrameCurator curates the frame itself against the composite schema, so
    # that one needs members; the scverse curators only dispatch on the slots.
    schema = ln.Schema(
        otype=otype,
        features=features if otype == "DataFrame" else [],
        slots={key: component},
    ).save()
    try:
        curators[otype](dataset, schema)
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


def _check_parity(ln: LamindbModuleType, otype: str, dataset: Any) -> None:
    features = [ln.Feature(name="sample", dtype=str).save()]
    component = ln.Schema(name=f"{otype}Component", features=features).save()

    mismatches = []
    for case_otype, key, accepted in SLOT_CASES:
        if case_otype != otype:
            continue
        error = _curator_accepts(ln, otype, key, dataset, component, features)
        expected = accepted != ((otype, key) in KNOWN_DIVERGENCES)
        if (error is None) is not expected:
            verb = "accepts" if error is None else f"rejects ({error})"
            mismatches.append(
                f"{otype} slot {key!r}: validate_slot_keys "
                f"{'accepts' if accepted else 'rejects'} it, but the curator {verb}"
            )
    assert not mismatches, "\n".join(mismatches)


def test_dataframe_slot_key_parity(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    _check_parity(ln, "DataFrame", _dataframe())


def test_anndata_slot_key_parity(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    pytest.importorskip("anndata")
    _check_parity(ln, "AnnData", _anndata())


def test_mudata_slot_key_parity(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    pytest.importorskip("mudata")
    _check_parity(ln, "MuData", _mudata())


def test_spatialdata_slot_key_parity(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    pytest.importorskip("spatialdata")
    _check_parity(ln, "SpatialData", _spatialdata())
