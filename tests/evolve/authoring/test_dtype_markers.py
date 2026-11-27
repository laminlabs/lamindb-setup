# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="misc,var-annotated"
# Source test semantics retained.
"""The `num` / `path` / `url` dtype markers, in every documented form."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

import pytest
from lamindb_setup.evolve import (
    DType,
    Feature,
    LaminDType,
    ListDType,
    SchemaConfig,
    SchemaModel,
    feature,
    get_dtype,
    num,
    path,
    url,
)
from lamindb_setup.evolve.engine._apply import ApplyContext
from lamindb_setup.evolve.engine._lock import serialize_config

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType

MARKERS = pytest.mark.parametrize(
    ("marker", "name"), [(num, "num"), (path, "path"), (url, "url")]
)


@MARKERS
def test_lamin_dtype_accepts_the_marker_class(marker: type, name: str) -> None:
    by_class = LaminDType(marker)
    by_string = LaminDType(name)  # type: ignore[call-overload]

    assert by_class.dtype == by_string.dtype == name
    assert by_class.dtype_str == by_string.dtype_str == name
    assert by_class.cat_filters is None
    # Stored as the string, so nothing downstream ever sees the marker class.
    assert by_class._spec == ("raw", name)


@MARKERS
def test_lamin_dtype_rejects_cat_filters_on_the_marker(marker: type, name: str) -> None:
    with pytest.raises(ValueError, match=f"'{name}'"):
        LaminDType(marker, source__uid="x")


@MARKERS
def test_lamin_dtype_accepts_a_list_of_the_marker(marker: type, name: str) -> None:
    assert LaminDType(list[marker]).dtype_str == f"list[{name}]"  # type: ignore[valid-type]


@MARKERS
def test_get_dtype_resolves_every_form(marker: type, name: str) -> None:
    assert get_dtype(DType[marker]).dtype == name  # type: ignore[valid-type]
    assert get_dtype(marker).dtype == name
    assert get_dtype(ListDType[marker]).dtype == f"list[{name}]"  # type: ignore[valid-type]


def test_schema_model_accepts_every_marker_form() -> None:
    class Markers(SchemaModel):
        schema_config = SchemaConfig(name="MarkerForms")
        a: Feature[DType[num]]
        b: Feature[DType[path]]
        c: Feature[DType[url]]
        d: Feature[num]
        e: Feature[path]
        f: Feature[url]
        g: Feature[ListDType[num]]
        h: Feature[list[url]]
        i: Feature[DType[path] | None]
        j: Feature[url] | None

    dtypes = {f.name: f.dtype.dtype_str for f in Markers.schema.features}
    assert dtypes == {
        "a": "num",
        "b": "path",
        "c": "url",
        "d": "num",
        "e": "path",
        "f": "url",
        "g": "list[num]",
        "h": "list[url]",
        "i": "path",
        "j": "url",
    }
    assert Markers.i.nullable is True
    assert Markers.schema.optionals == [Markers.j]


def test_feature_override_matches_the_marker_annotation() -> None:
    class Override(SchemaModel):
        schema_config = SchemaConfig(name="MarkerOverride")
        a: Feature[DType[num]] = feature(dtype=LaminDType(num), unit="mg")

    assert Override.a.dtype.dtype_str == "num"
    assert Override.a.unit == "mg"


@pytest.mark.parametrize(
    ("dtype", "wrapped"),
    [
        (num, "LaminDType(num)"),
        (path, "LaminDType(path)"),
        (url, "LaminDType(url)"),
        (int, "LaminDType(int)"),
        ("num", "LaminDType('num')"),
    ],
)
def test_feature_rejects_an_unwrapped_dtype_where_it_is_written(
    dtype: Any, wrapped: str
) -> None:
    with pytest.raises(TypeError, match=re.escape(f"wrap it: dtype={wrapped}")):
        Feature("x", dtype=dtype)
    with pytest.raises(TypeError, match=re.escape(f"wrap it: dtype={wrapped}")):
        feature(dtype=dtype)


@MARKERS
def test_the_lock_records_the_dtype_string_not_a_module_path(
    marker: type, name: str
) -> None:
    for dtype in (LaminDType(marker), get_dtype(DType[marker])):  # type: ignore[valid-type]
        config = serialize_config(Feature("x", dtype=dtype))

        assert config["dtype"] == name
        assert "lamindb_setup.evolve" not in json.dumps(config)


def test_markers_materialize_on_an_instance(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    class Materialized(SchemaModel):
        schema_config = SchemaConfig(name="MarkerMaterialized")
        amount: Feature[DType[num]]
        location: Feature[path]
        homepage: Feature[DType[url]]
        links: Feature[ListDType[url]]

    Materialized.schema.materialize(ApplyContext.standalone())

    stored = {
        f.name: f.dtype_as_str
        for f in ln.Feature.filter(name__in=["amount", "location", "homepage", "links"])
    }
    assert stored == {
        "amount": "num",
        "location": "path",
        "homepage": "url",
        "links": "list[url]",
    }
