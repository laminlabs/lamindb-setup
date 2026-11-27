"""Build a render-agnostic `SummaryModel` from a reconstructed plan.

Lets drift detection present the untracked objects it reconstructs in the same
grouped, type-nested layout the `validate` summary uses -- without re-loading
the generated plan file, which would trip the lamindb-not-loaded guard once the
instance is connected.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..render import ObjectInfo, PlanRef, SummaryModel

if TYPE_CHECKING:
    from ._reverse import DtypeInfo, ObjectSpec, ReconstructedPlan

__all__ = ["build_reconstructed_summary"]


def _dtype_label(dtype: DtypeInfo) -> str:
    """Render a reconstructed feature dtype as a compact, readable string."""
    if dtype.union_members:
        inner = "cat[{}]".format(
            "|".join(
                f"{reg}.{fld}" if fld is not None else reg
                for reg, fld in dtype.union_members
            )
        )
    elif dtype.simple_py is not None:
        inner = dtype.simple_py
    elif dtype.registry_expr is not None:
        inner = dtype.registry_expr
        if dtype.field_name:
            inner = f"{inner}.{dtype.field_name}"
    else:
        inner = "?"
    return f"list[{inner}]" if dtype.is_list else inner


def _spec_details(spec: ObjectSpec) -> list[str]:
    """Return compact `key=value` fragments describing a reconstructed object.

    Relational attributes already conveyed by the rendered layout are omitted:
    the `type` parent (shown via nesting) and bound schema/features (shown via
    `requires`); only counts and scalar configuration are surfaced here.
    """
    payload = spec.payload
    details: list[str] = []
    dtype = payload.get("dtype")
    if dtype is not None:
        details.append(f"dtype={_dtype_label(dtype)}")
    otype = payload.get("otype")
    if otype:
        details.append(f"otype={otype!r}")
    feature_uids = payload.get("feature_uids")
    if feature_uids:
        details.append(f"features={len(feature_uids)}")
    slots = payload.get("slots")
    if slots:
        details.append(f"slots={len(slots)}")
    if payload.get("nullable") is False:
        details.append("nullable=False")
    return details


def build_reconstructed_summary(
    plan: ReconstructedPlan, *, title: str, name: str
) -> SummaryModel:
    """Build a :class:`SummaryModel` from a reconstructed plan's object specs.

    Objects nest under their `type` parent (so feature, schema and record types
    own their members) and reference their other dependencies via `requires`,
    matching the layout produced for an in-memory plan by
    :func:`~lamindb_setup.evolve.render.build_summary_model`.
    """
    id_by_uid = {spec.uid: index for index, spec in enumerate(plan.specs)}
    objects: list[ObjectInfo] = []
    for index, spec in enumerate(plan.specs):
        type_uid = spec.payload.get("type_uid")
        parent_id = id_by_uid.get(type_uid) if type_uid is not None else None
        requires = sorted(
            id_by_uid[dep] for dep in spec.deps if dep in id_by_uid and dep != type_uid
        )
        objects.append(
            ObjectInfo(
                obj_id=index,
                kind=spec.kind,
                name=spec.name,
                details=_spec_details(spec),
                requires=requires,
                parent_id=parent_id,
            )
        )
    return SummaryModel(
        title=title,
        plan=PlanRef(name=name, default_space=None),
        objects=objects,
    )
