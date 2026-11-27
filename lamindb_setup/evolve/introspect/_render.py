"""Render a reconstructed plan as `lamin evolve` plan source.

Takes the ordered :class:`~lamindb_setup.evolve.introspect._reverse.ObjectSpec`
list and emits a Python plan file equivalent to one written by hand: a module
header with exactly the imports it uses, a `@create_plan` body that recreates
every object, and the standard `__main__` summary footer.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..engine import PLAN_STEM_UID_VAR
from ._template import STEM_UID_COMMENT

if TYPE_CHECKING:
    from ._reverse import DtypeInfo, ObjectSpec, ReconstructedPlan

__all__ = ["render_source"]

_INDENT = "    "


@dataclass(frozen=True)
class _ExternalPlan:
    """An owning plan that the rendered plan imports for its exports."""

    param: str
    module: str
    plan_var: str
    exports_cls: str


def _external_plans(plan: ReconstructedPlan) -> list[_ExternalPlan]:
    """Return the distinct owning plans referenced, each with a stable parameter."""
    distinct = sorted(
        {
            (ref.module, ref.plan_var, ref.exports_cls)
            for ref in plan.external_refs.values()
        }
    )
    return [
        _ExternalPlan(param=f"p{i}", module=module, plan_var=plan_var, exports_cls=cls)
        for i, (module, plan_var, cls) in enumerate(distinct)
    ]


def _external_resolve(plan: ReconstructedPlan) -> dict[str, str]:
    """Map each external uid to its `<param>.exports.<field>` reference."""
    param_by_key = {
        (ep.module, ep.plan_var, ep.exports_cls): ep.param
        for ep in _external_plans(plan)
    }
    return {
        uid: f"{param_by_key[(ref.module, ref.plan_var, ref.exports_cls)]}"
        f".exports.{ref.field}"
        for uid, ref in plan.external_refs.items()
    }


def _str(value: str) -> str:
    """Render a Python string literal (double-quoted)."""
    return json.dumps(value)


def _name(spec: ObjectSpec) -> str:
    """Return `spec`'s name for kinds that always carry one."""
    assert spec.name is not None
    return spec.name


def _space_arg(spec: ObjectSpec) -> list[str]:
    """Return the `space_uid=` argument for `spec`, or empty if it has none."""
    space_uid = spec.payload.get("space_uid")
    if space_uid is None:
        return []
    return [f"space_uid=SpaceUID({_str(space_uid)})"]


def _single_space_arg(spec: ObjectSpec) -> list[str]:
    """Return the `single_space_uid=` argument for `spec`, or empty if none."""
    single_space_uid = spec.payload.get("single_space_uid")
    if single_space_uid is None:
        return []
    return [f"single_space_uid=SpaceUID({_str(single_space_uid)})"]


def _used_kinds(specs: list[ObjectSpec]) -> set[str]:
    return {spec.kind for spec in specs}


def _natural_key(text: str) -> tuple[str | int, ...]:
    """Split `text` into its text and number runs, so `v2` sorts before `v10`."""
    return tuple(
        int(part) if part.isdigit() else part for part in re.split(r"(\d+)", text)
    )


def _member_key(name: str) -> tuple[int, tuple[str | int, ...], tuple[str | int, ...]]:
    """Sort key isort (as ruff runs it by default) gives an imported name.

    `CONSTANTS` first, then `Classes`, then functions and variables, each
    in natural order ignoring case.
    """
    if len(name) > 1 and name.isupper():
        rank = 0
    elif name[:1].isupper():
        rank = 1
    else:
        rank = 2
    return rank, _natural_key(name.lower()), _natural_key(name)


def _from_imports(imports: set[tuple[str, str]]) -> list[str]:
    """Render `(module, name)` pairs as one sorted `from` import per line.

    The order is the one ruff's isort rules with `force-single-line` produce:
    by module (`lamindb_setup.evolve` before `lamindb_setup.evolve.render`),
    then by name, so a plans repository linting with them accepts the file.
    """
    ordered = sorted(
        imports,
        key=lambda pair: (
            _natural_key(pair[0].lower()),
            _natural_key(pair[0]),
            _member_key(pair[1]),
        ),
    )
    return [f"from {module} import {name}" for module, name in ordered]


def _header(plan: ReconstructedPlan, description: str) -> list[str]:
    kinds = _used_kinds(plan.specs)
    lines = [f'"""{description}."""', "", "from __future__ import annotations", ""]

    std_imports = []
    if plan.needs_datetime:
        std_imports.append("import datetime")
    if std_imports:
        lines.extend(std_imports)
        lines.append("")

    names = {"PlanContext", "create_plan"}
    names |= kinds & {
        "Feature",
        "FeatureType",
        "RecordType",
        "Schema",
        "SchemaType",
        "Sheet",
    }
    if plan.needs_lamindtype:
        names.add("LaminDType")
    if plan.needs_sourceuid:
        names.add("SourceUID")
    if plan.needs_spaceuid:
        names.add("SpaceUID")
    external = _external_plans(plan)
    if external:
        names.add("Plan")
    imports = {("lamindb_setup.evolve", name) for name in names}
    imports.add(("lamindb_setup.evolve.render", "TerminalPlanSummary"))
    for ep in external:
        imports.add((ep.module, ep.exports_cls))
        imports.add((ep.module, ep.plan_var))
    lines.extend(_from_imports(imports))
    lines.append("")

    lines.append(STEM_UID_COMMENT)
    lines.append(f'{PLAN_STEM_UID_VAR} = "{plan_stem_placeholder()}"')
    return lines


def plan_stem_placeholder() -> str:
    """Sentinel replaced with the real stem uid by :func:`render_source`."""
    return "__STEM_UID__"


def _dtype_expr(dtype: DtypeInfo, resolve: dict[str, str]) -> str:
    if dtype.union_members:
        # A union categorical (cat[A|B]): LaminDType([R1, R2, ...]).
        members = [
            f"{reg}.{fld}" if fld is not None else reg
            for reg, fld in dtype.union_members
        ]
        inner = f"[{', '.join(members)}]"
        if dtype.is_list:
            inner = f"list[{inner}]"
        return f"LaminDType({inner})"
    if dtype.simple_py is not None:
        inner = dtype.simple_py
    elif dtype.field_name is not None:
        inner = f"{dtype.registry_expr}.{dtype.field_name}"
    else:
        inner = dtype.registry_expr or "str"
    if dtype.is_list:
        inner = f"list[{inner}]"

    args = [inner]
    if dtype.type_uid is not None:
        args.append(f"type__uid={resolve[dtype.type_uid]}.uid")
    if "schema__uid" in dtype.filters:
        args.append(f"schema__uid={resolve[dtype.filters['schema__uid']]}.uid")
    if "source__uid" in dtype.filters:
        args.append(f"source__uid=SourceUID({_str(dtype.filters['source__uid'])})")
    if "is_type" in dtype.filters:
        args.append(f"is_type={dtype.filters['is_type'] == 'True'}")
    return f"LaminDType({', '.join(args)})"


def _feature_args(spec: ObjectSpec, resolve: dict[str, str]) -> list[str]:
    p = spec.payload
    args = [_str(_name(spec)), f"dtype={_dtype_expr(p['dtype'], resolve)}"]
    if p["type_uid"] is not None:
        args.append(f"type={resolve[p['type_uid']]}")
    if p["synonyms"]:
        rendered = ", ".join(_str(s) for s in p["synonyms"])
        args.append(f"synonyms=[{rendered}]")
    if p["unit"] is not None:
        args.append(f"unit={_str(p['unit'])}")
    if p["description"]:
        args.append(f"description={_str(p['description'])}")
    if p["nullable"] is False:
        args.append("nullable=False")
    if p["default_value"] is not None:
        args.append(f"default_value={p['default_value']!r}")
    if p["coerce"] is True:
        args.append("coerce=True")
    args.extend(_space_arg(spec))
    return args


def _schema_args(spec: ObjectSpec, resolve: dict[str, str]) -> list[str]:
    p = spec.payload
    features = ", ".join(resolve[uid] for uid in p["feature_uids"])
    args: list[str] = []
    if spec.name is not None:
        args.append(f"name={_str(spec.name)}")
    args.append(f"features=[{features}]")
    if p["index_feature_uid"] is not None:
        args.append(f"index={resolve[p['index_feature_uid']]}")
    if p.get("optional_feature_uids"):
        optionals = ", ".join(resolve[uid] for uid in p["optional_feature_uids"])
        args.append(f"optionals=[{optionals}]")
    if p["description"]:
        args.append(f"description={_str(p['description'])}")
    if p["type_uid"] is not None:
        args.append(f"type={resolve[p['type_uid']]}")
    if p["otype"] is not None:
        args.append(f"otype={_str(p['otype'])}")
    if p.get("slots"):
        slots = ", ".join(
            f"{_str(key)}: {resolve[uid]}" for key, uid in p["slots"].items()
        )
        args.append(f"slots={{{slots}}}")
    if p.get("formats"):
        # JSON values, so their repr is a valid Python literal
        args.append(f"formats={p['formats']!r}")
    if p.get("itype_expr") is not None:
        args.append(f"itype={p['itype_expr']}")
    if p.get("dtype") is not None:
        args.append(f"dtype={_dtype_expr(p['dtype'], resolve)}")
    if p["flexible"] is True:
        args.append("flexible=True")
    if p["coerce"] is True:
        args.append("coerce=True")
    if p["minimal_set"] is False:
        args.append("minimal_set=False")
    if p["ordered_set"] is True:
        args.append("ordered_set=True")
    if p["maximal_set"] is True:
        args.append("maximal_set=True")
    args.extend(_space_arg(spec))
    return args


def _render_object(spec: ObjectSpec, resolve: dict[str, str]) -> str:
    p = spec.payload
    if spec.kind == "FeatureType":
        args = [_str(_name(spec))]
        if p["type_uid"] is not None:
            args.append(f"type={resolve[p['type_uid']]}")
        args.extend(_space_arg(spec))
        return f"FeatureType({', '.join(args)})"
    if spec.kind == "Feature":
        return f"Feature({', '.join(_feature_args(spec, resolve))})"
    if spec.kind == "SchemaType":
        args = [_str(_name(spec))]
        if p["description"]:
            args.append(f"description={_str(p['description'])}")
        if p["type_uid"] is not None:
            args.append(f"type={resolve[p['type_uid']]}")
        args.extend(_space_arg(spec))
        return f"SchemaType({', '.join(args)})"
    if spec.kind == "Schema":
        return f"Schema({', '.join(_schema_args(spec, resolve))})"
    if spec.kind == "RecordType":
        args = [_str(_name(spec))]
        if p["description"]:
            args.append(f"description={_str(p['description'])}")
        if p["type_uid"] is not None:
            args.append(f"type={resolve[p['type_uid']]}")
        args.extend(_space_arg(spec))
        args.extend(_single_space_arg(spec))
        if p.get("exclude_children_from_plans"):
            args.append("exclude_children_from_plans=True")
        return f"RecordType({', '.join(args)})"
    if spec.kind == "Sheet":
        args = [_str(_name(spec)), f"schema={resolve[p['schema_uid']]}"]
        if p["description"]:
            args.append(f"description={_str(p['description'])}")
        if p["type_uid"] is not None:
            args.append(f"type={resolve[p['type_uid']]}")
        args.extend(_space_arg(spec))
        args.extend(_single_space_arg(spec))
        if p.get("is_experiment_sheet"):
            args.append("is_experiment_sheet=True")
        if p.get("exclude_children_from_plans"):
            args.append("exclude_children_from_plans=True")
        return f"Sheet({', '.join(args)})"
    raise ValueError(f"unknown object kind {spec.kind!r}")


def _body(plan: ReconstructedPlan, func: str) -> list[str]:
    resolve = {spec.uid: spec.varname for spec in plan.specs}
    resolve.update(_external_resolve(plan))
    signature = "ctx: PlanContext, exports: None"
    for ep in _external_plans(plan):
        signature += f", {ep.param}: Plan[{ep.exports_cls}] = {ep.plan_var}"
    lines = [
        "",
        "",
        "@create_plan",
        f"def {func}({signature}) -> None:",
        f'{_INDENT}"""Recreate the introspected instance objects."""',
    ]
    runtime_imports = []
    if plan.needs_lamindb:
        runtime_imports.append(f"{_INDENT}import lamindb as ln")
    if plan.needs_bionty:
        runtime_imports.append(f"{_INDENT}import bionty as bt")
    if plan.needs_pertdb:
        runtime_imports.append(f"{_INDENT}import pertdb as pt")
    if runtime_imports:
        lines.extend(runtime_imports)
        lines.append("")

    if plan.default_space_uid is not None:
        space = f"SpaceUID({_str(plan.default_space_uid)})"
        lines.append(f"{_INDENT}ctx.default_space_uid = {space}")
        lines.append("")

    if not plan.specs:
        lines.append(f"{_INDENT}pass")
        return lines

    for spec in plan.specs:
        lines.append(f"{_INDENT}{spec.varname} = {_render_object(spec, resolve)}")
    return lines


def _footer(func: str) -> list[str]:
    return [
        "",
        "",
        'if __name__ == "__main__":',
        f"{_INDENT}TerminalPlanSummary().print_summary({func})",
    ]


def render_source(
    plan: ReconstructedPlan,
    *,
    description: str,
    func: str,
    stem_uid: str,
) -> str:
    """Render `plan` as plan-file source with `stem_uid` and entrypoint `func`."""
    lines = _header(plan, description) + _body(plan, func) + _footer(func)
    source = "\n".join(lines) + "\n"
    return source.replace(plan_stem_placeholder(), stem_uid)
