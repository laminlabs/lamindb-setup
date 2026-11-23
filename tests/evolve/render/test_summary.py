from __future__ import annotations

import json

from lamindb_setup.evolve.authoring._dtypes import LaminDType
from lamindb_setup.evolve.authoring._features import Feature, FeatureType
from lamindb_setup.evolve.authoring._plan import (
    Plan,
    PlanContext,
    PlanExports,
    create_plan,
)
from lamindb_setup.evolve.authoring._records import RecordType, Sheet
from lamindb_setup.evolve.authoring._schemas import Schema, SchemaType
from lamindb_setup.evolve.core import SpaceUID
from lamindb_setup.evolve.engine import (
    accept_index_change,
    keep_validated,
    map_values,
)
from lamindb_setup.evolve.render import (
    HtmlPlanSummary,
    JsonPlanSummary,
    MarkdownPlanSummary,
    PlanSummary,
    TerminalPlanSummary,
    build_summary_model,
)


class _Exports(PlanExports):
    record_type_0: RecordType


@create_plan
def _base_plan(ctx: PlanContext, exports: _Exports) -> None:
    exports.record_type_0 = RecordType("Base Record")


@create_plan
def _single_space_summary_plan(ctx: PlanContext, exports: None) -> None:
    f = Feature("ss_summary_col", dtype=LaminDType(str))
    schema = Schema(name="SsSummarySchema", features=[f])
    rt = RecordType("SsSummaryRT", single_space_uid=SpaceUID("spaceUID0002"))
    Sheet(
        "SsSummarySheet",
        schema=schema,
        type=rt,
        single_space_uid=SpaceUID("spaceUID0002"),
        is_experiment_sheet=True,
    )


@create_plan
def _child_plan(
    ctx: PlanContext,
    exports: None,
    base: Plan[_Exports] = _base_plan,
) -> None:
    ft = FeatureType("ft")
    st = SchemaType("st")
    f = Feature("col", dtype=LaminDType(str), type=ft)
    Schema(name="ChildSchema", features=[f], type=st)
    RecordType("Child Record", type=base.exports.record_type_0)


def test_plan_summary_is_abstract() -> None:
    assert issubclass(TerminalPlanSummary, PlanSummary)
    assert issubclass(HtmlPlanSummary, PlanSummary)
    assert issubclass(MarkdownPlanSummary, PlanSummary)
    try:
        PlanSummary()  # type: ignore[abstract]
    except TypeError:
        pass
    else:  # pragma: no cover
        raise AssertionError("PlanSummary should not be instantiable")


def test_summary_model_includes_parent_plans_and_dag() -> None:
    model = build_summary_model(_child_plan, "Plan summary")
    assert model.plan.name == "_child_plan"
    # parent plan is included in the plan dependency tree
    assert [p.name for p in model.plan.depends_on] == ["_base_plan"]
    # the full DAG contains objects from both plans
    kinds = {obj.kind for obj in model.objects}
    assert {"FeatureType", "SchemaType", "Feature", "Schema", "RecordType"} <= kinds
    # the child RecordType requires the base RecordType (dependency relation)
    child = next(o for o in model.objects if o.name == "Child Record")
    requirements = model.requirements_of(child, exclude_parent=False)
    assert any(req.name == "Base Record" for req in requirements)
    # the base RecordType is the child's nesting parent, so it is omitted by
    # default and the child is nested beneath it
    base = next(o for o in model.objects if o.name == "Base Record")
    assert child.parent_id == base.obj_id
    assert child in model.children_of(base)
    assert all(req.name != "Base Record" for req in model.requirements_of(child))


def test_terminal_summary_renders_plan_tree_and_dependencies() -> None:
    text = TerminalPlanSummary().render(_child_plan)
    assert "Plan summary: _child_plan" in text
    assert "_base_plan" in text
    assert "Plans" in text
    assert "Objects" in text
    assert "Schema 'ChildSchema'" in text
    assert "Feature 'col'" in text


def test_html_summary_is_self_contained() -> None:
    html_out = HtmlPlanSummary().render(_child_plan)
    assert html_out.startswith("<!DOCTYPE html>")
    assert html_out.rstrip().endswith("</html>")
    # embedded CSS makes it a single self-contained file
    assert "<style>" in html_out
    assert "<link" not in html_out
    # plan tree and dependency DAG are rendered
    assert "_child_plan" in html_out
    assert "_base_plan" in html_out
    assert "ChildSchema" in html_out
    assert "Child Record" in html_out


def test_html_summary_escapes_content() -> None:
    @create_plan
    def _escape_plan(ctx: PlanContext, exports: None) -> None:
        RecordType("A & B <test>")

    html_out = HtmlPlanSummary().render(_escape_plan)
    assert "A &amp; B &lt;test&gt;" in html_out
    assert "<test>" not in html_out


def test_summary_model_groups_objects_without_duplication() -> None:
    model = build_summary_model(_child_plan, "Plan summary")
    groups = dict(model.grouped_objects)
    # each kind lands in its user-facing group
    assert {o.kind for o in groups["Features"]} <= {"FeatureType", "Feature"}
    assert {o.kind for o in groups["Schemas"]} <= {"SchemaType", "Schema"}
    assert {o.kind for o in groups["Records"]} <= {"RecordType"}
    # every object appears exactly once across all groups
    flat = [o.obj_id for _, objs in model.grouped_objects for o in objs]
    assert len(flat) == len(set(flat)) == len(model.objects)


def test_terminal_summary_lists_requirements_once_per_line() -> None:
    text = TerminalPlanSummary().render(_child_plan)
    # the shared Schema is listed exactly once (no DAG duplication)
    assert text.count("Schema 'ChildSchema'") == 1
    # requirements are rendered under a 'requires' branch
    assert "requires" in text
    # the child RecordType references the base RecordType as a requirement
    assert "Base Record" in text


def test_markdown_summary_renders_headings_and_objects() -> None:
    md = MarkdownPlanSummary().render(_child_plan)
    # markdown headings for the plan tree and object sections
    assert md.startswith("## Plan summary: _child_plan")
    assert "### Plans" in md
    assert "### Objects (" in md
    # parent and child plans appear in the plan tree
    assert "**_base_plan**" in md
    assert "**_child_plan**" in md
    # object kinds are bold and names are wrapped in backticks
    assert "**Schema** `ChildSchema`" in md
    assert "**Feature** `col`" in md
    # requirements are rendered as nested bullets
    assert "_requires_" in md
    # the shared Schema is listed exactly once (no DAG duplication)
    assert md.count("`ChildSchema`") == 1


def test_summary_includes_single_space_and_experiment_sheet() -> None:
    model = build_summary_model(_single_space_summary_plan, "Plan summary")
    by_name = {obj.name: obj for obj in model.objects}

    rt = by_name["SsSummaryRT"]
    assert any("single_space='spaceUID0002'" in d for d in rt.details)

    sheet = by_name["SsSummarySheet"]
    assert any("single_space='spaceUID0002'" in d for d in sheet.details)
    assert "is_experiment_sheet=True" in sheet.details


def test_markdown_escapes_a_name_holding_a_backtick() -> None:
    """The HTML renderer has always escaped; the Markdown one used not to."""
    from lamindb_setup.evolve.render import MarkdownPlanSummary
    from lamindb_setup.evolve.render._model import ObjectInfo, PlanRef, SummaryModel

    model = SummaryModel(
        title="Plan summary",
        plan=PlanRef(name="p", default_space=None),
        objects=[ObjectInfo(obj_id=1, kind="Feature", name="a|b `c` <d>")],
    )

    rendered = MarkdownPlanSummary()._render(model)

    # the whole name sits inside one code span, not split at the inner backtick
    assert "`a|b `c` <d>`" in rendered


def test_the_ci_report_and_the_plan_summary_share_one_object_rendering() -> None:
    """Two copies of this had already drifted on truncation behavior."""
    from lamindb_setup.evolve.ci import _markdown as ci_markdown
    from lamindb_setup.evolve.render._markdown import object_label, object_ref

    assert ci_markdown._object_label is object_label
    assert ci_markdown._object_ref is object_ref


@create_plan
def _declaring_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("decl_ft")
    a = Feature("decl_a", dtype=LaminDType(int), type=ft)
    ctx.migrate(a, map_values(reason="a < b & c"))
    ctx.migrate("Schema:DeclLegacy", keep_validated(reason="superseded"))
    ctx.migrate(Schema(features=[a]), accept_index_change(reason="re-keyed"))
    ctx.moved(a, "decl_old")


def test_summary_model_lists_the_plans_declarations() -> None:
    model = build_summary_model(_declaring_plan, "Plan summary")

    assert [d.label for d in model.declarations] == [
        "migrate Feature 'decl_a' ← map_values()",
        "migrate 'Schema:DeclLegacy' ← keep_validated()",
        "migrate Schema ← accept_index_change()",
        "moved Feature 'decl_a' ← 'decl_old'",
    ]
    assert [d.reason for d in model.declarations] == [
        "a < b & c",
        "superseded",
        "re-keyed",
        None,
    ]


def test_a_prerequisites_declarations_are_its_own() -> None:
    assert build_summary_model(_child_plan, "Plan summary").declarations == []


def test_every_summary_renders_the_declarations() -> None:
    terminal = TerminalPlanSummary().render(_declaring_plan)
    md = MarkdownPlanSummary().render(_declaring_plan)
    page = HtmlPlanSummary().render(_declaring_plan)
    document = json.loads(JsonPlanSummary().render(_declaring_plan))

    assert "Declarations (4)" in terminal
    assert "migrate Feature 'decl_a' ← map_values()  (a < b & c)" in terminal
    assert "migrate Schema ← accept_index_change()  (re-keyed)" in terminal
    assert md.endswith(
        "### Declarations (4)\n"
        "- migrate **Feature** `decl_a` ← `map_values()` (a < b & c)\n"
        "- migrate `Schema:DeclLegacy` ← `keep_validated()` (superseded)\n"
        "- migrate **Schema** ← `accept_index_change()` (re-keyed)\n"
        "- moved **Feature** `decl_a` ← `'decl_old'`\n"
    )
    assert "<h2>Declarations (4)</h2>" in page
    assert 'migrate <span class="kind">Schema</span> &larr;' in page
    assert "(a &lt; b &amp; c)" in page
    assert "&#x27;Schema:DeclLegacy&#x27;" in page
    assert document["declarations"][3] == {
        "verb": "moved",
        "source": "'decl_old'",
        "kind": "Feature",
        "name": "decl_a",
        "key": None,
        "reason": None,
        "label": "moved Feature 'decl_a' ← 'decl_old'",
    }


def test_a_summary_without_declarations_has_no_declarations_section() -> None:
    assert "Declarations" not in TerminalPlanSummary().render(_child_plan)
    assert "Declarations" not in MarkdownPlanSummary().render(_child_plan)
    assert "Declarations" not in HtmlPlanSummary().render(_child_plan)
