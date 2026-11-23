"""Plan-file scaffolding used by `lamin evolve init`.

Holds the template for new plan files and the helpers that generate a stem uid,
render a fresh plan, and inject a `PLAN_TRANSFORM_STEM_UID` into an existing
plan file.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ..core import InvalidArgumentError
from ..engine import (
    PLAN_STEM_UID_VAR,
    StemUidError,
    generate_stem_uids,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

__all__ = [
    "MINIMAL_PLAN_TEMPLATE",
    "RICH_PLAN_TEMPLATE",
    "STEM_UID_COMMENT",
    "add_stem_uid_to_plan",
    "generate_plan_stem_uid",
    "plan_slug",
    "render_plan",
]

STEM_UID_COMMENT = (
    "# Stable transform stem uid identifying this plan in LaminDB. `lamin evolve\n"
    "# apply` passes it to `ln.track`, so every application is recorded as a new\n"
    "# version of this one transform. Do not change it once the plan has been "
    "applied."
)

# Header shared by both templates: module docstring, authoring imports, and the
# stem-uid block. Everything a plan declares with comes from the package top
# level, so a scaffolded plan has one import source to learn. It is all safe to
# import at module scope before connecting; optional registry imports are kept
# inside the plan body.
_HEADER = '''"""__DESCRIPTION__."""

from __future__ import annotations

import datetime

from lamindb_setup.evolve import CatDType
from lamindb_setup.evolve import CellTypeCat
from lamindb_setup.evolve import DType
from lamindb_setup.evolve import Feature
from lamindb_setup.evolve import FeatureGroup
from lamindb_setup.evolve import FeatureType
from lamindb_setup.evolve import GroupConfig
from lamindb_setup.evolve import LaminDType
from lamindb_setup.evolve import ListDType
from lamindb_setup.evolve import Plan
from lamindb_setup.evolve import PlanContext
from lamindb_setup.evolve import PlanExports
from lamindb_setup.evolve import Record
from lamindb_setup.evolve import RecordCat
from lamindb_setup.evolve import RecordType
from lamindb_setup.evolve import Schema
from lamindb_setup.evolve import SchemaConfig
from lamindb_setup.evolve import SchemaModel
from lamindb_setup.evolve import SchemaType
from lamindb_setup.evolve import Sheet
from lamindb_setup.evolve import SpaceUID
from lamindb_setup.evolve import ULabelCat
from lamindb_setup.evolve import UnionCat
from lamindb_setup.evolve import create_plan
from lamindb_setup.evolve import feature
from lamindb_setup.evolve import url
from lamindb_setup.evolve.render import TerminalPlanSummary

__COMMENT__
__VAR__ = "__UID__"
'''

_FOOTER = """

if __name__ == "__main__":
    TerminalPlanSummary().print_summary(__FUNC__)
"""

# Minimal body: a couple of pointers, nothing else.
_MINIMAL_BODY = '''

@create_plan
def __FUNC__(ctx: PlanContext, exports: None) -> None:
    """__DESCRIPTION__."""
    # Declare your schema as a SchemaModel: each annotated field becomes a feature.
    # Declaring the class inside the plan body registers its objects with the plan.
    #   class MySchema(SchemaModel):
    #       schema_config = SchemaConfig(name="MySchema")
    #       sample_id: Feature[str]
    #       replicate: Feature[int]
'''

# Rich body: a guided tour of every building block, fully commented out so the
# scaffolded plan still validates offline (an empty effective body). Uncomment and
# adapt the pieces you need.
#
# NOTE: templates are rendered by `render_plan` via plain `str.replace` of the
# `__TOKEN__` placeholders, NOT `str.format` -- the rich body contains literal
# `{` and `}` (dict literals) that `str.format` would choke on.
_RICH_BODY = '''

@create_plan
def __FUNC__(ctx: PlanContext, exports: None) -> None:
    """__DESCRIPTION__."""
    # --- lamindb / bionty live here, NEVER at module scope -------------------
    # Declare the plan before connecting to an instance; keep these imports
    # inside the plan body. (The building blocks imported at the top are fine.)
    #   import bionty as bt
    #   import lamindb as ln

    # --- plan-wide settings --------------------------------------------------
    # Place every object created by this plan in a specific space:
    #   ctx.default_space_uid = SpaceUID("abcdEF12abcd")

    # --- a declarative schema (preferred) ------------------------------------
    # Subclass SchemaModel; each annotated field becomes a feature, in order.
    # Declaring the class inside the plan body registers its objects with the plan.
    #   class Experiment(SchemaModel):
    #       schema_config = SchemaConfig(name="ExperimentSchema")
    #       sample_id: Feature[str]
    #       replicate: Feature[int]

    # --- field dtypes --------------------------------------------------------
    # Basic types need no wrapper; DType[...] / ListDType[...] cover the rest.
    #   class Measurements(SchemaModel):
    #       schema_config = SchemaConfig(name="MeasurementsSchema")
    #       amount: Feature[float]
    #       measured_at: Feature[datetime.date]
    #       homepage: Feature[DType[url]]
    #       counts: Feature[ListDType[int]]

    # --- per-field overrides with feature(...) -------------------------------
    # Set any Feature option (name, description, synonyms, unit, default_value):
    #   class Sample(SchemaModel):
    #       schema_config = SchemaConfig(name="SampleSchema")
    #       organism: Feature[str] = feature(description="Species")

    # --- nullable vs. optional -----------------------------------------------
    # The annotation decides nullability: `Feature[<dtype>]` is not nullable,
    # `Feature[<dtype> | None]` is. The outer `Feature[...] | None` is a
    # different axis -- an optional (not-required) schema member.
    #   class Nulls(SchemaModel):
    #       schema_config = SchemaConfig(name="NullsSchema")
    #       sample_id: Feature[str]           # required, not nullable
    #       organism: Feature[str | None]     # nullable
    #       replicate: Feature[int] | None    # optional

    # --- schema_config presets -----------------------------------------------
    # Attach a type, a default feature type, and an index (a declared str field).
    #   class Typed(SchemaModel):
    #       schema_config = SchemaConfig(
    #           name="TypedSchema",
    #           type=SchemaType("TypedSchema"),
    #           default_feature_type=FeatureType("typed_feature"),
    #           index="sample_id",
    #       )
    #       sample_id: Feature[str]

    # --- categorical (ontology / registry) features --------------------------
    # CatDType[Registry, <Cat>(...)] ties a feature to a lamindb/bionty registry.
    #   class Cells(SchemaModel):
    #       schema_config = SchemaConfig(name="CellsSchema")
    #       cell_type: Feature[CatDType[bt.CellType, CellTypeCat()]]
    #       condition: Feature[CatDType[ln.ULabel, ULabelCat(type__uid="uTEST00000000")]]

    # --- union categoricals (cat[A|B]) ---------------------------------------
    # Accept values from several registries with a union plus a UnionCat() marker.
    #   class Either(SchemaModel):
    #       schema_config = SchemaConfig(name="EitherSchema")
    #       label: Feature[CatDType[ln.ULabel | ln.Record, UnionCat()]]

    # --- cross-object relations via .uid promises ----------------------------
    # A categorical can point at another object created by this plan; its uid is
    # a promise resolved at apply time -- pass it into the matching *Cat filter.
    #   rt_sheets = RecordType("Test Sheets")
    #   class Linked(SchemaModel):
    #       schema_config = SchemaConfig(name="LinkedSchema")
    #       test_sheet: Feature[
    #           CatDType[ln.Record, RecordCat(type__uid=rt_sheets.uid, is_type=True)]
    #       ]

    # --- feature descriptions from the docstring -----------------------------
    # A numpy-style Features section documents fields; each entry names a field.
    #   class Documented(SchemaModel):
    #       """A documented schema.
    #
    #       Features
    #       --------
    #       sample_id
    #           The unique sample identifier.
    #       """
    #
    #       schema_config = SchemaConfig(name="DocumentedSchema")
    #       sample_id: Feature[str]

    # --- sharing a feature across schemas ------------------------------------
    # Each field *is* its emitted Feature, so assigning one reuses the same object
    # (the field name and dtype must match).
    #   class Left(SchemaModel):
    #       schema_config = SchemaConfig(name="LeftSchema")
    #       sample_id: Feature[str]
    #   class Right(SchemaModel):
    #       schema_config = SchemaConfig(name="RightSchema")
    #       sample_id: Feature[str] = Left.sample_id  # the exact same Feature

    # --- feature groups (no schema shape) ------------------------------------
    # Use FeatureGroup when features share a FeatureType but form no schema.
    #   class ExperimentFeatures(FeatureGroup):
    #       group_config = GroupConfig(feature_type="experiment_feature")
    #       sample_id: Feature[str]
    #       replicate: Feature[int]

    # --- grouping types ------------------------------------------------------
    # FeatureType / SchemaType / RecordType group related objects; nest with type=.
    #   rt = RecordType("My Records")
    #   rt_sheets = RecordType("My Sheets", type=rt)

    # --- records & sheets (standard syntax) ----------------------------------
    # A Sheet is a RecordType constrained by a Schema; a Record is a concrete row.
    #   Sheet("Experiments", schema=Experiment.schema, type=rt)
    #   Record(name="row-0", type=rt, features={Experiment.sample_id: "s0"})

    # --- composite (file-format) schemas (standard syntax) -------------------
    # Pass otype + slots to describe AnnData / MuData / SpatialData / DataFrame.
    # Declarative has no slot support, so build these with Schema(...); slot keys
    # depend on the otype (e.g. AnnData -> obs, var, var.T, uns).
    #   obs_schema = Schema(name="MyObs", features=[Experiment.sample_id])
    #   var_schema = Schema(name="MyVar", features=[Measurements.amount])
    #   Schema(
    #       name="MyAnnData",
    #       features=[],
    #       otype="AnnData",
    #       slots={"obs": obs_schema, "var": var_schema},
    #   )
    # A schema whose only slot is "__external__" needs no otype: it validates the
    # features= metadata of any artifact, e.g. a FASTQ file or a folder.
    #   Schema(name="MyFileMetadata", features=[], slots={"__external__": obs_schema})
'''

# Example showing how to export objects for other plans to import. Declared as a
# commented block near the plan because PlanExports subclasses live at module scope.
_RICH_EXPORTS_EXAMPLE = """

# --- sharing objects between plans -------------------------------------------
# A plan can export objects for other plans to depend on. Declare a PlanExports
# subclass (building-block types only), annotate the plan's `exports` parameter
# with it, and assign to its fields:
#
#   class MyExports(PlanExports):
#       record_type_0: RecordType
#       schema_0: Schema
#
#   @create_plan
#   def export_plan(ctx: PlanContext, exports: MyExports) -> None:
#       exports.record_type_0 = RecordType("Shared Records")
#
#       class Shared(SchemaModel):
#           schema_config = SchemaConfig(name="SharedSchema")
#           shared_col: Feature[str]
#
#       exports.schema_0 = Shared.schema  # a SchemaModel exposes its emitted Schema
#
# Another plan depends on it by adding a parameter annotated `Plan[MyExports]`
# with the exporting plan as the default, then reading `plan.exports`:
#
#   @create_plan
#   def import_plan(
#       ctx: PlanContext,
#       exports: None,
#       p0: Plan[MyExports] = export_plan,
#   ) -> None:
#       RecordType("Child Records", type=p0.exports.record_type_0)
"""

MINIMAL_PLAN_TEMPLATE = _HEADER + _MINIMAL_BODY + _FOOTER
RICH_PLAN_TEMPLATE = _HEADER + _RICH_BODY + _RICH_EXPORTS_EXAMPLE + _FOOTER


def plan_slug(name: str) -> str:
    """Normalize a user-supplied plan name to a lowercase identifier slug."""
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
    if not slug:
        raise InvalidArgumentError("plan name must contain alphanumeric characters")
    return slug


def generate_plan_stem_uid(instances: Sequence[str] = ()) -> str:
    """Generate a fresh stem uid, free on every instance in `instances`."""
    return generate_stem_uids(instances=instances)[0]


def render_plan(slug: str, func: str, uid: str, *, minimal: bool = False) -> str:
    """Render the contents of a new plan file for `slug`/`func` with `uid`.

    By default renders the rich template (all imports plus commented examples for
    every building block). Pass `minimal=True` for a bare scaffold without the
    example tour.
    """
    template = MINIMAL_PLAN_TEMPLATE if minimal else RICH_PLAN_TEMPLATE
    return (
        template.replace("__DESCRIPTION__", f"Schema plan {slug}")
        .replace("__COMMENT__", STEM_UID_COMMENT)
        .replace("__VAR__", PLAN_STEM_UID_VAR)
        .replace("__UID__", uid)
        .replace("__FUNC__", func)
    )


def add_stem_uid_to_plan(plan_path: Path, instances: Sequence[str] = ()) -> str:
    """Insert a fresh `PLAN_TRANSFORM_STEM_UID` into an existing plan file.

    The variable is placed just above the first `@create_plan` decorator (or
    appended if none is found).  Raises
    :class:`~lamindb_setup.evolve.engine.StemUidError` if the file is missing or
    already declares the variable.  The uid is free on every instance in
    `instances` (see :func:`~lamindb_setup.evolve.engine._track.generate_stem_uids`).
    Returns the generated uid.
    """
    if not plan_path.exists():
        raise StemUidError(f"{plan_path} does not exist.")
    text = plan_path.read_text()
    if re.search(rf"^{PLAN_STEM_UID_VAR}\s*=", text, re.MULTILINE):
        raise StemUidError(f"{plan_path} already defines {PLAN_STEM_UID_VAR}.")

    uid = generate_plan_stem_uid(instances)
    block = f'{STEM_UID_COMMENT}\n{PLAN_STEM_UID_VAR} = "{uid}"\n'

    lines = text.splitlines(keepends=True)
    insert_at = next(
        (i for i, line in enumerate(lines) if line.startswith("@create_plan")),
        None,
    )
    if insert_at is None:
        new_text = text.rstrip("\n") + "\n\n\n" + block
    else:
        lines.insert(insert_at, block + "\n")
        new_text = "".join(lines)
    plan_path.write_text(new_text)
    return uid
