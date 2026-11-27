"""Schema evolution.

Import plan declarations before connecting to an instance::

    from lamindb_setup.evolve import create_plan

Use the `lamin evolve` CLI to validate, preview, and apply the plan.

Internal subpackages may change without notice.

.. currentmodule:: lamindb_setup.evolve

Plans
-----

Define a :class:`Plan` and declare its reusable outputs with :class:`PlanExports`.

.. autosummary::
   :toctree: .
   :template: {reference_dir}/plans.rst

   create_plan

.. autosummary::

   Plan
   PlanContext
   PlanExports
   PlanDeclarationError

Declarations
------------

Declare features and group them into a :class:`Schema`, :class:`RecordType`, or :class:`Sheet`.

.. autosummary::
   :toctree: .
   :template: {reference_dir}/declarations.rst

   Feature

.. autosummary::

   FeatureType
   SchemaType
   Schema
   RecordType
   Sheet
   Record

Typed models and dtypes
-----------------------

Use Python type annotations and :func:`feature` to describe schema fields.

.. autosummary::
   :toctree: .
   :template: {reference_dir}/models.rst

   SchemaModel

.. autosummary::

   SchemaConfig
   LaminDType
   DType
   ListDType
   CatDType
   DEFAULT_NAME_FIELD
   RecordCat
   ULabelCat
   ResolvedDType
   get_dtype
   inline_cat_filters
   feature

Data migrations
---------------

Use :func:`fill_nulls`, :func:`map_values`, and other helpers to migrate existing data.

.. autosummary::
   :toctree: .
   :template: {reference_dir}/data-migrations.rst

   DataMigration

.. autosummary::

   MigrationReport
   MigrationError
   fill_nulls
   fill_missing
   map_values
   map_to_records
   map_to_str
   drop_values
   drop_rows
   revalidate_against
   revalidate_rows
   reexport_sheets
   accept_non_null
   accept_index_change
   accept_flexible_change
   drop_member_values
   keep_member_values
   drop_nonmember_annotations
   keep_stale_exports
   keep_values
   keep_validated
   keep_rows
   move_rows
   adopt_children
   update_record
   drop_record
   NULL
   DEFAULT

Custom migrations
-----------------

Write custom migration logic with helpers such as :func:`backfill_members`.

.. autosummary::
   :toctree: .
   :template: {reference_dir}/custom-migrations.rst

   MigrationContext

.. autosummary::

   DeclarationTarget
   MemberMigrationContext
   MemberRemovalContext
   IndexChangeContext
   FlexibleChangeContext
   FeatureRemovalContext
   RecordTypeRemovalContext
   SchemaRemovalContext
   SheetExportContext
   RecordUpdateContext
   RecordRemovalContext
   backfill_members
   clear_values
   clear_member_values
   clear_nonmember_annotations
   relocate_to_records
   relocate_to_str
   rewrite_values

Plan result types
-----------------

.. autosummary::

   SpaceUID
   ~lamindb_setup.evolve.core._uids.FeatureUID
   ~lamindb_setup.evolve.core._uids.RecordUID
   ~lamindb_setup.evolve.core._uids.SchemaUID
   ~lamindb_setup.evolve.core._graph.Promise
   ~lamindb_setup.evolve.core._graph.Dependency
   ~lamindb_setup.evolve.core._graph.DependencyDAG
   ~lamindb_setup.evolve.authoring._plan.Moved
   ~lamindb_setup.evolve.authoring._migrations.Migrated
   ~lamindb_setup.evolve.authoring._migrations.WriteStrategy

Environment variables
---------------------

Configure tracking before applying plans. Explicit nonblank `track_prefix`
and `track_space` settings in the instance's CI configuration take precedence
over the environment.

.. list-table::
   :header-rows: 1
   :widths: 38 42 20

   * - Variable
     - Value and purpose
     - Default
   * - `LAMIN_SCHEMA_EVOLUTION_TRACK_PREFIX`
     - String prefix for tracked transform keys.
     - `schema_evolution`
   * - `LAMIN_SCHEMA_EVOLUTION_TRACK_SPACE`
     - Space name or UID for tracked runs, transforms, and outputs.
     - `all`
   * - `LAMIN_SCHEMA_EVOLUTION_TRACEBACK`
     - Any nonempty value enables tracebacks for handled command-line errors.
       It does not change Python API exceptions.
     - Unset

`PLAN_TRANSFORM_STEM_UID` is a variable in each plan file, not an environment
variable. Keep it stable when editing the plan.
"""

from pathlib import Path as _Path
from typing import TYPE_CHECKING

from . import authoring as _authoring

# --- building blocks and data migrations ---
# --- dtypes ---
# --- declarative authoring surface ---
# --- plan authoring ---
from .authoring import (
    DEFAULT,
    NULL,
    ArtifactCat,
    BiologicCat,
    CellLineCat,
    CellMarkerCat,
    CellTypeCat,
    CollectionCat,
    CombinationPerturbationCat,
    CompoundCat,
    CompoundPerturbationCat,
    DangerousCat,
    DataMigration,
    DeclarationTarget,
    DevelopmentalStageCat,
    DiseaseCat,
    DType,
    EnvironmentalPerturbationCat,
    EthnicityCat,
    ExperimentalFactorCat,
    Feature,
    FeatureCat,
    FeatureGroup,
    FeatureRemovalContext,
    FeatureType,
    FlexibleChangeContext,
    GeneCat,
    GeneticPerturbationCat,
    GroupConfig,
    IndexChangeContext,
    LaminDType,
    ListDType,
    MemberMigrationContext,
    MemberRemovalContext,
    MigrationContext,
    MigrationError,
    MigrationReport,
    OrganismCat,
    PathwayCat,
    PerturbationTargetCat,
    PhenotypeCat,
    Plan,
    PlanContext,
    PlanDeclarationError,
    PlanExports,
    ProjectCat,
    ProteinCat,
    Record,
    RecordCat,
    RecordRemovalContext,
    RecordType,
    RecordTypeRemovalContext,
    RecordUpdateContext,
    ReferenceCat,
    ResolvedDType,
    RunCat,
    Schema,
    SchemaCat,
    SchemaConfig,
    SchemaModel,
    SchemaRemovalContext,
    SchemaType,
    Sheet,
    SheetExportContext,
    SourceCat,
    StorageCat,
    TissueCat,
    TransformCat,
    ULabelCat,
    UnionCat,
    UserCat,
    create_plan,
    feature,
    get_dtype,
    inline_cat_filters,
    num,
    path,
    url,
)

# --- errors and uids ---
from .core import LaminSchemasError, SourceUID, SpaceUID
from .engine import (
    accept_added_member,
    accept_flexible_change,
    accept_index_change,
    accept_maximal_set,
    accept_minimal_set,
    accept_non_null,
    accept_ordered_set,
    accept_slot_change,
    accept_unit_change,
    adopt_children,
    backfill_members,
    clear_member_values,
    clear_nonmember_annotations,
    clear_values,
    detach_from_schemas,
    drop_member_values,
    drop_nonmember_annotations,
    drop_record,
    drop_rows,
    drop_values,
    fill_missing,
    fill_nulls,
    keep_member_values,
    keep_rows,
    keep_stale_exports,
    keep_validated,
    keep_values,
    map_to_records,
    map_to_str,
    map_values,
    merge_schema,
    move_rows,
    reexport_sheets,
    relocate_records,
    relocate_to_records,
    relocate_to_str,
    remap_records,
    rescale,
    rescale_values,
    revalidate_against,
    revalidate_rows,
    rewrite_exports,
    rewrite_values,
    unwrap_list,
    unwrap_values,
    update_record,
    wrap_in_list,
    wrap_values,
)

# Both the standalone and combined docs build these topic pages from this package.
if __doc__:
    __doc__ = __doc__.replace(
        "{reference_dir}", str(_Path(__file__).with_name("_reference"))
    )

if TYPE_CHECKING:
    # Preserve Annotated's special meaning for type checkers.
    from .authoring import CatDType
else:
    #: Annotate a registry type with the filters for a categorical feature.
    CatDType = _authoring.CatDType

#: Default lookup field for each categorical registry.
DEFAULT_NAME_FIELD = _authoring.DEFAULT_NAME_FIELD

__all__ = [
    "ArtifactCat",
    "BiologicCat",
    "CatDType",
    "CellLineCat",
    "CellMarkerCat",
    "CellTypeCat",
    "CollectionCat",
    "CombinationPerturbationCat",
    "CompoundCat",
    "CompoundPerturbationCat",
    "DEFAULT",
    "DEFAULT_NAME_FIELD",
    "DType",
    "DangerousCat",
    "DataMigration",
    "DeclarationTarget",
    "DevelopmentalStageCat",
    "DiseaseCat",
    "EnvironmentalPerturbationCat",
    "EthnicityCat",
    "ExperimentalFactorCat",
    "Feature",
    "FeatureCat",
    "FeatureGroup",
    "FeatureRemovalContext",
    "FeatureType",
    "FlexibleChangeContext",
    "GeneCat",
    "GeneticPerturbationCat",
    "GroupConfig",
    "IndexChangeContext",
    "LaminDType",
    "LaminSchemasError",
    "ListDType",
    "MemberMigrationContext",
    "MemberRemovalContext",
    "RecordRemovalContext",
    "RecordTypeRemovalContext",
    "RecordUpdateContext",
    "SchemaRemovalContext",
    "MigrationContext",
    "MigrationError",
    "MigrationReport",
    "NULL",
    "OrganismCat",
    "PathwayCat",
    "PerturbationTargetCat",
    "PhenotypeCat",
    "Plan",
    "PlanContext",
    "PlanDeclarationError",
    "PlanExports",
    "ProjectCat",
    "ProteinCat",
    "Record",
    "RecordCat",
    "RecordType",
    "ReferenceCat",
    "ResolvedDType",
    "RunCat",
    "Schema",
    "SchemaCat",
    "SchemaConfig",
    "SchemaModel",
    "SchemaType",
    "Sheet",
    "SheetExportContext",
    "SourceCat",
    "SourceUID",
    "SpaceUID",
    "StorageCat",
    "TissueCat",
    "TransformCat",
    "ULabelCat",
    "UnionCat",
    "UserCat",
    "accept_flexible_change",
    "accept_added_member",
    "accept_index_change",
    "accept_maximal_set",
    "accept_minimal_set",
    "accept_non_null",
    "accept_ordered_set",
    "accept_slot_change",
    "accept_unit_change",
    "backfill_members",
    "clear_member_values",
    "clear_nonmember_annotations",
    "clear_values",
    "create_plan",
    "adopt_children",
    "detach_from_schemas",
    "drop_member_values",
    "drop_nonmember_annotations",
    "drop_record",
    "drop_rows",
    "drop_values",
    "feature",
    "fill_missing",
    "fill_nulls",
    "get_dtype",
    "inline_cat_filters",
    "keep_member_values",
    "keep_stale_exports",
    "keep_rows",
    "keep_validated",
    "keep_values",
    "map_to_records",
    "map_to_str",
    "map_values",
    "merge_schema",
    "move_rows",
    "revalidate_against",
    "num",
    "path",
    "reexport_sheets",
    "relocate_records",
    "relocate_to_records",
    "relocate_to_str",
    "remap_records",
    "rescale",
    "rescale_values",
    "revalidate_rows",
    "rewrite_exports",
    "rewrite_values",
    "unwrap_list",
    "unwrap_values",
    "update_record",
    "url",
    "wrap_in_list",
    "wrap_values",
]
