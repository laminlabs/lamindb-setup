"""Declare features, schemas, and records in a schema evolution plan.

Import plan declarations before connecting to an instance::

    from lamindb_setup.evolve import create_plan

Use the `lamin evolve` CLI to validate, preview, and apply the plan.

Internal subpackages may change without notice.

Plans
-----

.. autosummary::
   :toctree: .

   create_plan
   Plan
   PlanContext
   PlanExports

Declarations
------------

.. autosummary::
   :toctree: .

   FeatureType
   Feature
   SchemaType
   Schema
   RecordType
   Sheet
   Record

Typed models and dtypes
-----------------------

.. autosummary::
   :toctree: .

   SchemaModel
   SchemaConfig
   LaminDType
   DType
   ListDType
   RecordCat
   ULabelCat

.. autofunction:: feature

Data migrations
---------------

Attach a migration to its target with :meth:`PlanContext.migrate`. Preview it
with `lamin evolve plan --preview-migrations` before applying it.

.. autosummary::
   :toctree: .

   DataMigration
   MigrationContext
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

# --- building blocks and data migrations ---
# --- dtypes ---
# --- declarative authoring surface ---
# --- plan authoring ---
from .authoring import (
    DEFAULT,
    DEFAULT_NAME_FIELD,
    NULL,
    ArtifactCat,
    BiologicCat,
    CatDType,
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
