"""A small plan that exercises types, features, schemas, sheets, and records."""

from lamindb_setup.evolve import (
    Feature,
    FeatureType,
    LaminDType,
    PlanContext,
    Record,
    RecordType,
    Schema,
    SchemaType,
    Sheet,
    create_plan,
)

PLAN_TRANSFORM_STEM_UID = "demoUID00002"


@create_plan
def measurements(ctx: PlanContext, exports: None) -> None:
    kind = FeatureType("Measurements")
    sample = Feature("sample", dtype=LaminDType(str), type=kind)
    value = Feature("measurement", dtype=LaminDType(float), type=kind)
    schema = Schema(
        name="Measurements",
        type=SchemaType("Tables"),
        features=[sample, value],
        index=sample,
    )
    Sheet("Measurements", schema=schema, type=RecordType("Experiments"))
    Record("Pilot", type=RecordType("Studies"))
