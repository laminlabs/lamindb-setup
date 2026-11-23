"""Minimal feature-type plan for offline validation."""

from lamindb_setup.evolve import FeatureType, PlanContext, create_plan

PLAN_TRANSFORM_STEM_UID = "demoUID00001"


@create_plan
def demo(ctx: PlanContext, exports: None) -> None:
    parent = FeatureType("Measurements")
    FeatureType("ClinicalMeasurements", type=parent)
