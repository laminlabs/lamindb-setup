"""Generate a plan file from an existing LaminDB instance.

The :func:`introspect_instance` entry point reads the feature types, features,
schema types, schemas, record types and sheets of a connected instance and
renders a `lamin evolve` plan file that, when applied, adopts those objects
without changes.  It is the starting point for managing an existing instance
declaratively.

The reader, the plan reconstruction and the file template live in private
modules; the names the other layers need are re-exported here so nothing
reaches into them.
"""

from __future__ import annotations

from ._errors import IntrospectionError
from ._read import InstanceContents, read_instance
from ._reverse import ExternalRef, ReconstructedPlan, build_plan
from ._run import IntrospectionResult, introspect_instance
from ._template import (
    add_stem_uid_to_plan,
    generate_plan_stem_uid,
    plan_slug,
    render_plan,
)

__all__ = [
    "ExternalRef",
    "InstanceContents",
    "IntrospectionError",
    "IntrospectionResult",
    "ReconstructedPlan",
    "add_stem_uid_to_plan",
    "build_plan",
    "generate_plan_stem_uid",
    "introspect_instance",
    "plan_slug",
    "read_instance",
    "render_plan",
]
