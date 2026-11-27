"""Vocabulary and primitives every other layer shares.

No internal dependencies, and a single front door for them.  The modules
underneath are implementation detail -- import from
`lamindb_setup.evolve.core` rather than reaching into them:

- :class:`LaminSchemasError` is the base every user-facing error derives from;
  :class:`InvalidArgumentError` the one for an unusable value passed in.
- the `UID` types are the LaminDB identifiers a plan addresses objects by.
- :class:`Action`, :class:`ChangeItem` and friends are what a computed change
  *is*, the vocabulary the apply pipeline speaks; a change's warnings are
  :class:`ChangeWarning` strings, each with its :class:`WarningCategory`.
- :class:`LamindbModels` and :class:`CategoricalLink` describe the lamindb
  internals this package reaches into.
- :class:`Materializable`, :class:`Promise` and the dependency types are the
  object graph a plan is built from: `authoring` builds it, `engine` walks
  it, so neither owns it.

Nothing here imports `lamindb` at module level: a plan must be declarable
before lamindb is loaded, so the registries are described rather than
referenced.  Only :meth:`Materializable.commit`, which runs long after a plan is
declared, reaches it -- at call time.
"""

from __future__ import annotations

from ._changes import (
    PLACEHOLDER_UID,
    Action,
    ApplyMode,
    ChangeItem,
    ChangeSet,
    ChangeWarning,
    MissingPrerequisite,
    MoveOutcome,
    Placeholder,
    PrerequisitePlan,
    UidMisalignment,
    WarningCategory,
    warning_category,
)
from ._errors import InvalidArgumentError, LaminSchemasError
from ._graph import (
    SEED_ONLY_KINDS,
    Dependency,
    DependencyCollector,
    DependencyDAG,
    DependencyMeta,
    Materializable,
    MaterializeContext,
    Promise,
    active_collector,
    collecting,
    provisional_promises,
)
from ._lamindb_api import DEFAULT_MODELS, CategoricalLink, LamindbModels
from ._uids import (
    UID,
    FeatureUID,
    RecordUID,
    SchemaUID,
    SourceUID,
    SpaceUID,
    ULabelUID,
    validate_lamindb_uid,
)

__all__ = [
    "DEFAULT_MODELS",
    "PLACEHOLDER_UID",
    "SEED_ONLY_KINDS",
    "UID",
    "Action",
    "ApplyMode",
    "CategoricalLink",
    "ChangeItem",
    "ChangeSet",
    "ChangeWarning",
    "Dependency",
    "DependencyCollector",
    "DependencyDAG",
    "DependencyMeta",
    "FeatureUID",
    "InvalidArgumentError",
    "LamindbModels",
    "LaminSchemasError",
    "MaterializeContext",
    "Materializable",
    "MissingPrerequisite",
    "MoveOutcome",
    "Placeholder",
    "PrerequisitePlan",
    "Promise",
    "RecordUID",
    "SchemaUID",
    "SourceUID",
    "SpaceUID",
    "ULabelUID",
    "UidMisalignment",
    "WarningCategory",
    "active_collector",
    "collecting",
    "provisional_promises",
    "validate_lamindb_uid",
    "warning_category",
]
