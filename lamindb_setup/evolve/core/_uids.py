# ruff: noqa: B904
"""UID helpers for LaminDB identifiers.

The validator supports Base62 UIDs and can enforce registry-specific lengths.
"""

from __future__ import annotations

import string
from typing import NamedTuple, Self, TypeVar

__all__ = [
    "UID",
    "RecordUID",
    "SchemaUID",
    "SpaceUID",
    "SourceUID",
    "ULabelUID",
    "FeatureUID",
    "validate_lamindb_uid",
]

T = TypeVar("T", bound="UID")

#: The alphabet every LaminDB uid is drawn from.
BASE62_CHARSET = string.digits + string.ascii_uppercase + string.ascii_lowercase


class _UIDMetadata(NamedTuple):
    registry_name: str | None
    uid_length: int
    uid_stem_length: int


# Keyed by full model import path.  Values collected from Django model metadata.
REGISTRY_UID_METADATA: dict[str, tuple[int, int]] = {
    # --- lamindb ---
    "lamindb.models.artifact.Artifact": (20, 16),
    "lamindb.models.block.ArtifactBlock": (20, 16),
    "lamindb.models.block.BaseBlock": (20, 16),
    "lamindb.models.block.Block": (20, 16),
    "lamindb.models.block.BranchBlock": (20, 16),
    "lamindb.models.block.CollectionBlock": (20, 16),
    "lamindb.models.block.FeatureBlock": (20, 16),
    "lamindb.models.block.ProjectBlock": (20, 16),
    "lamindb.models.block.RecordBlock": (20, 16),
    "lamindb.models.block.RunBlock": (20, 16),
    "lamindb.models.block.SchemaBlock": (20, 16),
    "lamindb.models.block.SpaceBlock": (20, 16),
    "lamindb.models.block.TransformBlock": (20, 16),
    "lamindb.models.block.ULabelBlock": (20, 16),
    "lamindb.models.collection.Collection": (20, 16),
    "lamindb.models.feature.Feature": (12, 12),
    "lamindb.models.project.Project": (12, 12),
    "lamindb.models.project.Reference": (12, 12),
    "lamindb.models.record.Record": (16, 16),
    "lamindb.models.run.Run": (20, 20),
    "lamindb.models.run.User": (8, 8),
    "lamindb.models.schema.Schema": (16, 16),
    "lamindb.models.sqlrecord.Branch": (12, 12),
    "lamindb.models.sqlrecord.Space": (12, 12),
    "lamindb.models.storage.Storage": (12, 12),
    "lamindb.models.transform.Transform": (16, 12),
    "lamindb.models.ulabel.ULabel": (8, 8),
    # --- bionty ---
    "bionty.models.BioRecord": (14, 14),
    "bionty.models.CellLine": (14, 14),
    "bionty.models.CellMarker": (14, 14),
    "bionty.models.CellType": (14, 14),
    "bionty.models.DevelopmentalStage": (14, 14),
    "bionty.models.Disease": (14, 14),
    "bionty.models.Ethnicity": (14, 14),
    "bionty.models.ExperimentalFactor": (14, 14),
    "bionty.models.Gene": (14, 14),
    "bionty.models.Organism": (14, 14),
    "bionty.models.Pathway": (14, 14),
    "bionty.models.Phenotype": (14, 14),
    "bionty.models.Protein": (14, 14),
    "bionty.models.Source": (8, 8),
    "bionty.models.Tissue": (14, 14),
}
DEFAULT_UID_LENGTHS = {length for length, _ in REGISTRY_UID_METADATA.values()}


def validate_lamindb_uid(value: str, registry: str) -> _UIDMetadata:
    """Validate a LaminDB UID string against the expected format for a given registry, and extract metadata."""
    if not all(c in BASE62_CHARSET for c in value):
        raise ValueError(f"UID '{value}' contains non-Base62 characters.")

    # Resolve by full path first, then fall back to short name.
    try:
        uid_length, uid_stem_length = REGISTRY_UID_METADATA[registry]
    except KeyError:
        raise ValueError(f"Unsupported registry '{registry}' for UID validation.")

    metadata = _UIDMetadata(
        registry_name=registry, uid_length=uid_length, uid_stem_length=uid_stem_length
    )
    if len(value) not in (metadata.uid_length, metadata.uid_stem_length):
        raise ValueError(
            f"{metadata.registry_name} UIDs must be {metadata.uid_length} characters long, "
            f"got {len(value)}."
        )
    return metadata


class UID(str):
    """String subclass for registry-aware LaminDB uid validation."""

    __slots__ = ("_uid_length", "_uid_stem_length")
    _uid_length: int
    _uid_stem_length: int | None
    registry_name: str | None = None

    def __new__(cls, value: str, *, registry: str | None = None) -> Self:
        if not isinstance(value, str):
            raise TypeError(f"UID value must be a string, got {type(value)}")
        if registry is None:
            registry = cls.registry_name
        elif cls.registry_name is not None and registry != cls.registry_name:
            raise ValueError(
                f"UID registry '{registry}' does not match expected '{cls.registry_name}'"
            )
        if registry is None:
            if not all(c in BASE62_CHARSET for c in value):
                raise ValueError(f"UID '{value}' contains non-Base62 characters.")
            if (uid_length := len(value)) not in DEFAULT_UID_LENGTHS:
                raise ValueError(
                    f"UID length {uid_length} is not in default lengths and registry is not specified."
                )
            metadata = _UIDMetadata(
                registry_name=None, uid_length=uid_length, uid_stem_length=uid_length
            )
        else:
            metadata = validate_lamindb_uid(value, registry=registry)
        obj = super().__new__(cls, value)
        obj._uid_length = metadata.uid_length
        obj._uid_stem_length = metadata.uid_stem_length
        return obj

    @property
    def uid_length(self) -> int:
        """UID length used for validation."""
        return self._uid_length

    @property
    def stem(self) -> str:
        """Stem uid for versioned registries, else the full uid value."""
        return self[: self._uid_stem_length]

    @property
    def version(self) -> str | None:
        """Version suffix for versioned registries (4 chars), else None."""
        if self._uid_stem_length is not None:
            return self[self._uid_stem_length :] or None
        return None


class RecordUID(UID):
    """UID subclass for Record registry, enforcing 16-character length."""

    registry_name = "lamindb.models.record.Record"


class SchemaUID(UID):
    """UID subclass for Schema registry, enforcing 16-character length."""

    registry_name = "lamindb.models.schema.Schema"


class SourceUID(UID):
    """UID subclass for Source registry, enforcing 8-character length."""

    registry_name = "bionty.models.Source"


class ULabelUID(UID):
    """UID subclass for ULabel registry, enforcing 8-character length."""

    registry_name = "lamindb.models.ulabel.ULabel"


class FeatureUID(UID):
    """UID subclass for Feature registry, enforcing 12-character length."""

    registry_name = "lamindb.models.feature.Feature"


class SpaceUID(UID):
    """UID subclass for Space registry, enforcing 12-character length."""

    registry_name = "lamindb.models.sqlrecord.Space"


# TODO: make more if needed ...
