# Source behavior retained; host-repository lint exceptions.
# ruff: noqa: PTH105, PTH108
"""Lock file format and configuration serialization for plans.

A lock file records, for every object a :class:`~lamindb_setup.evolve.Plan`
materializes, a stable natural key, the materialized `uid`, and a canonical,
JSON-serializable snapshot of its configuration.  It plays the same role as a
terraform state file: it is the recorded state of what a plan created, and it is
verified against the live LaminDB instance on the next `apply`.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, get_origin

from ..core import LaminSchemasError, Promise

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ..core import UID

__all__ = [
    "LOCK_VERSION",
    "LockEntry",
    "LockFile",
    "LockFileError",
    "instance_lock_files",
    "lock_path",
    "natural_key",
    "remote_state_hash",
    "serialize_config",
    "serialize_existing",
]


class LockFileError(LaminSchemasError):
    """Raised when a lock file exists but cannot be trusted to say what it records.

    A lock is the recorded state of an apply, so reading it wrongly is worse
    than not reading it: every later decision -- adopt, update, delete -- is
    taken against it. Corrupt files and unsupported versions are refused.
    """


LOCK_VERSION = 6


def _package_version() -> str:
    """Read LaminDB's version without importing its auto-connecting package."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("lamindb-core")
    except PackageNotFoundError:
        return "unknown"


# === Natural keys and configuration serialization ============================


def _name_of(obj: Any) -> str | None:
    name = getattr(obj, "name", None)
    return name if isinstance(name, str) else None


def natural_key(obj: Any) -> str:
    """Return a stable identifier for `obj` across runs.

    The key is built from the object kind, its name, and its `type` parent
    chain.  Objects without a name (e.g. unnamed schemas, which LaminDB
    addresses by content hash) fall back to a short hash of their configuration
    so that distinct objects do not collide.
    """
    chain: list[str] = []
    node: Any = obj
    seen: set[int] = set()
    while node is not None and id(node) not in seen:
        seen.add(id(node))
        kind = type(node).__name__
        name = _name_of(node)
        token = name if name is not None else "#" + _config_hash(node)
        chain.append(f"{kind}:{token}")
        node = getattr(node, "type", None)
    return "/".join(reversed(chain))


def _config_hash(obj: Any) -> str:
    payload = json.dumps(serialize_config(obj), sort_keys=True, default=str)
    return hashlib.sha1(payload.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]


def _jsonify(value: Any) -> Any:
    """Convert `value` into a deterministic, JSON-serializable representation."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Promise):
        return {"ref": natural_key(value.get_arg())}
    if isinstance(value, type):
        return f"{value.__module__}.{value.__qualname__}"
    # FieldAttr / DeferredAttribute and other objects: use a stable string.
    return repr(value)


def _dtype_repr(lamin_dtype: Any) -> str:
    """Describe a :class:`LaminDType` underlying type as a stable string."""
    inner = getattr(lamin_dtype, "dtype", lamin_dtype)
    if get_origin(inner) is list:
        # `list[str]` is a `types.GenericAlias` -- neither a type nor a string,
        # and its `__name__` is a bare `list` -- so it is recorded by lamindb's
        # canonical string, which keeps the element type.
        return getattr(lamin_dtype, "dtype_str", None) or repr(inner)
    if isinstance(inner, type):
        return f"{inner.__module__}.{inner.__qualname__}"
    if isinstance(inner, (list, tuple)):
        # A union categorical (cat[A|B]); use lamindb's canonical string so the
        # drift classifier recognises a registry being added/removed.
        return getattr(lamin_dtype, "dtype_str", None) or repr(inner)
    if isinstance(inner, str):
        # A ResolvedDType or string dtype ('num'/'path'/'url'/'cat[...]'): its
        # canonical lamindb string, recorded verbatim.  Never wrap it in repr()
        # -- a quoted `'cat[...]'` never equals the instance's clean dtype and
        # would surface as spurious configuration drift.
        return getattr(lamin_dtype, "dtype_str", None) or inner
    return getattr(inner, "__name__", None) or repr(inner)


def _cat_filters_repr(lamin_dtype: Any) -> dict[str, Any] | None:
    cat_filters = getattr(lamin_dtype, "cat_filters", None)
    if not cat_filters:
        return None
    return {key: _jsonify(value) for key, value in sorted(cat_filters.items())}


def serialize_config(obj: Any) -> dict[str, Any]:
    """Return a canonical, JSON-serializable snapshot of `obj`'s configuration.

    References to other plan objects (the `type` parent, schema features,
    slots, index, and categorical filters) are stored as natural keys so the
    snapshot is independent of materialized `uid` values and stable across
    runs.
    """
    kind = type(obj).__name__
    cfg: dict[str, Any] = {"kind": kind, "name": _name_of(obj)}

    parent = getattr(obj, "type", None)
    if parent is not None:
        cfg["type"] = natural_key(parent)

    if kind in ("FeatureType", "SchemaType", "RecordType", "Sheet"):
        description = getattr(obj, "description", None)
        if description is not None:
            cfg["description"] = description

    if kind in ("RecordType", "Sheet"):
        single_space_uid = getattr(obj, "single_space_uid", None)
        if single_space_uid is not None:
            cfg["single_space_uid"] = str(single_space_uid)
        if getattr(obj, "exclude_children_from_plans", False):
            cfg["exclude_children_from_plans"] = True

    if kind == "Sheet":
        cfg["schema"] = natural_key(obj.schema)
        if getattr(obj, "is_experiment_sheet", False):
            cfg["is_experiment_sheet"] = True

    if kind == "Feature":
        cfg["dtype"] = _dtype_repr(obj.dtype)
        cat_filters = _cat_filters_repr(obj.dtype)
        if cat_filters is not None:
            cfg["cat_filters"] = cat_filters
        if obj.synonyms:
            cfg["synonyms"] = list(obj.synonyms)
        for attr in ("unit", "description", "nullable", "default_value", "coerce"):
            value = getattr(obj, attr, None)
            if value is not None:
                cfg[attr] = _jsonify(value)

    if kind == "Schema":
        cfg["features"] = [natural_key(f) for f in obj.features]
        optionals = getattr(obj, "optionals", None)
        if optionals:
            cfg["optionals"] = sorted(natural_key(f) for f in optionals)
        for attr in (
            "description",
            "otype",
            "flexible",
            "coerce",
            "minimal_set",
            "ordered_set",
            "maximal_set",
        ):
            value = getattr(obj, attr, None)
            if value is not None:
                cfg[attr] = _jsonify(value)
        itype = getattr(obj, "itype", None)
        if itype is not None:
            cfg["itype"] = _jsonify(itype)
        dtype = getattr(obj, "dtype", None)
        if dtype is not None:
            cfg["dtype"] = _jsonify(dtype)
        formats = getattr(obj, "formats", None)
        if formats:
            cfg["formats"] = formats
        index = getattr(obj, "index", None)
        if index is not None:
            cfg["index"] = natural_key(index)
        slots = getattr(obj, "slots", None)
        if slots:
            cfg["slots"] = {key: natural_key(value) for key, value in slots.items()}

    if kind == "Record":
        features = getattr(obj, "features", None)
        if features:
            serialized: dict[str, Any] = {}
            for key, value in features.items():
                ref = natural_key(key) if not isinstance(key, str) else key
                serialized[ref] = _jsonify(value)
            cfg["features"] = serialized
        for attr in ("description", "reference", "reference_type"):
            value = getattr(obj, attr, None)
            if value is not None:
                cfg[attr] = _jsonify(value)

    return cfg


# === Remote read-back snapshot ===============================================


def _existing_uid(obj: Any) -> str | None:
    """Return `str(obj.uid)` for a live LaminDB record, or `None`."""
    if obj is None:
        return None
    uid = getattr(obj, "uid", None)
    return str(uid) if uid is not None else None


def _existing_slots(existing: Any) -> dict[str, str | None]:
    """A composite schema's slots as `{slot: component uid}`, read from its links.

    Read from the link table rather than through `Schema.slots`, which caches
    the dict on the record -- and lamindb's `save` writes a cached dict back.
    """
    components = getattr(type(existing), "components", None)
    through = getattr(components, "through", None)
    if through is None or getattr(existing, "pk", None) is None:
        return {}
    links = through.objects.filter(composite_id=existing.pk).select_related("component")
    return {link.slot: _existing_uid(link.component) for link in links}


def serialize_existing(
    existing: Any,
    kind: str,
    *,
    members: list[Any] | None = None,
    slots: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    """Return a canonical snapshot of a *live* LaminDB record's relevant state.

    Unlike :func:`serialize_config`, which serializes a plan object, this reads
    a materialized LaminDB record.  It is used only for *remote-to-remote*
    comparison (the state recorded at the last apply versus the state now), so
    it only needs to be **deterministic across runs**, not identical to
    :func:`serialize_config`.  References are captured by the referenced
    record's `uid` (identity), and a :class:`~lamindb.Schema`'s structural
    content is captured by LaminDB's own content `hash`.

    `members` optionally supplies a schema's already-materialized member
    records so the snapshot reuses them instead of re-querying
    `existing.members` (a computed property that issues two SQL statements per
    access).  The caller is responsible for passing the members that match
    `existing`; when `None` the members are fetched lazily as before.
    `slots` likewise supplies a schema's `{slot: component uid}` (`{}` for
    one known to have none), read from its links when `None`.
    """
    snap: dict[str, Any] = {"kind": kind, "name": getattr(existing, "name", None)}

    parent = getattr(existing, "type", None)
    if parent is not None:
        snap["type"] = _existing_uid(parent)

    space = getattr(existing, "space", None)
    if space is not None:
        space_uid = _existing_uid(space)
        if space_uid is not None:
            snap["space_uid"] = space_uid

    if kind in ("FeatureType", "SchemaType", "RecordType", "Sheet"):
        description = getattr(existing, "description", None)
        if description is not None:
            snap["description"] = description

    if kind in ("RecordType", "Sheet"):
        aux = getattr(existing, "_aux", None) or {}
        ss = aux.get("ss")
        if ss is not None:
            if ss == 1:
                # `ss == 1` enforces the record type's own space; resolve it
                # to that uid so the snapshot matches a plan naming that space.
                space_uid = _existing_uid(getattr(existing, "space", None))
                if space_uid is not None:
                    snap["single_space_uid"] = space_uid
            else:
                snap["single_space_uid"] = str(ss)
        if aux.get("_excludeChildrenFromPlans"):
            snap["exclude_children_from_plans"] = True
        if aux.get("isExperimentSheet"):
            snap["is_experiment_sheet"] = True
        # Recorded for both kinds: what the plan calls the record says nothing
        # about whether it has a schema -- a sheet demoted to a plain record
        # type still has one until the apply drops it, and one attached outside
        # the plan turns a record type into a sheet.  `None` is recorded too,
        # so the snapshot explicitly records an unbound schema.
        snap["schema"] = _existing_uid(getattr(existing, "schema", None))

    if kind == "Feature":
        if hasattr(existing, "dtype_as_str"):
            snap["dtype"] = getattr(existing, "dtype_as_str", None)
        else:
            snap["dtype"] = getattr(existing, "dtype", None)
        synonyms = getattr(existing, "synonyms", None)
        if synonyms:
            snap["synonyms"] = synonyms
        for attr in ("unit", "description", "nullable", "default_value", "coerce"):
            value = getattr(existing, attr, None)
            if value is not None:
                snap[attr] = _jsonify(value)

    if kind == "Schema":
        content_hash = getattr(existing, "hash", None)
        if content_hash is not None:
            snap["hash"] = content_hash
        slot_uids = _existing_slots(existing) if slots is None else slots
        if slot_uids:
            snap["slots"] = slot_uids
        if members is not None:
            snap["features"] = [str(member.uid) for member in members]
        else:
            member_qs = getattr(existing, "members", None)
            if member_qs is not None:
                snap["features"] = [str(member.uid) for member in member_qs.all()]
        # The optional members live in `Schema._aux` and are *not* covered by
        # the member list above (which records every member, optional or not) --
        # nor usefully by `hash`, which the drift check skips as an opaque
        # digest.  `get_uids` reads the already-loaded aux, so this costs no
        # query.  Recorded only when non-empty, so a schema without optionals
        # keeps the key absent rather than diffing `None` against `[]`.
        optionals = getattr(existing, "optionals", None)
        optional_uids = sorted(optionals.get_uids()) if optionals is not None else []
        if optional_uids:
            snap["optionals"] = optional_uids
        # The index feature, which lamindb also keeps in `Schema._aux` rather
        # than on the member links, so it is not covered by the member list either.
        # Read straight from the already-loaded aux (no query), and recorded only
        # when set, for the same reason as `optionals`: a schema without an
        # index keeps the key absent.
        index_uid = getattr(existing, "_index_feature_uid", None)
        if index_uid is not None:
            snap["index"] = str(index_uid)
        for attr in (
            "description",
            "otype",
            "flexible",
            "coerce",
            "minimal_set",
            "ordered_set",
            "maximal_set",
            "itype",
            "dtype",
        ):
            value = getattr(existing, attr, None)
            if value is not None:
                snap[attr] = _jsonify(value)
        zarr = (getattr(existing, "_aux", None) or {}).get("zarr")
        if zarr:
            snap["formats"] = {"zarr": zarr}

    if kind == "Record":
        for attr in ("description", "reference", "reference_type"):
            value = getattr(existing, attr, None)
            if value is not None:
                snap[attr] = _jsonify(value)

    return snap


def remote_state_hash(snapshot: dict[str, Any]) -> str:
    """Return a stable sha1 of a remote-state `snapshot` for fast comparison."""
    payload = json.dumps(snapshot, sort_keys=True, default=str)
    return hashlib.sha1(payload.encode("utf-8"), usedforsecurity=False).hexdigest()


# === Lock file =================================================================


@dataclass
class LockEntry:
    """A single recorded object: its natural key, uid, and configuration.

    `remote_snapshot` is a canonical snapshot of the live LaminDB record as it
    looked at the last successful apply (see :func:`serialize_existing`), and
    `remote_hash` is its :func:`remote_state_hash`.  Together they let the next
    apply tell remote-side drift apart from intentional local plan edits.
    """

    key: str
    kind: str
    name: str | None
    uid: str | None
    config: dict[str, Any] = field(default_factory=dict)
    remote_snapshot: dict[str, Any] | None = None
    remote_hash: str | None = None

    def to_json(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "kind": self.kind,
            "name": self.name,
            "uid": self.uid,
            "config": self.config,
        }
        if self.remote_snapshot is not None:
            data["remote_snapshot"] = self.remote_snapshot
        if self.remote_hash is not None:
            data["remote_hash"] = self.remote_hash
        return data

    @classmethod
    def from_json(cls, key: str, data: dict[str, Any]) -> LockEntry:
        return cls(
            key=key,
            kind=data["kind"],
            name=data.get("name"),
            uid=data.get("uid"),
            config=data.get("config", {}),
            remote_snapshot=data.get("remote_snapshot"),
            remote_hash=data.get("remote_hash"),
        )


@dataclass
class LockFile:
    """The recorded state of a plan: an ordered mapping of natural keys to entries.

    `seeded` maps the natural key of every seed-only object the plan declares
    (see :data:`~lamindb_setup.evolve.core.SEED_ONLY_KINDS`) to the uid it
    resolved to.  It is identity only: a seeded object is never drift-checked,
    reconciled or deleted on the strength of it.  It exists so the uid can be
    pinned when the plan is applied to another instance, and aligned across
    instances by `repair align-uids`.
    """

    plan: str
    version: int = LOCK_VERSION
    lamindb_version: str = field(default_factory=_package_version)
    objects: dict[str, LockEntry] = field(default_factory=dict)
    seeded: dict[str, str] = field(default_factory=dict)

    def entry_for(self, key: str) -> LockEntry | None:
        """Return the recorded entry for `key` if present."""
        return self.objects.get(key)

    def uid_for(self, key: str) -> str | None:
        """Return the recorded uid for `key` if present."""
        entry = self.objects.get(key)
        return entry.uid if entry is not None else None

    def add(self, entry: LockEntry) -> None:
        """Record `entry` (replacing any existing entry with the same key)."""
        self.objects[entry.key] = entry

    def remove(self, key: str) -> bool:
        """Drop the entry recorded for `key`; return whether one was present."""
        tracked = self.objects.pop(key, None) is not None
        seeded = self.seeded.pop(key, None) is not None
        return tracked or seeded

    @property
    def is_empty(self) -> bool:
        """Whether the lock records nothing at all, tracked or seeded."""
        return not self.objects and not self.seeded

    def to_json(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "version": self.version,
            "lamindb_version": self.lamindb_version,
            "plan": self.plan,
            "objects": {key: entry.to_json() for key, entry in self.objects.items()},
        }
        if self.seeded:
            data["seeded"] = dict(self.seeded)
        return data

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> LockFile:
        version = data.get("version")
        if type(version) is not int or version != LOCK_VERSION:
            raise LockFileError(
                f"Unsupported lock version {version!r}; expected {LOCK_VERSION}."
            )
        objects = {
            key: LockEntry.from_json(key, value)
            for key, value in data.get("objects", {}).items()
        }
        seeded = data.get("seeded") or {}
        if not isinstance(seeded, dict):
            raise TypeError(f"'seeded' must map keys to uids, got {seeded!r}")
        return cls(
            plan=data.get("plan", ""),
            lamindb_version=data.get("lamindb_version", _package_version()),
            objects=objects,
            seeded={str(key): str(uid) for key, uid in seeded.items()},
        )

    @classmethod
    def read(cls, path: str | Path) -> LockFile | None:
        """Read a lock file from `path`, or return `None` if it does not exist.

        The two ways a lock can be unreadable are refused rather than guessed
        at, because both would otherwise be acted on as if the file said
        something it does not.  See :class:`LockFileError`.
        """
        p = Path(path)
        if not p.exists():
            return None
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LockFileError(
                f"{p} is not readable as a lock file ({exc}). A lock records what "
                "the last apply created, so it cannot be regenerated from the plan "
                "alone: restore it from version control. If it was never committed, "
                "and only if you accept that the objects it recorded will be looked "
                "up by name and re-adopted (or re-created) from scratch, delete it "
                "and re-apply."
            ) from exc
        if not isinstance(data, dict):
            raise LockFileError(
                f"{p} does not contain a lock object (found "
                f"{type(data).__name__}). Restore it from version control."
            )
        return cls._from_json_checked(data, p)

    @classmethod
    def _from_json_checked(cls, data: dict[str, Any], path: Path) -> LockFile:
        """Deserialize `data`, reporting a wrong-shaped lock as :class:`LockFileError`.

        Valid JSON is not yet a valid lock: a hand-edited or truncated-then-
        repaired file can parse and still be missing the fields an entry is made
        of.  Naming the file beats a bare `KeyError` from deep in the read.
        """
        try:
            return cls.from_json(data)
        except LockFileError as exc:
            raise LockFileError(f"{path}: {exc}") from exc
        except (KeyError, TypeError, AttributeError) as exc:
            raise LockFileError(
                f"{path} is a lock file with a malformed entry ({exc!r}). Restore "
                "it from version control."
            ) from exc

    def write(self, path: str | Path) -> None:
        """Write the lock file to `path` as indented, sorted JSON.

        The write is atomic: the content is written to a temporary file in the
        same directory and then renamed over `path`, so a crash mid-write
        never leaves a partially written (corrupt) lock file behind.
        """
        target = Path(path)
        text = json.dumps(self.to_json(), indent=2, sort_keys=True) + "\n"
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            dir=target.parent, prefix=f".{target.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, target)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_name)
            raise


def lock_path(plan_file: Path, instance_slug: str) -> Path:
    """Return the lock-file path for `plan_file` against a specific instance.

    The same plan may be applied to several instances (e.g. dev/staging/prod),
    each assigning its own `uid` values, so the lock is kept per-instance:
    `plan_foo.py` against instance `acme/prod` locks to
    `plan_foo.acme-prod.lock`.
    """
    return plan_file.with_name(f"{plan_file.stem}.{instance_slug}.lock")


# Directories never holding a plan's lock files, skipped when looking for them.
_SKIP_DIRS: frozenset[str] = frozenset(
    {"__pycache__", "node_modules", "site-packages", "build", "dist", "htmlcov"}
)


def instance_lock_files(roots: Iterable[Path], instance_slug: str) -> list[Path]:
    """Return every lock file against `instance_slug` under `roots`, resolved.

    Walks each root (typically a plan's :func:`~._loader.project_root`) for the
    `<plan stem>.<instance_slug>.lock` files :func:`lock_path` names, skipping
    hidden directories and the ones that never hold plans (:data:`_SKIP_DIRS`).
    A lock found under several roots is listed once.
    """
    found: dict[Path, None] = {}
    suffix = f".{instance_slug}.lock"
    for root in dict.fromkeys(Path(root).resolve() for root in roots):
        for directory, dirnames, filenames in os.walk(root):
            dirnames[:] = sorted(
                d for d in dirnames if not d.startswith(".") and d not in _SKIP_DIRS
            )
            for filename in sorted(filenames):
                if filename.endswith(suffix):
                    found.setdefault((Path(directory) / filename).resolve())
    return list(found)


def coerce_uid(uid: str, kind: str) -> UID | str:
    """Return a typed :class:`UID` for `kind` when possible, else the raw string."""
    from ..core import FeatureUID, RecordUID, SchemaUID

    registry = {
        "FeatureType": FeatureUID,
        "Feature": FeatureUID,
        "SchemaType": SchemaUID,
        "Schema": SchemaUID,
        "RecordType": RecordUID,
        "Sheet": RecordUID,
        "Record": RecordUID,
    }.get(kind)
    if registry is None:
        return uid
    try:
        return registry(uid)
    except ValueError:
        return uid


def registry_for_kind(kind: str) -> Any:
    """Return the LaminDB registry class backing `kind` (e.g. `ln.Feature`)."""
    import lamindb as ln

    return {
        "FeatureType": ln.Feature,
        "Feature": ln.Feature,
        "SchemaType": ln.Schema,
        "Schema": ln.Schema,
        "RecordType": ln.Record,
        "Sheet": ln.Record,
        "Record": ln.Record,
    }[kind]
