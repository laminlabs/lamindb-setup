"""Load and validate the `lamin_schemas.yml` CI configuration.

The configuration maps LaminDB instances to the plans that should be managed
against them, so the `ci` subcommands can run `validate`/`plan` for a pull
request and detect drift on a schedule without per-invocation arguments.

The file is a list of instances, each with the plans deployed to it::

    instances:
      - slug: account/instance
        plans:
          - plan_foo.py:foo
          - plan_bar.py:bar
        spaces: [<space-uid>]            # optional: restrict drift reads
        exclude_spaces: [<space-uid>]    # optional: applied after spaces
        track_space: Maintenance         # optional: per-instance apply space
        track_prefix: team/schemas       # optional: per-instance key prefix
    ignored_plans:                       # optional: globs, applied to all
      - plans/plan_drift_*.py            # not managed by any instance
    drift_output_dir: plans              # optional: default `ci drift` output dir
    project_root: .                      # optional: import root for plan files
    track_space: Maintenance             # optional: default apply space
    track_prefix: team/schemas           # optional: default key prefix
    allow_data_validated_changes: true   # optional: lift data-safe blocks
    strict_uids: false                   # optional: apply despite misaligned uids
    strict_renames: false                # optional: apply undeclared renames
    preview_migrations: true             # optional: rehearse in `ci plan`

Plan target paths are resolved relative to the configuration file's directory.
The optional top-level `ignored_plans` is a list of glob patterns (resolved
relative to the same directory) for committed plan files that are intentionally
not deployed to any instance; `ci verify-config` exempts them from the orphan
check but forbids them from carrying a lock file.  The optional top-level
`drift_output_dir` (also resolved relative to the same directory) is the
directory `ci drift` writes each `plan_drift_<slug>.py` into when no
`--output-dir` is given on the command line.

The optional top-level `project_root` (resolved relative to the same
directory) is the directory plan files are imported relative to, so one plan can
import another by its full dotted path; it defaults to the configuration file's
own directory (see :mod:`lamindb_setup.evolve.engine._loader`).

The optional `track_space` / `track_prefix` keys name the LaminDB space that
an apply records its runs, transforms and outputs in, and the prefix their
transform keys are built under.  A per-instance value wins over the top-level
default; both take precedence over the `LAMIN_SCHEMA_EVOLUTION_TRACK_SPACE` /
`LAMIN_SCHEMA_EVOLUTION_TRACK_PREFIX` environment variables, which are the
fallback when the config names neither.  Both `apply -c` and `ci apply`
honour them (see :meth:`CIConfig.track_config_for`).

The three optional top-level switches below are the configuration-file
counterparts of the flags of the same name.  `ci plan`, `ci apply` and
`apply -c` honour all three alike, so one file means the same to every command
that reads it.

`allow_data_validated_changes` (a boolean, default `false`) opts in to
lifting a blocked validation-affecting change to a feature when the live data
proves it safe -- the feature has no stored values, or every value validates
against the new dtype.  It is ORed with `--allow-data-validated-changes`:
either one opts the run in.

`strict_uids` (a boolean, default `true`) set to `false` lets an apply go
ahead while an object's uid differs from the uid the plan's lock files for its
other instances record -- the counterpart of `--no-strict-uids`.  It is ANDed
with `--strict-uids`: either one turns the check off.  `ci plan` then
reports a misaligned uid but does not count it as a pending change, so
`--fail-on-diff` passes.

`strict_renames` (a boolean, default `true`) set to `false` lets an apply
go ahead with a delete + create that looks like a rename not declared with
`ctx.moved` -- the counterpart of `--no-strict-renames`, ANDed with
`--strict-renames` the same way.  `ci plan` then only warns of such a
rename instead of failing the diff.

`preview_migrations` (a boolean, default `false`) is read by `ci plan`
alone, the counterpart of its `--preview-migrations` flag and ORed with it:
`true` rehearses each instance's plans in a transaction that is rolled back,
so the report shows what every declared migration would do to the data.  The
rehearsal *attempts writes*, so the account `ci plan` runs as needs write
access to the instance.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING

from ..core import InvalidArgumentError, LaminSchemasError
from ..engine import ProjectRootError, parse_project_root

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ..engine import TrackConfig

__all__ = [
    "CIConfig",
    "ConfigError",
    "InstanceConfig",
    "PlanRef",
    "load_config",
]


class ConfigError(LaminSchemasError):
    """Raised when a CI configuration file is missing or malformed."""


@dataclass(frozen=True)
class PlanRef:
    """A reference to a plan in the configuration, as `path[:variable]`."""

    target: str
    """The original `path/to/plan.py:variable` string from the config."""
    path: Path
    """The plan file path, resolved relative to the configuration directory."""
    var: str | None
    """The optional `:variable` suffix, or `None` when not given."""

    @property
    def load_target(self) -> str:
        """Return the `path:var` string to hand to the CLI plan loader."""
        return f"{self.path}:{self.var}" if self.var else str(self.path)

    @property
    def identity(self) -> tuple[Path, str | None]:
        """Return the canonical file and optional variable that identify the plan."""
        return self.path.resolve(), self.var


@dataclass
class InstanceConfig:
    """One instance and the plans that should be managed against it."""

    slug: str
    plans: list[PlanRef]
    spaces: list[str] = field(default_factory=list)
    exclude_spaces: list[str] = field(default_factory=list)
    track_space: str | None = None
    """Space that an apply records this instance's runs/outputs in, if set."""
    track_prefix: str | None = None
    """Transform key prefix for this instance's tracked runs, if set."""


@dataclass
class CIConfig:
    """The parsed CI configuration: instances and their plans."""

    instances: list[InstanceConfig]
    config_path: Path
    ignored_plans: list[str] = field(default_factory=list)
    drift_output_dir: Path | None = None
    """Default directory for `ci drift` output, resolved from the config dir."""
    project_root: Path | None = None
    """Directory plan files are imported relative to; defaults to the config dir."""
    track_space: str | None = None
    """Default space for an apply when an instance sets no `track_space`."""
    track_prefix: str | None = None
    """Default key prefix when an instance sets no `track_prefix`."""
    allow_data_validated_changes: bool = False
    """Lift data-safe blocks when the live data allows; ORed with the flag."""
    strict_uids: bool = True
    """Refuse to apply a uid that differs between instances; ANDed with the flag."""
    strict_renames: bool = True
    """Refuse to apply an undeclared rename; ANDed with the flag."""
    preview_migrations: bool = False
    """Rehearse the migrations in `ci plan`; ORed with the flag."""

    @property
    def import_root(self) -> Path:
        """Return the directory the configured plan files are imported relative to.

        The configured `project_root` when set, else the configuration file's
        own directory -- which is already what plan target paths resolve against.
        """
        return self.project_root or self.config_path.parent.resolve()

    def select(self, slugs: Sequence[str] | None) -> list[InstanceConfig]:
        """Return the configured instances named by `slugs`, in configuration order.

        `None` (or no slug at all) selects every instance.  The order is the
        configuration's, not the order the slugs are given in, since `ci apply`
        relies on it; a slug named twice selects its instance once.  A slug the
        configuration does not list raises :class:`InvalidArgumentError` naming
        the configured ones: it is a typo in the argument, and a run that
        silently skipped the instance would look like a run that succeeded.
        """
        if not slugs:
            return list(self.instances)
        wanted = set(slugs)
        configured = [instance.slug for instance in self.instances]
        unknown = [slug for slug in dict.fromkeys(slugs) if slug not in configured]
        if unknown:
            raise InvalidArgumentError(
                f"unknown instance(s) {', '.join(map(repr, unknown))}: "
                f"{self.config_path} configures {', '.join(configured)}"
            )
        return [instance for instance in self.instances if instance.slug in wanted]

    def restricted_to(self, slugs: Sequence[str] | None) -> CIConfig:
        """Return this configuration narrowed to the instances named by `slugs`.

        Every other setting is kept; see :meth:`select` for the selection.
        """
        if not slugs:
            return self
        return replace(self, instances=self.select(slugs))

    def plan_targets(self) -> list[str]:
        """Return the unique plan target strings across all instances."""
        seen: set[tuple[Path, str | None]] = set()
        targets: list[str] = []
        for instance in self.instances:
            for plan in instance.plans:
                if plan.identity in seen:
                    continue
                seen.add(plan.identity)
                targets.append(plan.target)
        return targets

    def track_space_for(self, slug: str) -> str | None:
        """Return the configured `track_space` for the instance `slug`.

        A matching instance's own `track_space` wins over the top-level
        default; returns `None` when neither is set (callers then fall back to
        the `LAMIN_SCHEMA_EVOLUTION_TRACK_SPACE` environment variable).
        """
        for instance in self.instances:
            if instance.slug == slug and instance.track_space is not None:
                return instance.track_space
        return self.track_space

    def track_prefix_for(self, slug: str) -> str | None:
        """Return the configured `track_prefix` for the instance `slug`.

        A matching instance's own `track_prefix` wins over the top-level
        default; returns `None` when neither is set (callers then fall back to
        the `LAMIN_SCHEMA_EVOLUTION_TRACK_PREFIX` environment variable).
        """
        for instance in self.instances:
            if instance.slug == slug and instance.track_prefix is not None:
                return instance.track_prefix
        return self.track_prefix

    def track_config_for(self, slug: str) -> TrackConfig:
        """Return the :class:`TrackConfig` an apply to `slug` should run under.

        Folds this configuration's `track_prefix`/`track_space` over the
        environment, so one resolved object carries both settings down into the
        tracking (see :meth:`TrackConfig.resolve`).
        """
        from ..engine import TrackConfig

        return TrackConfig.resolve(
            prefix=self.track_prefix_for(slug), space=self.track_space_for(slug)
        )


_INSTANCE_KEYS = frozenset(
    {"slug", "plans", "spaces", "exclude_spaces", "track_space", "track_prefix"}
)


def _parse_track_setting(value: object, *, where: str) -> str | None:
    """Validate an optional non-blank string `track_*` setting."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where} must be a non-empty string")
    return value.strip()


def _parse_bool(value: object, *, where: str, default: bool = False) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ConfigError(f"{where} must be a boolean")
    return value


def _parse_target(target: object, base_dir: Path) -> PlanRef:
    if not isinstance(target, str) or not target.strip():
        raise ConfigError(f"plan entry must be a non-empty string, got {target!r}")
    path_str, sep, var = target.partition(":")
    if sep and not var:
        raise ConfigError(
            f"plan {target!r} must be of the form 'path/to/plan.py:plan_variable'"
        )
    path = Path(path_str)
    if not path.is_absolute():
        path = base_dir / path
    path = path.resolve()
    if not path.exists():
        raise ConfigError(f"plan file not found: {path} (from {target!r})")
    return PlanRef(target=target, path=path, var=(var or None))


def _str_list(value: object, *, where: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ConfigError(f"{where} must be a list of strings")
    return [v for v in value if isinstance(v, str)]


def _parse_drift_output_dir(value: object, base_dir: Path) -> Path | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigError("'drift_output_dir' must be a non-empty string")
    path = Path(value)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def _parse_project_root(value: object, base_dir: Path) -> Path | None:
    try:
        return parse_project_root(value, base_dir)
    except ProjectRootError as error:
        raise ConfigError(str(error)) from error


def _parse_instance(raw: object, index: int, base_dir: Path) -> InstanceConfig:
    if not isinstance(raw, dict):
        raise ConfigError(
            f"instances[{index}] must be a mapping, got {type(raw).__name__}"
        )
    unknown = set(raw) - _INSTANCE_KEYS
    if unknown:
        raise ConfigError(
            f"instances[{index}] has unknown key(s): {', '.join(sorted(map(str, unknown)))}; "
            f"allowed keys are: {', '.join(sorted(_INSTANCE_KEYS))}"
        )
    slug = raw.get("slug")
    if not isinstance(slug, str) or not slug.strip():
        raise ConfigError(f"instances[{index}] requires a non-empty 'slug' string")
    plans_raw = raw.get("plans")
    if not isinstance(plans_raw, list) or not plans_raw:
        raise ConfigError(f"instance {slug!r} requires a non-empty 'plans' list")
    plans = [_parse_target(target, base_dir) for target in plans_raw]
    seen_plans: dict[Path, list[PlanRef]] = {}
    for plan in plans:
        for first in seen_plans.get(plan.path, []):
            if first.var == plan.var or first.var is None or plan.var is None:
                raise ConfigError(
                    f"instance {slug!r} lists plan {plan.target!r} more than once "
                    f"(first as {first.target!r})"
                )
        seen_plans.setdefault(plan.path, []).append(plan)
    return InstanceConfig(
        slug=slug,
        plans=plans,
        spaces=_str_list(raw.get("spaces"), where=f"instance {slug!r} 'spaces'"),
        exclude_spaces=_str_list(
            raw.get("exclude_spaces"), where=f"instance {slug!r} 'exclude_spaces'"
        ),
        track_space=_parse_track_setting(
            raw.get("track_space"), where=f"instance {slug!r} 'track_space'"
        ),
        track_prefix=_parse_track_setting(
            raw.get("track_prefix"), where=f"instance {slug!r} 'track_prefix'"
        ),
    )


def load_config(path: str | Path) -> CIConfig:
    """Read and validate the CI configuration at `path`.

    Plan target paths are resolved relative to `path`'s directory.  Raises
    :class:`ConfigError` if the file is missing, is not valid YAML, or does not
    match the expected structure.
    """
    import yaml  # type: ignore[import-untyped]

    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"configuration file not found: {config_path}")
    try:
        data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"could not parse {config_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"{config_path} must contain a top-level mapping")
    unknown = set(data) - {
        "instances",
        "ignored_plans",
        "drift_output_dir",
        "project_root",
        "track_space",
        "track_prefix",
        "allow_data_validated_changes",
        "strict_uids",
        "strict_renames",
        "preview_migrations",
    }
    if unknown:
        raise ConfigError(
            f"{config_path} has unknown top-level key(s): {', '.join(sorted(map(str, unknown)))}"
        )
    ignored_plans = _str_list(data.get("ignored_plans"), where="'ignored_plans'")
    base_dir = config_path.parent
    drift_output_dir = _parse_drift_output_dir(data.get("drift_output_dir"), base_dir)
    project_root = _parse_project_root(data.get("project_root"), base_dir)
    track_space = _parse_track_setting(data.get("track_space"), where="'track_space'")
    track_prefix = _parse_track_setting(
        data.get("track_prefix"), where="'track_prefix'"
    )
    allow_data_validated_changes = _parse_bool(
        data.get("allow_data_validated_changes"),
        where="'allow_data_validated_changes'",
    )
    strict_uids = data.get("strict_uids", True)
    if not isinstance(strict_uids, bool):
        raise ConfigError("'strict_uids' must be a boolean")
    strict_renames = _parse_bool(
        data.get("strict_renames"), where="'strict_renames'", default=True
    )
    preview_migrations = _parse_bool(
        data.get("preview_migrations"), where="'preview_migrations'"
    )
    instances_raw = data.get("instances")
    if not isinstance(instances_raw, list) or not instances_raw:
        raise ConfigError(f"{config_path} requires a non-empty 'instances' list")

    instances = [
        _parse_instance(raw, index, base_dir) for index, raw in enumerate(instances_raw)
    ]
    seen_slugs: set[str] = set()
    for instance in instances:
        if instance.slug in seen_slugs:
            raise ConfigError(f"duplicate instance slug: {instance.slug!r}")
        seen_slugs.add(instance.slug)
    vars_by_path: dict[Path, set[str | None]] = {}
    for instance in instances:
        for plan in instance.plans:
            vars_by_path.setdefault(plan.path, set()).add(plan.var)
    for path, variables in vars_by_path.items():
        if None in variables and len(variables) > 1:
            raise ConfigError(
                f"plan file {path} is referenced both with and without a plan "
                "variable; use explicit variables consistently"
            )
    return CIConfig(
        instances=instances,
        config_path=config_path,
        ignored_plans=ignored_plans,
        drift_output_dir=drift_output_dir,
        project_root=project_root,
        track_space=track_space,
        track_prefix=track_prefix,
        allow_data_validated_changes=allow_data_validated_changes,
        strict_uids=strict_uids,
        strict_renames=strict_renames,
        preview_migrations=preview_migrations,
    )
