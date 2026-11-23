# Source behavior retained; host-repository lint exceptions.
# ruff: noqa: PTH118
"""Import plan files under the module name a project root gives them.

A plan file is a plain Python module, and plans import each other to reuse the
objects they declare.  Importing a plan *by location* under a synthetic name --
what the loader used to do -- gave one file a different module name depending on
how it was reached: as an entry target it became `_lamin_schema_plan_foo`, and
imported from a sibling plan it became `plan_foo`.  Both were executed, so the
plan it defines existed twice, as two :class:`~lamindb_setup.evolve.Plan`
objects over two sets of `Feature`/`Schema` objects that `apply` -- which
keys its caches on `id()` -- cannot see as one.

This loader resolves a *project root* per plan file and imports the file as the
dotted module name it has underneath that root (`plans.wetlab.plan_foo`).  The
name is canonical: reaching the file as an entry target and reaching it through
an absolute import from another plan yield the same `sys.modules` entry, hence
one execution.  It also makes absolute cross-directory imports resolve without
`PYTHONPATH`, since the root is on `sys.path` while the plan is imported.  No
`__init__.py` is needed anywhere -- the intermediate directories are namespace
packages (PEP 420).

The root is the first of:

1. an override in force for the run -- `--project-root`, or the root a
   configuration file passed with `--config` gives (:func:`project_root_override`),
2. the `project_root` of the nearest ancestor directory holding a
   `lamin_schemas.yml`, else that directory; then the nearest ancestor holding
   a `pyproject.toml`, then a `.git`,
3. the plan file's own directory (the loader's original behavior).

`.git` as the last marker keeps module names aligned with the git-root-relative
paths that :mod:`lamindb_setup.evolve.engine._track` derives transform keys from.

Nested plans import siblings by their package-qualified name or a relative import.

On top of that sits the layer that turns the `path/to/plan.py:plan_variable`
targets a command (or a `lamin_schemas.yml` entry) is given into the plans it
acts on: :func:`load_plan`, :func:`load_all_plans` and
:func:`order_dependencies_first`.
"""

from __future__ import annotations

import contextlib
import importlib
import importlib.util
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from ..core import InvalidArgumentError, LaminSchemasError

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from types import ModuleType

    from ..authoring import Plan

__all__ = [
    "PlanImportError",
    "PlanTargetError",
    "StemUidError",
    "co_applied_prerequisite_keys",
    "import_plan_module",
    "load_all_plans",
    "load_plan",
    "load_plans_in_file",
    "order_dependencies_first",
    "parse_target",
    "plan_target",
    "project_root",
    "project_root_override",
    "require_stem_uid",
    "resolve_plan_var",
    "sibling_claimed_keys",
]

_CONFIG_MARKER = "lamin_schemas.yml"

_ROOT_MARKERS = (_CONFIG_MARKER, "pyproject.toml", ".git")

_root_override: Path | None = None


class ProjectRootError(LaminSchemasError):
    """A declared `project_root` is missing or malformed."""


class PlanImportError(LaminSchemasError):
    """Raised when a plan file cannot be imported at all."""


class PlanTargetError(InvalidArgumentError):
    """A `path/to/plan.py:plan_variable` target does not name a single plan.

    The file is missing or fails to import, it defines no plan or several, the
    variable is not a plan, or the same plan is listed twice.  The CLI reports
    it against the command's arguments, as a bad parameter.
    """


class StemUidError(LaminSchemasError):
    """A plan's `PLAN_TRANSFORM_STEM_UID` is missing or malformed, or cannot be added."""


@contextlib.contextmanager
def project_root_override(root: Path | None) -> Iterator[None]:
    """Resolve plan imports against `root` for the duration of the block.

    An override already in force wins, so the explicit `--project-root` of a
    run is not displaced by the root a configuration file implies.  `None`
    leaves the resolution to :func:`project_root`.
    """
    global _root_override

    if root is None or _root_override is not None:
        yield
        return
    previous = _root_override
    _root_override = Path(root).resolve()
    try:
        yield
    finally:
        _root_override = previous


def parse_project_root(value: object, base_dir: Path) -> Path | None:
    """Resolve a declared `project_root` against `base_dir`.

    What a `project_root` key means is the loader's business, not the CI
    configuration's: the loader is what turns one into a module name.  Keeping
    the rule here lets both the walk-up in :func:`project_root` and
    `lamin_schemas.yml` validation share it without the loader having to
    reach into :mod:`lamindb_setup.evolve.ci`.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ProjectRootError("'project_root' must be a non-empty string")
    path = Path(value)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    if not path.is_dir():
        raise ProjectRootError(f"'project_root' is not a directory: {path}")
    return path


def _root_from_config(config: Path) -> Path | None:
    """Return the `project_root` a `lamin_schemas.yml` declares, if any.

    A configuration that cannot be read is not this walk-up's business -- the
    command actually pointed at one reports what is wrong with it -- so anything
    unreadable leaves the file's own directory as the root.
    """
    import yaml  # type: ignore[import-untyped]

    try:
        data = yaml.safe_load(config.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError):
        return None
    if not isinstance(data, dict):
        return None
    try:
        return parse_project_root(data.get("project_root"), config.parent)
    except ProjectRootError:
        return None


def project_root(path: Path) -> Path:
    """Return the directory the module name of the plan file `path` is relative to."""
    if _root_override is not None:
        return _root_override
    for marker in _ROOT_MARKERS:
        for candidate in path.parents:
            found = candidate / marker
            if not found.exists():
                continue
            declared = _root_from_config(found) if marker == _CONFIG_MARKER else None
            return declared or candidate
    return path.parent


def _dotted_name(path: Path, root: Path) -> str | None:
    """Return the module name `path` has when imported from `root`.

    `None` when `path` is not under `root`, or when a path segment is not
    a valid identifier (`plans/my-plans/plan_x.py`) and therefore unreachable
    by any import statement.
    """
    try:
        parts = path.relative_to(root).with_suffix("").parts
    except ValueError:
        return None
    if not parts or not all(part.isidentifier() for part in parts):
        return None
    return ".".join(parts)


@contextlib.contextmanager
def _dir_on_syspath(directory: Path) -> Iterator[None]:
    """Put `directory` at the front of `sys.path` for the duration of the block.

    The entry is removed afterwards so it does not leak into later plan loads.
    """
    entry = str(directory)
    already_present = entry in sys.path
    if not already_present:
        sys.path.insert(0, entry)
    try:
        yield
    finally:
        if not already_present:
            with contextlib.suppress(ValueError):
                sys.path.remove(entry)


def _import_dotted(dotted: str, path: Path) -> ModuleType | None:
    """Import `path` as `dotted`, or return `None` if that name is not it.

    The name can belong to something else -- a stdlib module (a `types.py`
    sitting at the root), an installed distribution, a package already imported
    from elsewhere -- and importing that instead would silently run the wrong
    code and hand back a module without the plan.
    """
    try:
        spec = importlib.util.find_spec(dotted)
    except (ImportError, ValueError):
        return None
    if spec is None or spec.origin is None:
        return None
    if Path(spec.origin).resolve() != path:
        return None
    return importlib.import_module(dotted)


def _import_by_location(path: Path) -> ModuleType:
    """Import `path` under a synthetic module name, bypassing `sys.path`."""
    module_name = f"_lamin_schema_plan_{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise PlanImportError(f"could not import plan file: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _reject_shadowed_plan_modules(root: Path) -> None:
    """Reject noncanonical imports of nested plans."""
    directory = os.path.join(root, "")
    # An explicit PYTHONPATH or the working directory can still make a nested
    # plan importable by its bare name. Catch that even on its first import.
    for name, module in sys.modules.copy().items():
        if module is None or name.startswith("_lamin_schema_plan_"):
            continue
        file = vars(module).get("__file__")
        if not isinstance(file, str) or not file.startswith(directory):
            continue
        path = Path(file)
        if path.stem != name.rsplit(".", 1)[-1] or path.parent == root:
            continue
        canonical = _dotted_name(path, root)
        if canonical and canonical != name and _defined_plan_vars(module):
            raise PlanImportError(
                f"Import {file} as {canonical!r} instead of {name!r}."
            )


def import_plan_module(path: Path) -> ModuleType:
    """Import the plan file at `path` and return the resulting module.

    The plan file is imported before any LaminDB connection is made, so plan
    construction succeeds under the `_ensure_lamindb_not_loaded` guard.  The
    whole run holds :func:`no_default_instance`, so a transitive `import
    lamindb` cannot auto-connect to the user's default instance either.

    A file already imported under its canonical name is *not* executed again;
    the cached module is returned, so the plan it defines is one object no
    matter how many targets or instances reference it.
    """
    resolved = path.resolve()
    root = project_root(resolved)
    try:
        with _dir_on_syspath(root):
            dotted = _dotted_name(resolved, root)
            module = _import_dotted(dotted, resolved) if dotted is not None else None
            if module is None:
                module = _import_by_location(resolved)
    except ModuleNotFoundError as exc:
        raise PlanImportError(
            f"Could not import {resolved}: {exc}. "
            f"Import sibling plans relative to the project root {root} "
            "or use a package-relative import."
        ) from exc
    _reject_shadowed_plan_modules(root)
    return module


# --- from a target to the plans a command acts on ----------------------------
#
# A command (and a `lamin_schemas.yml` entry) names plans as
# `path/to/plan.py:plan_variable` targets rather than as files, and it may name
# several at once.  Resolving those targets -- parsing them, picking the plan
# variable, rejecting a plan listed twice under two spellings, and ordering the
# result so a prerequisite comes before the plan that borrows from it -- is the
# layer above the import machinery above, and the `ci` entry points need it as
# much as the CLI does.  Failures are raised as :class:`PlanTargetError`, which
# the CLI reports against the command that was given the target.


def parse_target(target: str) -> tuple[Path, str | None]:
    """Split a `path/to/plan.py:plan_variable` target, checking the file exists."""
    path_str, sep, var = target.partition(":")
    if sep and not var:
        raise PlanTargetError(
            "target must be of the form 'path/to/plan.py:plan_variable'"
        )
    path = Path(path_str)
    if not path.exists():
        raise PlanTargetError(f"plan file not found: {path}")
    return path, (var or None)


def _defined_plan_vars(module: ModuleType) -> list[str]:
    """Return the variables holding a :class:`Plan` *defined* in `module`.

    Plans imported from other modules are left out, so that sharing a plan
    across files does not make a target's `:plan_variable` suffix ambiguous.
    """
    from ..authoring import Plan

    return [
        name
        for name, obj in vars(module).items()
        if isinstance(obj, Plan) and obj.defining_module == module.__name__
    ]


def _plan_vars_hint(module: ModuleType) -> str:
    """Name the plan variables `module` defines, for a target that missed."""
    plan_vars = _defined_plan_vars(module)
    if not plan_vars:
        return "it defines no plan"
    return f"available: {', '.join(sorted(plan_vars))}"


def resolve_plan_var(module: ModuleType, path: Path, var: str | None) -> str:
    """Return the plan variable to use, inferring it when `var` is omitted.

    When the target has no `:plan_variable` suffix, the module must define
    exactly one :class:`Plan`; that variable is used.  Only plans *defined* in
    the file count -- plans imported from other modules are ignored -- so that
    sharing a plan across files does not make the suffix ambiguous.  Zero or
    several local plans require an explicit `:plan_variable`.
    """
    if var is not None:
        return var
    plan_vars = _defined_plan_vars(module)
    if len(plan_vars) == 1:
        return plan_vars[0]
    if not plan_vars:
        raise PlanTargetError(f"no Plan defined in {path}")
    raise PlanTargetError(
        f"{path} defines several plans ({', '.join(sorted(plan_vars))}); "
        "specify one as 'path/to/plan.py:plan_variable'"
    )


def _import_targeted_module(path: Path) -> ModuleType:
    """Import the plan file a target named, reporting failure against the target.

    Same import as :func:`import_plan_module`; only the failure differs, being
    re-raised as a :class:`PlanTargetError` so the command blames the target that
    named the file rather than showing an internal error.
    """
    try:
        return import_plan_module(path)
    except PlanImportError as exc:
        raise PlanTargetError(str(exc)) from exc


def plan_target(plan: Plan) -> str | None:
    """Return the `path/to/plan.py[:plan_variable]` target that names `plan`.

    The inverse of :func:`load_plan`, for a message telling the user what to
    run: the file the plan's defining module was loaded from -- relative to the
    working directory when it lies beneath it -- with a `:plan_variable`
    suffix only when the file defines more than one plan, as
    :func:`resolve_plan_var` requires.  `None` when the plan was not loaded
    from a file.
    """
    module = sys.modules.get(plan.defining_module or "")
    file = getattr(module, "__file__", None)
    if module is None or not file:
        return None
    path = Path(file)
    with contextlib.suppress(OSError, ValueError):
        path = path.resolve().relative_to(Path.cwd().resolve())
    plan_vars = _defined_plan_vars(module)
    if len(plan_vars) <= 1:
        return str(path)
    var = next((name for name in plan_vars if vars(module)[name] is plan), None)
    return str(path) if var is None else f"{path}:{var}"


def load_plan(target: str) -> tuple[Plan, Path, ModuleType]:
    """Import the plan file and return the named :class:`Plan`, its path and module.

    The `:plan_variable` suffix is optional when the file defines exactly one
    :class:`Plan`.
    """
    from ..authoring import Plan

    path, var = parse_target(target)
    module = _import_targeted_module(path)

    var = resolve_plan_var(module, path, var)
    obj = getattr(module, var, None)
    if obj is None:
        raise PlanTargetError(
            f"plan variable {var!r} not found in {path}; {_plan_vars_hint(module)}"
        )
    if not isinstance(obj, Plan):
        raise PlanTargetError(
            f"{var!r} in {path} is not a Plan (got {type(obj).__name__}); "
            f"{_plan_vars_hint(module)}"
        )
    return obj, path, module


def load_plans_in_file(path: Path) -> list[tuple[Plan, Path, ModuleType]]:
    """Import `path` and return every :class:`Plan` *defined* in it.

    Plans imported from other modules are ignored (only those whose
    `defining_module` is the file itself count), matching the inference rules
    of :func:`resolve_plan_var`.  The plans are returned in the order they are
    defined in the file.  Raises if the file defines no plan.
    """
    from ..authoring import Plan

    module = _import_targeted_module(path)
    plans = [
        (obj, path, module)
        for obj in vars(module).values()
        if isinstance(obj, Plan) and obj.defining_module == module.__name__
    ]
    if not plans:
        raise PlanTargetError(f"no Plan defined in {path}")
    return plans


def load_all_plans(targets: list[str]) -> list[tuple[Plan, Path, ModuleType]]:
    """Load every plan target before finalizing any of them.

    Finalizing a plan imports lamindb, after which the plan-construction guard
    refuses to build any further plan; importing (constructing) all plan files
    up front keeps each construction lamindb-free.  Duplicate targets are
    rejected so a plan cannot be its own sibling.
    """
    seen: dict[tuple[Path, str | None], str] = {}
    for target in targets:
        path, var = parse_target(target)
        identity = (path.resolve(), var)
        if first := seen.get(identity):
            raise PlanTargetError(
                f"plan {target!r} is listed more than once (first as {first!r})"
            )
        seen[identity] = target

    loaded: list[tuple[Plan, Path, ModuleType]] = []
    resolved_seen: dict[tuple[Path, str], str] = {}
    for target in targets:
        item = load_plan(target)
        plan, path, module = item
        _, requested_var = parse_target(target)
        resolved_var = resolve_plan_var(module, path, requested_var)
        identity = (path.resolve(), resolved_var)
        if first := resolved_seen.get(identity):
            raise PlanTargetError(
                f"plan {target!r} is listed more than once (first as {first!r})"
            )
        resolved_seen[identity] = target
        loaded.append((plan, path, module))
    return loaded


def order_dependencies_first(
    loaded: list[tuple[Plan, Path, ModuleType]],
) -> list[tuple[Plan, Path, ModuleType]]:
    """Order loaded plans so each comes after the listed plans it depends on.

    A plan that `depends_on` another *borrows* its objects rather than
    creating them, so the owner has to be applied first -- otherwise the
    borrowed objects do not exist yet and the apply is refused (see
    :func:`~lamindb_setup.evolve.engine._apply_run.apply_one`).  `destroy`
    needs the same order for the opposite reason: it tears the plans down as one
    merged, *reversed* deletion, so dependencies-first means dependents are
    removed before the types they rely on.

    A depth-first walk over `depends_on` (restricted to the listed plans)
    yields that order; plans that depend on nothing listed keep their given
    order.  `depends_on` is populated when the plan object is constructed,
    from the plan function's signature, so this works before any plan has been
    finalized.
    """
    listed: dict[int, tuple[Plan, Path, ModuleType]] = {
        id(item[0]): item for item in loaded
    }
    ordered: list[tuple[Plan, Path, ModuleType]] = []
    visited: set[int] = set()

    def visit(plan_obj: Plan) -> None:
        if id(plan_obj) in visited:
            return
        visited.add(id(plan_obj))
        for dependency in plan_obj.depends_on:
            if id(dependency) in listed:
                visit(dependency)
        item = listed.get(id(plan_obj))
        if item is not None:
            ordered.append(item)

    for plan_obj, _file, _module in loaded:
        visit(plan_obj)
    return ordered


def sibling_claimed_keys(plans: list[Plan]) -> list[set[str]]:
    """Return, for each plan, the natural keys declared by the *other* plans.

    Listing several plans evaluates them together: an object one plan drops but
    a sibling still declares is a move, not a delete.  With a single plan every
    set is empty, preserving standalone single-plan behavior.
    """
    from ._apply import plan_natural_keys

    keys = [plan_natural_keys(plan) for plan in plans]
    result: list[set[str]] = []
    for i in range(len(plans)):
        claimed: set[str] = set()
        for j, other in enumerate(keys):
            if j != i:
                claimed |= other
        result.append(claimed)
    return result


def co_applied_prerequisite_keys(plans: Sequence[Plan]) -> list[set[str]]:
    """Return, for each plan, the natural keys its prerequisites among `plans` declare.

    `plans` are the plans of one run: listed together, or configured for the
    same instance.  A plan borrows the objects of the plans it `depends_on`
    (transitively); a prerequisite in the run is applied before it and creates
    whatever of its own objects the instance lacks, so a preview reports such a
    borrowed object as created by its owner in this run rather than as missing
    (see :func:`~lamindb_setup.evolve.engine._apply.compute_changes`).  Only the
    objects a listed prerequisite declares itself count: one it borrows in
    turn from a plan outside the run is still missing.

    A prerequisite is matched by identity, as :func:`order_dependencies_first`
    matches it.  The listed prerequisites are finalized.
    """
    from ._lock import natural_key

    listed = {id(plan) for plan in plans}
    own_keys: dict[int, set[str]] = {}

    def keys_of(plan: Plan) -> set[str]:
        if id(plan) not in own_keys:
            plan.finalize()
            own_keys[id(plan)] = {
                natural_key(dependency.obj) for dependency in plan.own_dependencies
            }
        return own_keys[id(plan)]

    result: list[set[str]] = []
    for plan in plans:
        keys: set[str] = set()
        visited: set[int] = set()
        pending = list(plan.depends_on)
        while pending:
            prerequisite = pending.pop()
            if id(prerequisite) in visited or prerequisite is plan:
                continue
            visited.add(id(prerequisite))
            if id(prerequisite) in listed:
                keys |= keys_of(prerequisite)
            pending.extend(prerequisite.depends_on)
        result.append(keys)
    return result


def require_stem_uid(plan_module: ModuleType, plan_file: Path) -> str:
    """Return the plan's `PLAN_TRANSFORM_STEM_UID`, erroring if it is missing.

    `validate`, `plan` and `apply` all require the plan to declare a stable
    transform stem uid so that applications are tracked as versions of a single
    transform.  Raises :class:`StemUidError` (pointing at `utils add-uid`) when
    the variable is absent or malformed.
    """
    from ._track import PLAN_STEM_UID_VAR, is_valid_stem_uid

    stem_uid = getattr(plan_module, PLAN_STEM_UID_VAR, None)
    if stem_uid is None:
        raise StemUidError(
            f"plan {plan_file} does not define {PLAN_STEM_UID_VAR}; "
            f"add one with: lamin evolve utils add-uid {plan_file}"
        )
    if not is_valid_stem_uid(stem_uid):
        raise StemUidError(
            f"{PLAN_STEM_UID_VAR} in {plan_file} is not a valid "
            f"12-character base62 stem uid: {stem_uid!r}"
        )
    return stem_uid
