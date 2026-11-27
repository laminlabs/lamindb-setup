# ruff: noqa: B010
"""Git-gated run tracking for plan application.

Applying a plan records a LaminDB run, tracked as a transform via `ln.track`.
To keep the recorded transform reproducible, a plan may only be applied when its
file is committed to git and the current commit has been pushed to the branch's
remote tracking branch.  The transform key combines a globally configurable
prefix with the plan file's path within the repository.

Each plan gets its *own* transform, identified by a stem uid.  The preferred
source of that uid is a `PLAN_TRANSFORM_STEM_UID` module-level variable in the
plan file (scaffolded by `lamin evolve init`); when a plan does not declare
one, a uid is derived deterministically from the plan's track key as a fallback
(see :func:`plan_transform_uid`).  Either way, applying the same plan again --
even after edits -- registers as a new *version* of that one transform rather
than as an unrelated transform, while different plans map to different
transforms.  When the repository's remote is known, LaminDB's git sync is
enabled so the transform records the exact git commit/blob URL of the applied
plan file.

By default runs are recorded in LaminDB's `"all"` space.  Set the
:data:`TRACK_SPACE_ENV_VAR` environment variable to a space name or uid to
route every `lamin evolve` run -- and the transforms and outputs it creates
-- into a dedicated *maintenance* space instead (see :func:`track_space`).
"""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import subprocess
import sys
import tempfile
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, ExitStack, contextmanager, nullcontext
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..core import LaminSchemasError

if TYPE_CHECKING:
    from lamindb.models import Run

__all__ = [
    "DEFAULT_TRACK_PREFIX",
    "GitRepo",
    "GitWorkspace",
    "PLAN_STEM_UID_VAR",
    "STEM_UID_LENGTH",
    "TRACK_PREFIX_ENV_VAR",
    "TRACK_SPACE_ENV_VAR",
    "TrackConfig",
    "TrackError",
    "attach_operation_report",
    "ensure_all_committed_and_pushed",
    "ensure_committed_and_pushed",
    "ensure_lock_files_committed",
    "generate_stem_uid",
    "generate_stem_uids",
    "is_valid_stem_uid",
    "plan_track_key",
    "plan_transform_uid",
    "repo_relative_path",
    "taken_stem_uids",
    "track_operation",
    "track_plan",
    "track_prefix",
    "verified_session",
]

DEFAULT_TRACK_PREFIX = "schema_evolution"
TRACK_PREFIX_ENV_VAR = "LAMIN_SCHEMA_EVOLUTION_TRACK_PREFIX"

# Environment variable naming the LaminDB space that `lamin evolve` runs (and
# the transforms/outputs they create) are recorded in.  Unset means LaminDB's
# default `"all"` space.
TRACK_SPACE_ENV_VAR = "LAMIN_SCHEMA_EVOLUTION_TRACK_SPACE"

# Name of the module-level variable a plan file uses to declare its transform
# stem uid (populated by `lamin evolve init`).
PLAN_STEM_UID_VAR = "PLAN_TRANSFORM_STEM_UID"

# Length of a LaminDB transform *stem* uid (the full uid is the stem plus a
# 4-character version suffix).
STEM_UID_LENGTH = 12

# base62 alphabet used by LaminDB for transform stem uids.
_BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


class TrackError(LaminSchemasError):
    """Raised when a plan cannot be tracked (git state or configuration issue)."""


def track_prefix() -> str:
    """Return the transform key prefix from the environment.

    Thin wrapper over :meth:`TrackConfig.resolve` for the call sites that have
    no resolved configuration to hand.  Prefer passing a :class:`TrackConfig`
    down from the command boundary.
    """
    return TrackConfig.resolve().prefix


@dataclass(frozen=True)
class TrackConfig:
    """How one run records its transforms: under which key prefix, in which space.

    Resolved **once, at the command boundary** (see :meth:`resolve`) and passed
    down, rather than re-read from the environment at each of the places that
    needs it.  That keeps a single run internally consistent -- every transform
    it opens agrees on the prefix and space even if the environment changes
    underneath it -- and gives the settings one documented home.

    `prefix` joins with a plan's repo-relative path to form its transform key.
    `space` is a LaminDB space *name* or *uid* passed to `ln.track(space=...)`
    so the run and the transforms/outputs it creates land there; `None` means
    LaminDB's default `"all"` space.
    """

    prefix: str = DEFAULT_TRACK_PREFIX
    space: str | None = None

    @classmethod
    def resolve(
        cls,
        *,
        prefix: str | None = None,
        space: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> TrackConfig:
        """Fold explicit settings over the environment; explicit wins.

        `prefix` / `space` come from a `lamin_schemas.yml` (see
        :meth:`~lamindb_setup.evolve.ci._config.CIConfig.track_prefix_for` and
        :meth:`~lamindb_setup.evolve.ci._config.CIConfig.track_space_for`); a
        blank one counts as unset so an empty YAML value cannot silently erase
        the environment's answer.  `env` defaults to :data:`os.environ` and is
        a parameter so the resolution can be exercised as a pure function.
        """
        env = os.environ if env is None else env

        resolved_prefix = prefix if prefix is not None and prefix.strip() else None
        if resolved_prefix is None:
            resolved_prefix = env.get(TRACK_PREFIX_ENV_VAR, DEFAULT_TRACK_PREFIX)

        resolved_space = space if space is not None and space.strip() else None
        if resolved_space is None:
            resolved_space = env.get(TRACK_SPACE_ENV_VAR)

        return cls(
            prefix=resolved_prefix.strip().strip("/"),
            space=resolved_space.strip() or None if resolved_space else None,
        )

    def key_for(self, rel: Path | str) -> str:
        """Return the transform key for a repo-relative plan path or operation name."""
        text = rel.as_posix() if isinstance(rel, Path) else rel
        return f"{self.prefix}/{text}"


#: How a git command is invoked: `(args, cwd) -> CompletedProcess`.  A
#: :class:`GitWorkspace` carries one, so a caller can observe or replace the way
#: git is run without reaching into this module.
GitRunner = Callable[[list[str], Path], subprocess.CompletedProcess[str]]


def _run_git(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run `git` in `cwd`. The only place this package spawns a subprocess."""
    try:
        return subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:  # pragma: no cover - git missing is rare
        raise TrackError(
            "git executable not found; install git to apply plans."
        ) from exc


@dataclass(frozen=True)
class GitRepo:
    """One git repository, and the questions this package asks of it.

    Every check below is a property of the *repository*, which is why they are
    methods here rather than free functions taking a root: grouping the plans of
    one repository and asking it once is what keeps a run applying many plans to
    a handful of subprocesses rather than five per plan.

    `run` is the subprocess runner, defaulting to :func:`_run_git`.  It is a
    field rather than a module lookup so a caller can observe or replace how git
    is invoked -- see :class:`GitWorkspace`, which supplies it.
    """

    root: Path
    run: GitRunner = _run_git

    def git(self, *args: str) -> subprocess.CompletedProcess[str]:
        """Run `git <args>` in this repository."""
        return self.run(list(args), self.root)

    def require(self, *args: str, error: str) -> str:
        """Run `git <args>`, raising :class:`TrackError` with `error` on failure."""
        result = self.git(*args)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise TrackError(f"{error}{': ' + detail if detail else ''}")
        return result.stdout.strip()

    def ensure_tracked(self, rel_posix: list[str]) -> None:
        """Raise unless every path in `rel_posix` is tracked by git."""
        # `-z` reports the paths verbatim (git otherwise quotes unusual names),
        # so the untracked ones are exactly those missing from the output.
        listed = self.require(
            "ls-files", "-z", "--", *rel_posix, error="failed to list tracked files"
        )
        tracked = set(listed.split("\0"))
        for rel in rel_posix:
            if rel not in tracked:
                raise TrackError(
                    f"plan file {rel!r} is not tracked by git; "
                    "commit it before applying."
                )

    def ensure_unmodified(self, rel_posix: list[str]) -> None:
        """Raise unless every path in `rel_posix` is free of uncommitted changes."""
        status = self.require(
            "status",
            "--porcelain",
            "-z",
            "--",
            *rel_posix,
            error="failed to read git status",
        )
        if not status:
            return
        # Something is dirty, but `--porcelain` output cannot be attributed
        # back to a path without parsing rename records.  This is the failing
        # path and it ends the run, so simply ask again per file to name the
        # culprit exactly.
        for rel in rel_posix:
            if self.require(
                "status", "--porcelain", "--", rel, error="failed to read git status"
            ):
                raise TrackError(
                    f"plan file {rel!r} has uncommitted changes; "
                    "commit them before applying."
                )

    def ensure_head_pushed(self) -> None:
        """Raise unless `HEAD` is contained in a remote tracking branch."""
        # On a normal local branch we compare HEAD against its configured
        # upstream.  In CI the plan is checked out at an exact commit (a detached
        # HEAD), so there is no upstream to compare against; fall back to
        # verifying the commit is contained in some remote-tracking branch, which
        # is the property we actually require (the commit has been pushed).
        upstream = self.git(
            "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"
        )
        if upstream.returncode == 0:
            pushed = self.git("merge-base", "--is-ancestor", "HEAD", "@{upstream}")
            if pushed.returncode != 0:
                raise TrackError(
                    "the current commit has not been pushed to its remote "
                    "tracking branch; push it before applying."
                )
        elif self.git("symbolic-ref", "-q", "HEAD").returncode == 0:
            # On a branch but with no upstream configured (e.g. a freshly created
            # local branch that was never pushed).
            raise TrackError(
                "the current branch has no upstream tracking branch; "
                "push it before applying."
            )
        else:
            # Detached HEAD (e.g. CI checked out an exact commit): require the
            # commit to be contained in a remote-tracking branch.
            contained = self.git("branch", "-r", "--contains", "HEAD")
            if contained.returncode != 0 or not contained.stdout.strip():
                raise TrackError(
                    "the current commit has not been pushed to any remote "
                    "tracking branch; push it before applying."
                )

    def remote_url(self) -> str | None:
        """Return this repository's `origin` URL, or `None` if it has none."""
        result = self.git("config", "--get", "remote.origin.url")
        if result.returncode != 0:
            return None
        return result.stdout.strip() or None

    @contextmanager
    def https_origin_with_ssh_insteadof(self) -> Iterator[str | None]:
        """Temporarily present an https `origin` URL while keeping SSH for transport.

        LaminDB's git sync requires the `origin` remote to be an `https://`
        URL, but a repository may legitimately use an SSH remote.  For the
        duration of the `with` block this rewrites `remote.origin.url` to its
        https form and adds a `url.<ssh-base>.insteadOf <https-base>` rule so
        that any git network operation still goes over SSH.  Both changes are
        reverted on exit.

        Yields the https origin URL (or `None` when no rewrite is
        possible/needed, e.g. a local-path or already-https remote).
        """
        origin = self.remote_url()
        https = _https_remote_url(origin)
        # Nothing to rewrite: no remote, unmappable remote, or already https.
        if origin is None or https is None or origin.startswith("https://"):
            yield https
            return

        bases = _git_url_bases(origin)
        if bases is None:
            yield https
            return
        ssh_base, https_base = bases
        insteadof_key = f"url.{ssh_base}.insteadOf"
        had_insteadof = self.git("config", "--get", insteadof_key).returncode == 0

        self.git("config", "remote.origin.url", https)
        if not had_insteadof:
            self.git("config", insteadof_key, https_base)
        try:
            yield https
        finally:
            self.git("config", "remote.origin.url", origin)
            if not had_insteadof:
                self.git("config", "--unset", insteadof_key)


def plan_transform_uid(key: str) -> str:
    """Derive a deterministic 12-character base62 transform stem uid from `key`.

    Used as a fallback for plans that do not declare a `PLAN_TRANSFORM_STEM_UID`.
    The uid is a pure function of the plan's track key, so a given plan always
    maps to the same transform stem: re-applying it creates a new *version*,
    while different plans (different keys) map to different transforms.
    """
    digest = int.from_bytes(
        hashlib.md5(key.encode(), usedforsecurity=False).digest(), "big"
    )
    chars = []
    for _ in range(STEM_UID_LENGTH):
        digest, rem = divmod(digest, 62)
        chars.append(_BASE62_ALPHABET[rem])
    return "".join(chars)


def is_valid_stem_uid(uid: object) -> bool:
    """Return whether `uid` is a valid 12-character base62 transform stem uid."""
    return (
        isinstance(uid, str)
        and len(uid) == STEM_UID_LENGTH
        and all(char in _BASE62_ALPHABET for char in uid)
    )


def generate_stem_uid(is_taken: Callable[[str], bool] | None = None) -> str:
    """Generate a fresh random 12-character base62 transform stem uid.

    If `is_taken` is given, it is called with each candidate and generation is
    retried until it returns `False`, so the result does not collide with uids
    already in use.
    """
    while True:
        uid = "".join(secrets.choice(_BASE62_ALPHABET) for _ in range(STEM_UID_LENGTH))
        if is_taken is None or not is_taken(uid):
            return uid


def taken_stem_uids(ln: Any | None = None) -> set[str]:
    """Return the stem uids the *connected* instance's transforms already hold.

    A transform's uid is its stem plus a version suffix, so it is the stems that
    a new plan has to stay clear of: reusing one would file the plan's runs as
    further *versions* of somebody else's transform.  `ln` defaults to the real
    LaminDB module (see :func:`_lamindb`).
    """
    ln = _lamindb() if ln is None else ln
    registry: Any = ln.Transform
    return {
        uid[:STEM_UID_LENGTH] for uid in registry.filter().values_list("uid", flat=True)
    }


def generate_stem_uids(
    count: int = 1,
    *,
    instances: Sequence[str] = (),
    connect: Callable[[str], str] | None = None,
    ln: Any | None = None,
) -> list[str]:
    """Generate `count` distinct stem uids that are free on every named instance.

    A plan is usually applied to several instances (dev, staging, prod), each
    assigning its own uids, so a uid is only safe once it is unclaimed on *all*
    of them: each instance in `instances` is connected to in turn and the stems
    its transforms hold are collected before anything is generated.  Passing no
    instance queries nothing and yields plain random uids -- what an offline
    `lamin evolve utils generate-stem-uid` does.  The uids are also kept
    distinct from one another, so a batch can be handed out to several plans.

    `connect` defaults to :func:`~lamindb_setup.evolve.engine._connect.safe_connect`;
    it is a parameter so a caller can drive the lookup against a stand-in.  An
    instance named twice is only connected to once, and the run is left
    connected to the last one, as connecting anywhere always does.
    """
    if connect is None:
        from ._connect import safe_connect

        connect = safe_connect

    taken: set[str] = set()
    for instance in dict.fromkeys(instances):
        connect(instance)
        taken |= taken_stem_uids(ln)

    uids: list[str] = []
    for _ in range(count):
        uid = generate_stem_uid(taken.__contains__)
        taken.add(uid)
        uids.append(uid)
    return uids


def _https_remote_url(url: str | None) -> str | None:
    """Convert a git remote URL to its `https://` form, or `None` if it can't be.

    LaminDB's git sync only accepts `https://` URLs (it asserts on the scheme),
    so SSH-style remotes must be rewritten before they are handed over.  Handles
    the common GitHub/GitLab forms:

    - `git@host:org/repo(.git)`        -> `https://host/org/repo`
    - `ssh://git@host[:port]/org/repo` -> `https://host/org/repo`
    - `https://host/org/repo(.git)`    -> `https://host/org/repo`

    Returns `None` for anything that is not an https/SSH remote (e.g. a local
    filesystem path used as the origin in tests).
    """
    if not url:
        return None

    # scp-like syntax: git@github.com:org/repo.git
    match = re.match(r"^[\w.+-]+@([\w.-]+):(.+)$", url)
    if match:
        host, path = match.groups()
    elif url.startswith("ssh://"):
        # ssh://git@github.com:22/org/repo.git or ssh://github.com/org/repo
        remainder = url[len("ssh://") :]
        if "@" in remainder:
            remainder = remainder.split("@", 1)[1]
        host, sep, path = remainder.partition("/")
        if not sep:
            return None
        host = host.split(":", 1)[0]  # drop optional port
    elif url.startswith("https://"):
        host, path = None, None
    else:
        return None

    if host is None:
        normalized = url
    else:
        normalized = f"https://{host}/{path}"
    return normalized[: -len(".git")] if normalized.endswith(".git") else normalized


def _git_url_bases(ssh_url: str) -> tuple[str, str] | None:
    """Return `(ssh_base, https_base)` prefixes for an SSH remote, or `None`.

    These are the prefixes used by git's `url.<base>.insteadOf` rewriting, e.g.
    `("git@github.com:", "https://github.com/")` for `git@github.com:org/repo`.
    """
    # scp-like syntax: git@github.com:org/repo(.git)
    match = re.match(r"^([\w.+-]+@[\w.-]+:)", ssh_url)
    if match:
        ssh_base = match.group(1)
        host = ssh_base.split("@", 1)[1].rstrip(":")
        return ssh_base, f"https://{host}/"
    if ssh_url.startswith("ssh://"):
        remainder = ssh_url[len("ssh://") :]
        userhost, sep, _ = remainder.partition("/")
        if not sep:
            return None
        host = userhost.split("@", 1)[1] if "@" in userhost else userhost
        host = host.split(":", 1)[0]
        return f"ssh://{userhost}/", f"https://{host}/"
    return None


class GitWorkspace:
    """The repositories one run touches, and what it has already established.

    Owns the three pieces of state the git gate needs to keep across a run:

    * which repository each directory belongs to -- memoized, because that
      cannot change while the process runs and several code paths ask it once
      per plan;
    * which plans a :meth:`verified_session` has already gated, so tracking them
      does not re-run the whole gate per plan; and
    * the https `origin` rewrite held open per repository for that session.

    Holding them here rather than in module globals means the memo is keyed by
    directory alone (the runner is a field, so injecting one cannot defeat it),
    the state is scoped to whoever owns the workspace rather than to the
    process, and a caller that wants to watch how git is invoked supplies a
    `run` instead of patching a module attribute.
    """

    def __init__(self, run: GitRunner = _run_git) -> None:
        self._run = run
        self._roots: dict[Path, Path] = {}
        self._verified: dict[Path, Path] = {}
        self._session_remotes: dict[Path, str | None] = {}

    def repo_at(self, root: Path) -> GitRepo:
        """Return the :class:`GitRepo` rooted at `root`."""
        return GitRepo(root, self._run)

    def repo_for(self, path: Path) -> GitRepo:
        """Return the repository containing `path` (a file or directory)."""
        return self.repo_at(self.root_for(path))

    def root_for(self, path: Path) -> Path:
        """Return the git root containing `path`, resolving each directory once.

        Only successful lookups are remembered -- a :class:`TrackError` propagates
        rather than being cached -- so a directory that becomes a repository
        later is picked up.
        """
        directory = path.parent if path.is_file() else path
        root = self._roots.get(directory)
        if root is None:
            found = self.repo_at(directory).require(
                "rev-parse",
                "--show-toplevel",
                error=f"{directory} is not inside a git repository",
            )
            root = self._roots[directory] = Path(found).resolve()
        return root

    def ensure_all_committed_and_pushed(
        self, plan_files: Iterable[str | Path]
    ) -> dict[Path, Path]:
        """Gate every plan in `plan_files`; see the module-level wrapper."""
        resolved = [Path(plan_file).resolve() for plan_file in plan_files]
        if not resolved:
            return {}

        # Group by repository, resolving the root once per distinct directory:
        # plans usually share a directory, and always share a repository.
        roots: dict[Path, Path] = {}
        by_root: dict[Path, list[Path]] = {}
        for plan_path in resolved:
            root = roots.get(plan_path.parent)
            if root is None:
                root = roots[plan_path.parent] = self.root_for(plan_path)
            by_root.setdefault(root, []).append(plan_path)

        for root, paths in by_root.items():
            repo = self.repo_at(root)
            rel_posix = [path.relative_to(root).as_posix() for path in paths]
            repo.ensure_tracked(rel_posix)
            repo.ensure_unmodified(rel_posix)
            repo.ensure_head_pushed()

        return {path: path.relative_to(roots[path.parent]) for path in resolved}

    def verified_rel(self, plan_path: Path) -> Path | None:
        """The repo-relative path an enclosing session already gated, if any."""
        return self._verified.get(plan_path)

    def session_remote(self, root: Path) -> AbstractContextManager[str | None]:
        """The https origin for `root`: held open by a session, else entered now."""
        if root in self._session_remotes:
            return nullcontext(self._session_remotes[root])
        return self.repo_at(root).https_origin_with_ssh_insteadof()

    @contextmanager
    def verified_session(self, verified: Mapping[Path, Path]) -> Iterator[None]:
        """Hold the git gate's findings open; see the module-level wrapper."""
        if not verified:
            yield
            return

        roots = dict.fromkeys(self.root_for(plan_path) for plan_path in verified)
        previous_rels = dict(self._verified)
        previous_remotes = dict(self._session_remotes)
        with ExitStack() as stack:
            for root in roots:
                self._session_remotes[root] = stack.enter_context(
                    self.repo_at(root).https_origin_with_ssh_insteadof()
                )
            self._verified.update(verified)
            try:
                yield
            finally:
                self._verified.clear()
                self._verified.update(previous_rels)
                self._session_remotes.clear()
                self._session_remotes.update(previous_remotes)


#: The workspace the module-level entry points use when a caller names none.
#: Process-lived on purpose: its repository memo is what keeps a run from
#: re-asking git which repository each plan belongs to.
_DEFAULT_WORKSPACE = GitWorkspace()


def repo_relative_path(
    plan_file: str | Path, *, workspace: GitWorkspace | None = None
) -> Path:
    """Return `plan_file` relative to its git repository root."""
    workspace = _DEFAULT_WORKSPACE if workspace is None else workspace
    plan_path = Path(plan_file).resolve()
    return plan_path.relative_to(workspace.root_for(plan_path))


def ensure_committed_and_pushed(
    plan_file: str | Path, *, workspace: GitWorkspace | None = None
) -> Path:
    """Verify `plan_file` is committed and its commit is pushed; return its repo path.

    The check fails (raising :class:`TrackError`) unless the plan file is tracked
    by git, has no uncommitted changes, and the current `HEAD` commit is
    contained in the branch's remote tracking branch.
    """
    plan_path = Path(plan_file).resolve()
    return ensure_all_committed_and_pushed([plan_path], workspace=workspace)[plan_path]


def ensure_all_committed_and_pushed(
    plan_files: Iterable[str | Path], *, workspace: GitWorkspace | None = None
) -> dict[Path, Path]:
    """Run :func:`ensure_committed_and_pushed` over `plan_files` in bulk.

    Returns each plan file's resolved path mapped to its path within the
    repository, and raises :class:`TrackError` for the first plan that fails --
    so a run applying many plans still refuses to start on any dirty one.

    Checking the plans together matters because git is a subprocess: done one at
    a time this is five `git` invocations per plan, and only two of those
    depend on the plan at all.  Whether `HEAD` is pushed is a property of the
    repository, so it is established once per repository, and both file-level
    questions accept many paths at once.  A run over any number of plans in one
    repository therefore costs a handful of subprocesses rather than `5 * N`.

    `workspace` carries the repository memo and, when a caller supplies one,
    the runner git is invoked through (see :class:`GitWorkspace`).
    """
    workspace = _DEFAULT_WORKSPACE if workspace is None else workspace
    return workspace.ensure_all_committed_and_pushed(plan_files)


def ensure_lock_files_committed(
    lock_files: Iterable[str | Path], *, workspace: GitWorkspace | None = None
) -> None:
    """Raise :class:`TrackError` unless every existing lock file is committed as is.

    `repair align-uids` rewrites lock files and reads the uids it aligns to
    from them.  Starting from committed state is what makes the result a clean,
    reviewable diff -- and what stops a locally edited or half-merged lock from
    deciding which uids win.  A lock file that does not exist yet has nothing to
    lose and is skipped.
    """
    workspace = _DEFAULT_WORKSPACE if workspace is None else workspace
    by_root: dict[Path, list[Path]] = {}
    for lock_file in lock_files:
        path = Path(lock_file).resolve()
        if path.exists():
            by_root.setdefault(workspace.root_for(path), []).append(path)
    for root, paths in by_root.items():
        repo = workspace.repo_at(root)
        for path in paths:
            rel = path.relative_to(root).as_posix()
            status = repo.require(
                "status", "--porcelain", "--", rel, error="failed to read git status"
            )
            if status:
                raise TrackError(
                    f"lock file {rel!r} has uncommitted changes; commit or discard "
                    "them first, so the uids are aligned from committed state and "
                    "the rewrite shows up as its own diff."
                )


def plan_track_key(
    plan_file: str | Path,
    *,
    config: TrackConfig | None = None,
    workspace: GitWorkspace | None = None,
) -> str:
    """Return the `ln.track` key for `plan_file` after verifying its git state."""
    rel = ensure_committed_and_pushed(plan_file, workspace=workspace)
    config = TrackConfig.resolve() if config is None else config
    return config.key_for(rel)


@contextmanager
def verified_session(
    verified: Mapping[Path, Path], *, workspace: GitWorkspace | None = None
) -> Iterator[None]:
    """Hold the git gate's findings open for a run that tracks many plans.

    `verified` is what :func:`ensure_all_committed_and_pushed` returned for
    every plan the run is about to apply: each plan's resolved path mapped to
    its path within the repository.  For the duration of the block
    :func:`track_plan` reuses those answers instead of asking git again --
    without a session it re-runs the whole gate per plan, four subprocesses
    apiece, to establish what the batched check just established -- and the
    https-origin rewrite LaminDB's git sync needs is entered once per repository
    rather than once per plan (one to six further subprocesses each).

    The rewrite is therefore live for the whole run rather than for each
    `ln.track` call.  It is reverted on exit, including when the block raises;
    only a hard kill leaves it in place, exactly as the per-plan form does.

    An empty `verified` -- a caller that did not run the gate -- is a no-op,
    and a plan outside `verified` still gates itself when it is tracked, so
    nothing is ever tracked on unverified git state.
    """
    workspace = _DEFAULT_WORKSPACE if workspace is None else workspace
    with workspace.verified_session(verified):
        yield


@lru_cache(maxsize=1)
def _pip_freeze(*, run: Callable[..., Any] = subprocess.run) -> str | None:
    """Return this interpreter's `pip freeze` output, or `None` if it failed.

    Computed once per process: the answer describes the interpreter, which no
    `lamin evolve` run changes while it is running.  `run` is a parameter so
    a test can prove the memoization without spawning `pip`; it takes part in
    the cache key, so a test's runner never shares an entry with the real one.
    """
    try:
        result = run(
            [sys.executable, "-m", "pip", "freeze"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:  # pragma: no cover - pip missing is rare
        return None
    return result.stdout if result.returncode == 0 else None


@contextmanager
def _cached_python_environment(
    ln: Any,
    *,
    pip_freeze: Callable[[], str | None] = _pip_freeze,
    cache_dir: Path | None = None,
) -> Iterator[None]:
    """Compute the run's `pip freeze` once per process instead of per run.

    `ln.track` records the Python environment by shelling out to `pip
    freeze` for every run it opens, and `ln.finish` uploads the resulting
    `run_env_pip.txt` as the run's environment artifact.  A run applying many
    plans opens one run per plan per instance, so it pays for that subprocess --
    around a third of a second -- once per plan, for an answer that is identical
    every time.

    For the duration of the block LaminDB's hook is replaced by one that writes
    the same file from the memoized output of :func:`_pip_freeze`, so the
    artifact is still produced, byte for byte, from a single subprocess.  A
    LaminDB whose hook cannot be found, or an interpreter where `pip freeze`
    fails, falls back to the original behaviour.

    The hook is reached through the caller's `ln` module rather than a fresh
    import, so a caller that injected a stand-in LaminDB does not pull the real
    one in behind its back.
    """
    lamindb_context = getattr(getattr(ln, "core", None), "_context", None)
    if lamindb_context is None:  # pragma: no cover - guards a LaminDB rename
        yield
        return
    original = getattr(lamindb_context, "track_python_environment", None)
    if original is None:  # pragma: no cover - guards a LaminDB rename
        yield
        return

    def cached(run: Run) -> None:
        text = pip_freeze()
        if text is None:  # pragma: no cover - pip freeze failing is rare
            original(run)
            return
        root = cache_dir
        if root is None:
            import lamindb_setup as ln_setup

            root = ln_setup.settings.cache_dir  # type: ignore[assignment]

        path = root / "environments" / f"run_{run.uid}" / "run_env_pip.txt"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    # `setattr` rather than attribute assignment: the target is a third-party
    # module attribute, which a type checker treats as fixed.
    setattr(lamindb_context, "track_python_environment", cached)
    try:
        yield
    finally:
        setattr(lamindb_context, "track_python_environment", original)


def _lamindb() -> Any:
    """Import and return the `lamindb` module.

    The import is deferred to call time -- and funnelled through this one
    function -- because a plan must be declarable before LaminDB is loaded (see
    :func:`~lamindb_setup.evolve.authoring._plan._ensure_lamindb_not_loaded`).  Every entry
    point below takes an `ln` argument defaulting to this, so a caller that
    needs to drive the tracking against a stand-in LaminDB passes one explicitly
    instead of swapping the module out of :data:`sys.modules` behind our back.
    """
    import lamindb as ln

    return ln


# lamindb's `Run._status_code` for a run that ended with an error and for one
# that was interrupted (see `lamindb.base.types.RUN_STATUS_TO_CODE`).
_RUN_ERRORED = 1
_RUN_ABORTED = 2


def _failure_status_code(failure: BaseException) -> int:
    """The status a run that ended by raising `failure` is recorded with."""
    return _RUN_ABORTED if isinstance(failure, KeyboardInterrupt) else _RUN_ERRORED


def _finish_run(ln: Any, run: Any, *, failure: BaseException | None = None) -> None:
    """Call `ln.finish` and record how the command's run ended.

    `ln.finish` marks every run it closes `completed`.  LaminDB records a
    failure only from its uncaught-exception hook, which never sees an error the
    CLI reports itself -- a rolled-back `apply`, a refused `repair`.  So when
    the tracked block raised `failure`, the run is recorded `errored` once
    `ln.finish` has returned, or `aborted` for a `KeyboardInterrupt`, as
    LaminDB's own signal handler would.

    `ln.finish` also saves the log streams it captured as an artifact and
    points `run.report` at it -- announcing `overwriting run.report` when the
    command had already attached its own report (see
    :func:`attach_operation_report`).  That report is what someone reading the
    run opens first, so it is set aside while `ln.finish` runs -- which keeps
    the announcement out of the output -- and put back once it has returned.
    LaminDB saves the logs unlinked from the run, which only `run.report` would
    reach; they are recorded as an output of the run instead, so
    `ln.Artifact.filter(run=run, kind="__lamindb_run__")` still finds them.
    A run without a report of its own keeps the logs as its report.
    """
    report = getattr(run, "report", None)
    if report is None and failure is None:
        ln.finish()
        return
    if report is not None:
        run.report = None
    try:
        ln.finish()
    finally:
        if report is not None:
            logs = run.report
            if logs is not None and logs.run_id is None:
                logs.run = run
                logs.save()
            run.report = report
        if failure is not None:
            run._status_code = _failure_status_code(failure)
        run.save()


@contextmanager
def track_plan(
    plan_file: str | Path,
    *,
    stem_uid: str | None = None,
    config: TrackConfig | None = None,
    workspace: GitWorkspace | None = None,
    ln: Any | None = None,
) -> Iterator[Any]:
    """Track the application of `plan_file` as a transform run, yielding `(key, run)`.

    A context manager: `ln.track` is called on entry and `ln.finish` is
    always called on exit -- whether the `with` block completes successfully or
    raises -- so the run is closed out in both cases.  When the block raises,
    the run is recorded as errored (aborted for a `KeyboardInterrupt`) and
    the exception propagates, so callers do not need to set the run status
    themselves::

        with track_plan(plan_file, stem_uid=stem_uid) as (key, run):
            ...  # apply the plan; attach a report to `run` on failure

    A report attached during the block stays `run.report` once `ln.finish`
    has saved the run's logs, which become an output of the run instead (see
    :func:`_finish_run`).

    Gates on git state (see :func:`ensure_committed_and_pushed`), unless the
    caller already gated this plan in an enclosing :func:`verified_session`.
    Each plan is tracked under its own transform stem so that re-applying a plan
    registers as a new *version* of that plan's transform while different plans
    map to different transforms.  Pass `stem_uid` to use the plan's declared
    `PLAN_TRANSFORM_STEM_UID`; when omitted, a stem uid is derived
    deterministically from the plan's track key (see :func:`plan_transform_uid`).
    When the repository exposes an `origin` remote, LaminDB's git sync is
    enabled so the transform records the git commit/blob URL of the committed
    plan file.

    `config` is the run's :class:`TrackConfig` -- the key prefix and the space
    the run, transforms and outputs are recorded in.  It should be resolved once
    at the command boundary and passed down; when omitted it is resolved from the
    environment here.  `workspace` is the :class:`GitWorkspace` holding the
    run's git state, defaulting to the process-wide one an enclosing
    :func:`verified_session` populates.  `ln` defaults to the real LaminDB
    module (see :func:`_lamindb`).
    """
    ln = _lamindb() if ln is None else ln
    config = TrackConfig.resolve() if config is None else config
    workspace = _DEFAULT_WORKSPACE if workspace is None else workspace
    plan_path = Path(plan_file).resolve()
    # Inside a :func:`verified_session` the git gate has already run for this
    # plan, so its repo-relative path is known and re-checking it would only
    # respawn git; outside one the plan gates itself here.
    verified_rel = workspace.verified_rel(plan_path)
    if verified_rel is not None:
        key = config.key_for(verified_rel)
    else:
        key = plan_track_key(plan_path, config=config, workspace=workspace)
    if stem_uid is not None and not is_valid_stem_uid(stem_uid):
        raise TrackError(
            f"invalid transform stem uid {stem_uid!r}; expected a "
            f"{STEM_UID_LENGTH}-character base62 string."
        )
    uid = stem_uid if stem_uid is not None else plan_transform_uid(key)

    # Enable git sync so the transform records the exact commit + blob URL of the
    # plan file.  LaminDB only accepts an https origin, so temporarily present
    # one while keeping SSH for the actual git transport -- unless a session
    # already holds that rewrite open for the whole repository.
    repo_root = workspace.root_for(plan_path)
    with workspace.session_remote(repo_root) as remote:
        if remote:
            try:
                ln.settings.sync_git_repo = remote
            except (ValueError, AssertionError):
                pass

        with _cached_python_environment(ln):
            ln.track(uid, key=key, path=str(plan_path), space=config.space)
        run = ln.context.run
        failure: BaseException | None = None
        try:
            yield key, run
        except BaseException as exc:
            failure = exc
            raise
        finally:
            _finish_run(ln, run, failure=failure)


@contextmanager
def track_operation(
    name: str,
    *,
    params: dict[str, Any] | None = None,
    config: TrackConfig | None = None,
    ln: Any | None = None,
) -> Iterator[Any]:
    """Track a parameterized package operation as a transform run, yielding its run.

    Unlike :func:`track_plan`, this tracks an operation that has no plan file and
    is not gated on git state -- e.g. `repair-dangling` run against an instance.
    The transform is identified by a stable key `"<prefix>/<name>"` whose stem
    uid is derived deterministically from that key (see :func:`plan_transform_uid`),
    so re-running the operation registers as a new *version* of one transform.
    `params` is recorded as the run's parameters.

    A context manager: `ln.track` is called on entry and `ln.finish` is always
    called on exit -- whether the `with` block completes successfully or raises::

        with track_operation("repair-dangling", params=params) as run:
            ...  # perform the operation, attach a report to `run`

    As with :func:`track_plan`, a block that raises leaves the run errored, the
    attached report stays `run.report` and the logs `ln.finish` saves become
    an output of the run.

    `config` is the run's :class:`TrackConfig` (prefix and space), resolved
    from the environment when omitted.  `ln` defaults to the real LaminDB
    module (see :func:`_lamindb`).
    """
    ln = _lamindb() if ln is None else ln
    config = TrackConfig.resolve() if config is None else config
    key = config.key_for(name)
    uid = plan_transform_uid(key)

    ln.track(uid, key=key, params=params, space=config.space)
    run = ln.context.run
    failure: BaseException | None = None
    try:
        yield run
    except BaseException as exc:
        failure = exc
        raise
    finally:
        _finish_run(ln, run, failure=failure)


def attach_operation_report(
    run: Any,
    report_text: str,
    *,
    instance: str,
    prefix: str,
    operation: str,
    filename: str,
    folder: str,
    ln: Any | None = None,
) -> None:
    """Save `report_text` as an artifact and attach it to `run`.

    The report is what a run leaves behind for someone reading the instance
    later: what the operation did, or -- for a failed `apply` -- what it
    refused to do and why.  It is saved as an output of `run` and set as
    `run.report`, which :func:`track_plan` and :func:`track_operation` keep
    pointing at it after `ln.finish`.  `ln` defaults to the real LaminDB
    module (see :func:`_lamindb`).
    """
    ln = _lamindb() if ln is None else ln

    with tempfile.TemporaryDirectory() as tmp:
        report_path = Path(tmp) / filename
        report_path.write_text(report_text)
        artifact = ln.Artifact(
            report_path,
            key=f"{prefix}/{folder}/{instance.replace('/', '-')}.txt",
            description=f"{operation} report for {instance}",
        ).save()
    if run is not None:
        run.report = artifact
        run.save()
