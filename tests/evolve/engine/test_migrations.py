# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="arg-type,attr-defined,call-overload"
# Source test semantics retained.
# ruff: noqa: TC003
"""Tests for lamindb_setup.evolve.engine._migrations."""

from __future__ import annotations

import re
import types
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from lamindb_setup.evolve import (
    DataMigration,
    Feature,
    FeatureType,
    LaminDType,
    MigrationContext,
    MigrationError,
    MigrationReport,
    Record,
    RecordType,
    drop_rows,
    drop_values,
    map_to_records,
    map_to_str,
    map_values,
)
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
from lamindb_setup.evolve.engine import _migrations as migrations
from lamindb_setup.evolve.engine._apply import (
    Action,
    ApplyMode,
    DriftError,
    PlanChangeRefusedError,
    compute_changes,
    write_lock,
)
from lamindb_setup.evolve.engine._lock import LockFile

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as BiontyModuleType
    from typing import Any as LamindbModuleType


# === plans ===================================================================


@create_plan
def _str_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("mig_ft")
    Feature("compound", dtype=LaminDType(str), type=ft)


def _record_plan(migration: object, name: str):
    """Build a plan flipping `compound` to `cat[Record[Compound]]`.

    The target records are declared *after* the feature on purpose: nothing in
    the object graph links them, and no creation-order edge does either.  The
    plan only works because the engine runs migrations in their own phase, once
    every plan object has been materialized.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("mig_ft")
        rt = RecordType("Compound")
        feature = Feature(
            "compound", dtype=LaminDType(ln.Record, type__uid=rt.uid), type=ft
        )
        Record("aspirin", type=rt)
        Record("ibuprofen", type=rt)
        ctx.migrate(feature, migration)

    _plan.name = name
    return _plan


_plain_plan = _record_plan(map_to_records(), "plain")
_mapping_plan = _record_plan(
    map_to_records(mapping={"ASA": "aspirin", "n/a": None}),
    "mapping",
)
_transform_plan = _record_plan(map_to_records(transform=str.lower), "transform")
_bad_transform_plan = _record_plan(
    map_to_records(transform=lambda value: "nope"),
    "bad_transform",
)
_raising_transform_plan = _record_plan(
    map_to_records(
        transform=lambda value: (_ for _ in ()).throw(RuntimeError("boom")),
    ),
    "raising_transform",
)
_drop_plan = _record_plan(drop_values(), "drop")


@create_plan
def _self_referential_plan(ctx: PlanContext, exports: None) -> None:
    """A compound whose parent compound is itself a compound.

    `acetylsalicylic acid` is annotated with the very feature being migrated,
    so the object graph makes that record require the feature -- and it is also
    what the migration maps the stale scalar values onto, so it must exist before
    the migration runs.  Expressing the latter as a creation-order edge on the
    feature closes a cycle; deferring the migration is what keeps this plan
    expressible.
    """
    import lamindb as ln

    ft = FeatureType("mig_ft")
    rt = RecordType("Compound")
    feature = Feature(
        "compound", dtype=LaminDType(ln.Record, type__uid=rt.uid), type=ft
    )
    Record("aspirin", type=rt)
    Record("acetylsalicylic acid", type=rt, features={feature: "aspirin"})
    ctx.migrate(feature, map_to_records())


@create_plan
def _int_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("mig_ft")
    feature = Feature("compound", dtype=LaminDType(int), type=ft)
    ctx.migrate(feature, map_values(transform=lambda value: None))


@create_plan
def _back_to_str_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("mig_ft")
    feature = Feature("compound", dtype=LaminDType(str), type=ft)
    ctx.migrate(feature, map_to_str(mapping={"ibuprofen": None}))
    # The vocabulary is no longer declared: its values go with it, deliberately.
    ctx.migrate("RecordType:Compound", drop_rows())


@create_plan
def _by_description_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("mig_ft")
    rt = RecordType("Compound")
    feature = Feature(
        "compound", dtype=LaminDType(ln.Record, type__uid=rt.uid), type=ft
    )
    ctx.migrate(feature, map_to_records(field="description"))


@create_plan
def _celltype_plan(ctx: PlanContext, exports: None) -> None:
    import bionty as bt

    ft = FeatureType("mig_ft")
    feature = Feature("compound", dtype=LaminDType(bt.CellType), type=ft)
    ctx.migrate(feature, map_to_records(standardize=True))


def _keep_links(context: MigrationContext) -> None:
    """Relocate nothing: `cat[...]` and `list[cat[...]]` share one link table."""


def _list_record_plan(migration: object, name: str):
    """Build a plan flipping `compound` to `list[cat[Record[Compound]]]`."""

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("mig_ft")
        rt = RecordType("Compound")
        feature = Feature(
            "compound", dtype=LaminDType(list[ln.Record], type__uid=rt.uid), type=ft
        )
        Record("aspirin", type=rt)
        Record("ibuprofen", type=rt)
        ctx.migrate(feature, migration)

    _plan.name = name
    return _plan


_cat_to_list_plan = _list_record_plan(
    DataMigration(name="keep_links", migrate=_keep_links), "cat_to_list"
)
_str_to_list_plan = _list_record_plan(map_to_records(), "str_to_list")


@create_plan
def _empty_filter_plan(ctx: PlanContext, exports: None) -> None:
    """A `cat[Record[...]]` target whose extra filter lamindb refuses."""
    import lamindb as ln

    ft = FeatureType("mig_ft")
    rt = RecordType("Compound")
    feature = Feature(
        "compound", dtype=LaminDType(ln.Record, type__uid=rt.uid, name=" "), type=ft
    )
    ctx.migrate(feature, map_to_records())


# === helpers =================================================================


def _persist_lock(ctx: object, plan: object, path: Path) -> LockFile:
    write_lock(ctx, plan, path)  # type: ignore[arg-type]
    lock = LockFile.read(path)
    assert lock is not None
    return lock


def _seed(ln: LamindbModuleType, tmp_path: Path, **values: str) -> LockFile:
    """Apply the `str` plan and annotate one record per `name=value` pair."""
    _str_plan.reset()
    applied = compute_changes(_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _str_plan, tmp_path / "mig.lock")
    sheet = ln.Record(name="MigSheet", is_type=True).save()
    for record_name, value in values.items():
        ln.Record(name=record_name, type=sheet, features={"compound": value}).save()
    return lock


def _scalar_values(feat: object) -> list[str]:
    return sorted(feat.links_recordjson.values_list("value", flat=True))  # type: ignore[attr-defined]


def _register_local_storage(ln: LamindbModuleType) -> None:
    """Point the instance's `Storage` row at the root it is connected to.

    A restored SQLite snapshot can retain its original `Storage.root`.
    Align the row with the current temporary storage before saving artifacts.
    """
    import lamindb_setup

    root = lamindb_setup.settings.storage.root_as_str
    ln.Storage.objects.exclude(root=root).update(root=root)


# === unit tests (no instance) ================================================


def test_migration_report_summary_omits_empty_counters() -> None:
    report = MigrationReport()
    assert report.summary() is None

    report.matched = 3
    report.dropped = 1
    assert report.summary() == "3 matched, 1 dropped"

    report.note("also this")
    assert report.summary() == "3 matched, 1 dropped; also this"


def test_migrate_registers_no_creation_order_edges() -> None:
    # `ctx.migrate` must not constrain when the feature is created: migrations
    # run in their own phase after the whole plan is materialized, so an edge
    # here would only ever be redundant -- or, for a self-referential ontology,
    # a cycle.
    _self_referential_plan.reset()
    _self_referential_plan.finalize()

    ordered = [d.obj for d in _self_referential_plan.dependencies]
    feature = next(o for o in ordered if getattr(o, "name", None) == "compound")
    label = next(
        o for o in ordered if getattr(o, "name", None) == "acetylsalicylic acid"
    )
    # The only edge is the one the object graph expresses: the annotated record
    # references the feature, so the feature is created first.
    assert ordered.index(feature) < ordered.index(label)


# === str -> cat[Record] ======================================================


def test_map_to_records_relocates_matching_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, s1="aspirin", s2="ibuprofen", s3="aspirin")
    _plain_plan.reset()

    ctx = compute_changes(_plain_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    # The per-value report reaches the change details.
    assert any("3 matched" in d for d in change.details)

    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str.startswith("cat[Record[")
    # Every scalar row was relocated into the categorical store.
    assert _scalar_values(feat) == []
    assert ln.Record.get(name="s3").features.get_values()["compound"] == "aspirin"
    # Nothing was created: only the two pre-declared targets exist.
    assert sorted(
        ln.Record.filter(type__name="Compound").values_list("name", flat=True)
    ) == ["aspirin", "ibuprofen"]


def test_map_to_records_migrates_a_self_referential_ontology(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A compound's parent is a compound: `acetylsalicylic acid` is annotated
    # with the very feature being migrated *and* is what the stale value maps
    # onto.  A creation-order edge from the feature to that target would close a
    # cycle, so this only applies because migrations run after the whole plan is
    # materialized.
    lock = _seed(ln, tmp_path, s1="acetylsalicylic acid")
    _self_referential_plan.reset()

    ctx = compute_changes(_self_referential_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert any("1 matched" in d for d in change.details)

    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str.startswith("cat[Record[")
    assert _scalar_values(feat) == []
    # The stale scalar row was relocated onto the plan-declared record ...
    assert (
        ln.Record.get(name="s1").features.get_values()["compound"]
        == "acetylsalicylic acid"
    )
    # ... which itself carries a self-referential annotation, written directly
    # against the new dtype while the plan was being materialized.
    assert (
        ln.Record.get(name="acetylsalicylic acid").features.get_values()["compound"]
        == "aspirin"
    )


def test_map_to_records_mapping_redirects_and_drops(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, s1="ASA", s2="n/a", s3="ibuprofen")
    _mapping_plan.reset()

    ctx = compute_changes(_mapping_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("1 matched, 1 mapped, 1 dropped" in d for d in change.details)

    feat = ln.Feature.get(name="compound", is_type=False)
    assert _scalar_values(feat) == []
    # "ASA" was redirected onto the existing aspirin record.
    assert ln.Record.get(name="s1").features.get_values()["compound"] == "aspirin"
    # "n/a" mapped to None, so the annotation is simply gone.
    assert "compound" not in ln.Record.get(name="s2").features.get_values()


def test_map_to_records_transform_rescues_unmatched_value(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, s1="Aspirin", s2="ibuprofen")
    _transform_plan.reset()

    ctx = compute_changes(_transform_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("1 matched, 1 transformed" in d for d in change.details)
    assert ln.Record.get(name="s1").features.get_values()["compound"] == "aspirin"


def test_map_to_records_unmatched_without_transform_raises(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from django.db import transaction

    lock = _seed(ln, tmp_path, s1="paracetamol")
    _plain_plan.reset()

    with pytest.raises(MigrationError, match="does not resolve against"):
        with transaction.atomic():
            compute_changes(_plain_plan, mode=ApplyMode.APPLY, lock=lock)

    # The failed apply rolled back: the feature is untouched.
    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str == "str"
    assert _scalar_values(feat) == ["paracetamol"]


def test_map_to_records_transform_result_must_rematch(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from django.db import transaction

    lock = _seed(ln, tmp_path, s1="paracetamol")
    _bad_transform_plan.reset()

    with pytest.raises(MigrationError, match="still does not resolve"):
        with transaction.atomic():
            compute_changes(_bad_transform_plan, mode=ApplyMode.APPLY, lock=lock)

    assert ln.Feature.get(name="compound", is_type=False).dtype_as_str == "str"


def test_map_to_records_transform_raising_rolls_back(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from django.db import transaction

    lock = _seed(ln, tmp_path, s1="paracetamol")
    _raising_transform_plan.reset()

    with pytest.raises(RuntimeError, match="boom"):
        with transaction.atomic():
            compute_changes(_raising_transform_plan, mode=ApplyMode.APPLY, lock=lock)

    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str == "str"
    assert _scalar_values(feat) == ["paracetamol"]


def test_map_to_records_relocates_both_scalar_stores(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Values live in two stores: RecordJson (per record) and JsonValue (shared by
    # the artifacts and runs it annotates). A helper must drain both, or the
    # engine's stranded check fires.
    from lamindb.models import JsonValue

    lock = _seed(ln, tmp_path, s1="aspirin")
    feat = ln.Feature.get(name="compound", is_type=False)
    JsonValue(feature=feat, value="ibuprofen").save()
    assert feat.values.count() == 1
    _plain_plan.reset()

    compute_changes(_plain_plan, mode=ApplyMode.APPLY, lock=lock)

    feat = ln.Feature.get(name="compound", is_type=False)
    assert _scalar_values(feat) == []
    assert feat.values.count() == 0


def test_map_to_records_previews_report_without_persisting(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, s1="aspirin", s2="ibuprofen")
    # The dry-run can only resolve the target dtype once the record type exists,
    # so create it up front -- the re-plan case, after a first apply.
    rt = ln.Record(name="Compound", is_type=True).save()
    ln.Record(name="aspirin", type=rt).save()
    ln.Record(name="ibuprofen", type=rt).save()
    _plain_plan.reset()

    ctx = compute_changes(
        _plain_plan, mode=ApplyMode.PLAN, lock=lock, preview_migrations=True
    )

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("would apply 2 matched" in d for d in change.details), change.details
    # A dry-run persists nothing.
    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str == "str"
    assert _scalar_values(feat) == ["aspirin", "ibuprofen"]


def test_plan_skips_dry_run_when_the_target_type_is_not_created_yet(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The record type is declared by this very plan, so in PLAN mode its uid is
    # still a placeholder: the migration cannot be dry-run, but planning must
    # still report the intended dtype change rather than crash.
    lock = _seed(ln, tmp_path, s1="aspirin", s2="ibuprofen")
    _plain_plan.reset()

    ctx = compute_changes(
        _plain_plan, mode=ApplyMode.PLAN, lock=lock, preview_migrations=True
    )

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("not previewed" in d for d in change.details), change.details
    # The note names the filter that is still a placeholder, rather than
    # relaying whatever lamindb raised when handed the placeholder uid.
    assert any(
        "known only after apply: RecordType('Compound').uid" in d
        for d in change.details
    ), change.details
    assert any("via map_to_records" in d for d in change.details), change.details
    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str == "str"


def test_plan_surfaces_a_target_dtype_that_cannot_be_built(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Only a placeholder filter is a reason to skip the dry-run; any other
    # failure to build the target dtype would fail the apply too, so the plan
    # must fail loudly instead of passing it off as "does not exist yet".
    lock = _seed(ln, tmp_path, s1="aspirin")
    ln.Record(name="Compound", is_type=True).save()
    _empty_filter_plan.reset()

    with pytest.raises(ValueError, match="Empty value in filter name"):
        compute_changes(
            _empty_filter_plan, mode=ApplyMode.PLAN, lock=lock, preview_migrations=True
        )


def test_scalar_migration_does_not_restage_the_existing_feature(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Staging an existing feature under a new dtype only works for a scalar
    # target by an accident of lamindb's constructor ordering (and logs spurious
    # "updated ..." warnings for the live record): the full dtype is built
    # without it.
    lock = _seed(ln, tmp_path, s1="aspirin")
    _plain_plan.reset()

    def _no_stage(self: Feature, ctx: object) -> None:
        raise AssertionError(f"Feature {self.name!r} was staged")

    monkeypatch.setattr(Feature, "stage", _no_stage)
    compute_changes(_plain_plan, mode=ApplyMode.APPLY, lock=lock)

    type_uid = ln.Record.get(name="Compound", is_type=True).uid
    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str == f"cat[Record[{type_uid}]]"


# === cat[Record] / str -> list[cat[Record]] ==================================


def _seed_categorical(ln: LamindbModuleType, tmp_path: Path) -> LockFile:
    """Seed `compound` as `cat[Record[Compound]]` with `s1` -> aspirin."""
    lock = _seed(ln, tmp_path, s1="aspirin")
    _plain_plan.reset()
    migrated = compute_changes(_plain_plan, mode=ApplyMode.APPLY, lock=lock)
    return _persist_lock(migrated, _plain_plan, tmp_path / "cat.lock")


def test_migration_to_a_filtered_list_cat_applies(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed_categorical(ln, tmp_path)
    type_uid = ln.Record.get(name="Compound", is_type=True).uid
    _cat_to_list_plan.reset()

    ctx = compute_changes(_cat_to_list_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    feat = ln.Feature.get(name="compound", is_type=False)
    # The type filter survives the flip rather than being dropped.
    assert feat.dtype_as_str == f"list[cat[Record[{type_uid}]]]"
    # cat[...] and list[cat[...]] share the link table, so the stored link
    # survives the flip and now reads back as a one-element list.
    assert ln.Record.get(name="s1").features.get_values()["compound"] == ["aspirin"]


def test_migration_from_str_to_a_filtered_list_cat_applies(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, s1="aspirin", s2="ibuprofen")
    _str_to_list_plan.reset()

    compute_changes(_str_to_list_plan, mode=ApplyMode.APPLY, lock=lock)

    type_uid = ln.Record.get(name="Compound", is_type=True).uid
    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str == f"list[cat[Record[{type_uid}]]]"
    assert _scalar_values(feat) == []
    assert ln.Record.get(name="s2").features.get_values()["compound"] == ["ibuprofen"]


def test_plan_previews_a_migration_to_a_filtered_list_cat(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed_categorical(ln, tmp_path)
    type_uid = ln.Record.get(name="Compound", is_type=True).uid
    _cat_to_list_plan.reset()

    ctx = compute_changes(
        _cat_to_list_plan, mode=ApplyMode.PLAN, lock=lock, preview_migrations=True
    )

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert not any("does not exist yet" in d for d in change.details), change.details
    # The full target dtype, filter included, not the bare `list[cat[Record]]`.
    assert any(
        f"\u2192 'list[cat[Record[{type_uid}]]]' via keep_links" in d
        for d in change.details
    ), change.details
    assert any(
        "dry-run: keep_links relocated all values, none stranded" in d
        for d in change.details
    ), change.details
    # A plan persists nothing.
    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str == f"cat[Record[{type_uid}]]"


# === cat[Record] -> str ======================================================


def test_map_to_str_renders_labels_and_drops(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, s1="aspirin", s2="ibuprofen")
    _plain_plan.reset()
    migrated = compute_changes(_plain_plan, mode=ApplyMode.APPLY, lock=lock)
    lock = _persist_lock(migrated, _plain_plan, tmp_path / "mig.lock")
    _back_to_str_plan.reset()

    compute_changes(_back_to_str_plan, mode=ApplyMode.APPLY, lock=lock)

    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str == "str"
    # The label rendered back to its name; the mapped-to-None one was dropped.
    assert _scalar_values(feat) == ["aspirin"]
    assert feat.links_recordrecord.count() == 0


# === scalar -> scalar ========================================================


def test_map_values_coerces_and_drops(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, s1="42", s2="notanint")
    _int_plan.reset()

    ctx = compute_changes(_int_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("1 matched, 1 dropped" in d for d in change.details)
    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.dtype_as_str == "int"
    assert _scalar_values(feat) == [42]


def test_write_leaves_what_it_cannot_link_itself_to_lamindb(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # A categorical value the batched link rows cannot express is handed back to
    # lamindb.  On a `Record` that has to be `update_values`: lamindb >= 2.12
    # refuses `add_values` for a single-valued feature there.
    label = ln.ULabel(name="mig_label").save()
    feat = ln.Feature(name="mig_cat", dtype=ln.ULabel).save()
    owner = ln.Record(name="mig_owner").save()

    migrations._write(owner, feat, label)

    assert owner.features.get_values() == {"mig_cat": "mig_label"}


# === drop everything =========================================================


def test_drop_values_clears_every_store(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from lamindb.models import JsonValue

    lock = _seed(ln, tmp_path, s1="aspirin", s2="paracetamol")
    feat = ln.Feature.get(name="compound", is_type=False)
    JsonValue(feature=feat, value="orphan").save()
    _drop_plan.reset()

    ctx = compute_changes(_drop_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("3 dropped" in d for d in change.details)
    feat = ln.Feature.get(name="compound", is_type=False)
    assert _scalar_values(feat) == []
    assert feat.values.count() == 0


def test_drop_values_clears_a_value_an_artifact_still_links(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A `JsonValue` is reached from the artifacts and runs it annotates through
    # link tables whose foreign key back to it is `PROTECT`, so deleting the
    # value while a link survives raises Django's `ProtectedError` and rolls
    # the whole apply back.  The links have to go first.
    _register_local_storage(ln)
    lock = _seed(ln, tmp_path, s1="aspirin")
    payload = tmp_path / "payload.txt"
    payload.write_text("payload")
    artifact = ln.Artifact(payload, key="drop/payload.txt").save()
    artifact.features.add_values({"compound": "ibuprofen"})
    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.values.count() == 1
    _drop_plan.reset()

    ctx = compute_changes(_drop_plan, mode=ApplyMode.APPLY, lock=lock)

    assert {c.kind: c for c in ctx.changes}["Feature"].action is Action.UPDATE
    feat = ln.Feature.get(name="compound", is_type=False)
    assert feat.values.count() == 0
    assert "compound" not in artifact.features.get_values()


# === batched categorical writes ==============================================


def _link_fixture(
    ln: LamindbModuleType, tmp_path: Path
) -> tuple[object, object, object]:
    """Seed one annotated record and return `(feature, owner, target)`."""
    _seed(ln, tmp_path, s1="aspirin")
    feature = ln.Feature.get(name="compound", is_type=False)
    record_type = ln.Record(name="Compound", is_type=True).save()
    target = ln.Record(name="aspirin", type=record_type).save()
    return feature, ln.Record.get(name="s1"), target


def test_relocation_writes_link_rows_instead_of_add_values(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
) -> None:
    # The whole point of the batched path: a str -> cat[Record] relocation writes
    # the link rows itself, so `add_values` -- which re-resolves the feature and
    # runs a curator per annotated object -- is never reached.  Both paths end
    # with the same link rows, so the migration records which one it took.
    lock = _seed(ln, tmp_path, s1="aspirin", s2="ibuprofen")
    _plain_plan.reset()

    ctx = compute_changes(_plain_plan, mode=ApplyMode.APPLY, lock=lock)

    details = [detail for change in ctx.changes for detail in change.details]
    assert any("written via batched link rows" in detail for detail in details), details
    assert not any("written via add_values" in detail for detail in details), details

    feat = ln.Feature.get(name="compound", is_type=False)
    assert _scalar_values(feat) == []
    assert sorted(feat.links_recordrecord.values_list("value__name", flat=True)) == [
        "aspirin",
        "ibuprofen",
    ]


def test_plan_link_writes_declines_without_saved_rows(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    feature, owner, target = _link_fixture(ln, tmp_path)

    # Nothing to write, an unsaved feature and an unsaved target all mean the
    # link rows cannot be addressed by primary key.
    assert migrations._plan_link_writes(feature, []) is None
    unsaved_feature = ln.Feature(name="new", dtype=str)
    assert migrations._plan_link_writes(unsaved_feature, [(owner, target)]) is None
    unsaved_target = ln.Record(name="unsaved")
    assert migrations._plan_link_writes(feature, [(owner, unsaved_target)]) is None


def test_plan_link_writes_deduplicates_repeated_links(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from lamindb.models import RecordRecord

    feature, owner, target = _link_fixture(ln, tmp_path)

    # Both scalar stores can hold the same value for one record, and the link
    # table is unique on (record, feature, value), so the repeat collapses away.
    rows = migrations._plan_link_writes(feature, [(owner, target), (owner, target)])

    assert [type(row) for row in rows] == [RecordRecord]
    assert (rows[0].record_id, rows[0].feature_id, rows[0].value_id) == (
        owner.id,
        feature.id,
        target.id,
    )


def test_plan_link_writes_declines_for_a_schema_index_feature(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # An index feature is stored on `Record.name`, not in a link table, so only
    # `add_values` knows how to write it.
    feature, owner, target = _link_fixture(ln, tmp_path)
    sheet = ln.Record.get(name="MigSheet", is_type=True)
    # lamindb's `Schema.__init__` prepends the index to `features`, so it
    # must not be listed there as well.
    other = ln.Feature(name="note", dtype=str).save()
    sheet.schema = ln.Schema(features=[other], index=feature).save()
    sheet.save()

    assert migrations._plan_link_writes(feature, [(owner, target)]) is None


def test_plan_link_writes_declines_for_an_unknown_owner_registry(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # lamindb has no categorical link table pairing this registry with the
    # target, so the write has to go back through `add_values`.
    feature, _owner, target = _link_fixture(ln, tmp_path)
    stranger = types.SimpleNamespace(pk=1, _state=types.SimpleNamespace(adding=False))

    assert migrations._plan_link_writes(feature, [(stranger, target)]) is None


def test_plan_link_writes_declines_when_the_link_model_lacks_columns(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
) -> None:
    # Defensive: a future lamindb link table that is not keyed the way this code
    # assumes must be handed back to `add_values` rather than mis-populated.
    # The shape check lives in the adapter, so a stand-in that reports no usable
    # link is what a mis-keyed table would look like from here.
    from lamindb_setup.evolve.core import LamindbModels

    class _MisKeyedLinkTables(LamindbModels):
        def categorical_link(self, owner_model, value_model, *, instance):
            return None

    feature, owner, target = _link_fixture(ln, tmp_path)

    assert (
        migrations._plan_link_writes(
            feature, [(owner, target)], models=_MisKeyedLinkTables()
        )
        is None
    )


def test_delete_all_falls_back_when_permanent_is_unsupported() -> None:
    calls: list[dict] = []

    class _QuerySet:
        def delete(self, **kwargs: object) -> None:
            calls.append(kwargs)
            if kwargs:
                raise TypeError("delete() got an unexpected keyword 'permanent'")

    class _Manager:
        def filter(self, **kwargs: object) -> _QuerySet:
            calls.append(kwargs)
            return _QuerySet()

    class _Row:
        objects = _Manager()
        pk = 7

    migrations._delete_all([migrations._StoredValue(row=_Row(), owners=(), value="x")])

    assert calls == [{"pk__in": [7]}, {"permanent": True}, {}]


# === scalar conversions ======================================================


@pytest.mark.parametrize(
    ("dtype", "value", "expected"),
    [
        ("int", "42", 42),
        ("int", 42.0, 42),
        ("int", "42.7", None),  # would truncate
        ("int", True, None),  # a bool is not an int annotation
        ("int", "notanint", None),
        ("float", "42.7", 42.7),
        ("num", "42", 42),
        ("num", "42.7", 42.7),
        ("bool", True, True),
        ("bool", "yes", None),  # truthiness is not a conversion
        ("str", 42, "42"),
    ],
)
def test_scalar_casts_refuse_to_change_a_value_silently(
    dtype: str, value: object, expected: object
) -> None:
    from lamindb_setup.evolve.engine._migrations import _scalar_lookup

    context = MigrationContext(feature=None, desired_dtype=dtype, live_dtype="str")
    assert _scalar_lookup(context)(value) == expected


def test_scalar_lookup_rejects_an_unsupported_target_dtype() -> None:
    from lamindb_setup.evolve.engine._migrations import _scalar_lookup

    context = MigrationContext(feature=None, desired_dtype="date", live_dtype="str")
    with pytest.raises(MigrationError, match="no built-in conversion"):
        _scalar_lookup(context)


def test_map_to_records_reports_an_ambiguous_target(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Two records share the lookup field's value, so the migration refuses to
    # guess which one the annotation meant.
    lock = _seed(ln, tmp_path, s1="aspirin")
    rt = ln.Record(name="Compound", is_type=True).save()
    ln.Record(name="asa", type=rt, description="aspirin").save()
    ln.Record(name="acetylsalicylic acid", type=rt, description="aspirin").save()
    _by_description_plan.reset()

    with pytest.raises(MigrationError, match="matches more than one record"):
        compute_changes(_by_description_plan, mode=ApplyMode.APPLY, lock=lock)


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_map_to_records_standardize_resolves_a_synonym(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    bt: BiontyModuleType,
    tmp_path: Path,
) -> None:
    # `standardize=True` routes an unmatched value through the registry's
    # synonym table before giving up on it.
    lock = _seed(ln, tmp_path, s1="T lymphocyte", s2="B cell")
    bt.CellType(name="T cell", synonyms="T lymphocyte").save()
    bt.CellType(name="B cell").save()
    _celltype_plan.reset()

    ctx = compute_changes(_celltype_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    # The synonym resolves inside the lookup, so it counts as a plain match.
    assert any("2 matched" in d for d in change.details), change.details
    feat = ln.Feature.get(name="compound", is_type=False)
    assert _scalar_values(feat) == []
    assert ln.Record.get(name="s1").features.get_values()["compound"] == "T cell"
    assert ln.Record.get(name="s2").features.get_values()["compound"] == "B cell"


def test_in_clause_batches_bounds_a_single_statement() -> None:
    """A feature with more annotations than the backend's bind-parameter cap."""
    from lamindb_setup.evolve.engine._migrations import (
        _DELETE_BATCH_SIZE,
        _in_clause_batches,
    )

    pks = list(range(_DELETE_BATCH_SIZE * 2 + 7))

    batches = list(_in_clause_batches(pks))

    assert len(batches) == 3
    assert [len(b) for b in batches] == [
        _DELETE_BATCH_SIZE,
        _DELETE_BATCH_SIZE,
        7,
    ]
    # every key is emitted exactly once, in order
    assert [pk for batch in batches for pk in batch] == pks
    assert list(_in_clause_batches([])) == []


def test_delete_all_splits_its_in_clause() -> None:
    """The delete stays one query per table *per batch*, never one per row."""
    from lamindb_setup.evolve.engine import _migrations as migrations_mod

    seen: list[list[int]] = []

    class _QuerySet:
        def delete(self, permanent: bool = False) -> None:
            return None

    class _Manager:
        def filter(self, **kwargs: object) -> _QuerySet:
            seen.append(list(kwargs["pk__in"]))  # type: ignore[arg-type]
            return _QuerySet()

    class _Row:
        objects = _Manager()

        def __init__(self, pk: int) -> None:
            self.pk = pk

    stored = [
        migrations_mod._StoredValue(row=_Row(i), owners=(), value=None)
        for i in range(5)
    ]

    migrations_mod._delete_all(
        stored, batch_size=2, unlink_owners=lambda model, pks: None
    )

    assert seen == [[0, 1], [2, 3], [4]]


# === categorical filter changes =======================================


def _filter_plan(
    target: str,
    migration: object | None,
    name: str,
    extra_filters: dict[str, str] | None = None,
):
    """Build a plan whose `vocab` feature is filtered on record type `target`.

    Both record types and an `alpha` label under each are declared whatever
    `target` is, so the plans differ only in the feature's filters and the
    migration.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("mig_ft")
        types = {"A": RecordType("A"), "B": RecordType("B")}
        for record_type in types.values():
            Record("alpha", type=record_type)
        dtype = LaminDType(
            ln.Record, type__uid=types[target].uid, **(extra_filters or {})
        )
        feature = Feature("vocab", dtype=dtype, type=ft)
        if migration is not None:
            ctx.migrate(feature, migration)

    _plan.name = name
    return _plan


def _relink_to_b(context: MigrationContext) -> None:
    """Re-point every `vocab` link at the same-named label under `B`."""
    import lamindb as ln

    b = ln.Record.get(name="B", is_type=True)
    for link in context.feature.links_recordrecord.all():
        link.value = ln.Record.get(name=link.value.name, type=b)
        link.save()


_on_a_plan = _filter_plan("A", None, "on_a")
_on_b_plan = _filter_plan("B", None, "on_b")
_alpha_on_a_plan = _filter_plan("A", None, "alpha_on_a", {"name": "alpha"})
_to_b_plan = _filter_plan(
    "B", DataMigration(name="relink_to_b", migrate=_relink_to_b), "to_b"
)


def _seed_on_a(
    ln: LamindbModuleType, tmp_path: Path, plan: object = _on_a_plan
) -> LockFile:
    """Apply a plan filtering on `A` and annotate `row1` with `alpha@A`."""
    plan.reset()  # type: ignore[attr-defined]
    applied = compute_changes(plan, mode=ApplyMode.APPLY, lock=None)  # type: ignore[arg-type]
    lock = _persist_lock(applied, plan, tmp_path / "on_a.lock")
    sheet = ln.Record(name="VocabSheet", is_type=True).save()
    alpha = ln.Record.get(name="alpha", type=ln.Record.get(name="A", is_type=True))
    ln.Record(name="row1", type=sheet, features={"vocab": alpha}).save()
    return lock


def _type_uid(ln: LamindbModuleType, name: str) -> str:
    return ln.Record.get(name=name, is_type=True).uid


def test_cat_filter_change_with_a_migration_flips_the_live_dtype(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `cat[Record[A]]` and `cat[Record[B]]` are different dtypes.  They used to
    # be compared without their filters, so the declared migration never ran, the
    # instance kept validating against A, and the rebuilt lock recorded B.
    lock = _seed_on_a(ln, tmp_path)
    a_uid, b_uid = _type_uid(ln, "A"), _type_uid(ln, "B")
    _to_b_plan.reset()

    ctx = compute_changes(_to_b_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert any(
        f"'cat[Record[{a_uid}]]' \u2192 'cat[Record[{b_uid}]]' via relink_to_b" in d
        for d in change.details
    ), change.details
    feat = ln.Feature.get(name="vocab", is_type=False)
    assert feat.dtype_as_str == f"cat[Record[{b_uid}]]"
    (link,) = feat.links_recordrecord.all()
    assert link.value.type.uid == b_uid
    # The rebuilt lock agrees with the instance, not only with the plan.
    rebuilt = _persist_lock(ctx, _to_b_plan, tmp_path / "to_b.lock")
    entry = rebuilt.entry_for("FeatureType:mig_ft/Feature:vocab")
    assert entry is not None
    assert entry.config["cat_filters"] == {"type__uid": {"ref": "RecordType:B"}}
    assert entry.remote_snapshot is not None
    assert entry.remote_snapshot["dtype"] == f"cat[Record[{b_uid}]]"


def test_cat_filter_change_is_dry_run_by_plan(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed_on_a(ln, tmp_path)
    a_uid, b_uid = _type_uid(ln, "A"), _type_uid(ln, "B")
    _to_b_plan.reset()

    ctx = compute_changes(
        _to_b_plan, mode=ApplyMode.PLAN, lock=lock, preview_migrations=True
    )

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert any(
        f"dtype 'cat[Record[{a_uid}]]' \u2192 'cat[Record[{b_uid}]]'" in d
        for d in change.details
    ), change.details
    assert any(
        "dry-run: relink_to_b relocated all values" in d for d in change.details
    ), change.details
    # A plan persists nothing.
    feat = ln.Feature.get(name="vocab", is_type=False)
    assert feat.dtype_as_str == f"cat[Record[{a_uid}]]"


def test_cat_filter_change_without_a_migration_is_refused(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed_on_a(ln, tmp_path)
    _on_b_plan.reset()

    a_uid, b_uid = _type_uid(ln, "A"), _type_uid(ln, "B")
    # The filter is part of the dtype: one line, spelled as the diff spells it,
    # rather than the lock's `cat_filters` references.
    with pytest.raises(
        PlanChangeRefusedError,
        match=re.escape(
            "cannot change Feature 'vocab' (FeatureType:mig_ft/Feature:vocab) in "
            f"place:\n  dtype 'cat[Record[{a_uid}]]' \u2192 'cat[Record[{b_uid}]]'"
        ),
    ) as excinfo:
        compute_changes(_on_b_plan, mode=ApplyMode.APPLY, lock=lock)
    assert "cat_filters" not in str(excinfo.value)

    feat = ln.Feature.get(name="vocab", is_type=False)
    assert feat.dtype_as_str == f"cat[Record[{_type_uid(ln, 'A')}]]"


def test_cat_filter_change_without_a_lock_is_refused(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Without a lock only the diff against the instance judges the change, and it
    # used to read the two filters as the same dtype: no change, nothing applied.
    _seed_on_a(ln, tmp_path)
    a_uid, b_uid = _type_uid(ln, "A"), _type_uid(ln, "B")
    _on_b_plan.reset()

    with pytest.raises(PlanChangeRefusedError) as excinfo:
        compute_changes(_on_b_plan, mode=ApplyMode.PLAN, lock=None)

    assert f"dtype 'cat[Record[{a_uid}]]' \u2192 'cat[Record[{b_uid}]]'" in str(
        excinfo.value
    )


def test_dropping_a_cat_filter_applies_in_place(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Dropping a filter only admits more labels, so it needs neither the flag
    # nor a migration -- and the link survives the flip untouched.
    lock = _seed_on_a(ln, tmp_path, _alpha_on_a_plan)
    a_uid = _type_uid(ln, "A")
    feat = ln.Feature.get(name="vocab", is_type=False)
    assert feat.dtype_as_str == f"cat[Record[{a_uid}, name=alpha]]"
    _on_a_plan.reset()

    ctx = compute_changes(_on_a_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    feat.refresh_from_db()
    assert feat.dtype_as_str == f"cat[Record[{a_uid}]]"
    assert ln.Record.get(name="row1").features.get_values()["vocab"] == "alpha"


def test_cat_filter_changed_outside_the_plan_is_remote_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The remote snapshot records the dtype with its filter inlined, so a filter
    # changed on the instance is a dtype change too -- not a safe edit to keep.
    lock = _seed_on_a(ln, tmp_path)
    feat = ln.Feature.get(name="vocab", is_type=False)
    feat._dtype_str = f"cat[Record[{_type_uid(ln, 'B')}]]"
    feat.save()
    _on_a_plan.reset()

    with pytest.raises(DriftError, match="remote drift") as excinfo:
        compute_changes(_on_a_plan, mode=ApplyMode.PLAN, lock=lock)

    assert "dtype: instance now" in str(excinfo.value)
    # Genuine drift keeps its wording and is not a refused plan change.
    assert str(excinfo.value).startswith(
        "remote drift for 'FeatureType:mig_ft/Feature:vocab': the instance was "
        "changed outside the plan"
    )
    assert not isinstance(excinfo.value, PlanChangeRefusedError)


# === categorical sources ==============================================


@create_plan
def _ulabel_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("mig_ft")
    Feature("tag", dtype=LaminDType(ln.ULabel), type=ft)


def _ulabel_to_record_plan(migration: object, name: str):
    """Build a plan flipping `tag` from `cat[ULabel]` to `cat[Record[RT]]`."""

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("mig_ft")
        rt = RecordType("RT")
        Record("beta", type=rt)
        feature = Feature("tag", dtype=LaminDType(ln.Record, type__uid=rt.uid), type=ft)
        ctx.migrate(feature, migration)

    _plan.name = name
    return _plan


_ulabel_to_record_map_plan = _ulabel_to_record_plan(map_to_records(), "ulabel_map")


def _noop(context: MigrationContext) -> None:
    """Relocate nothing."""


_noop_to_b_plan = _filter_plan("B", DataMigration(name="noop", migrate=_noop), "noop_b")


@create_plan
def _subtype_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("mig_ft")
    parent = RecordType("P")
    child = RecordType("C", type=parent)
    Record("gamma", type=child)
    Feature("vocab", dtype=LaminDType(ln.Record, type__uid=child.uid), type=ft)


@create_plan
def _up_to_parent_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("mig_ft")
    parent = RecordType("P")
    child = RecordType("C", type=parent)
    Record("gamma", type=child)
    feature = Feature(
        "vocab", dtype=LaminDType(ln.Record, type__uid=parent.uid), type=ft
    )
    ctx.migrate(feature, DataMigration(name="noop", migrate=_noop))


def test_map_to_records_refuses_a_categorical_source(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `map_to_records` reads the scalar stores only.  On `cat[ULabel] ->
    # cat[Record[RT]]` it used to relocate nothing, report success, and leave
    # every `RecordULabel` link under a feature that only admits Records.
    from django.db import transaction

    _ulabel_plan.reset()
    applied = compute_changes(_ulabel_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _ulabel_plan, tmp_path / "ulabel.lock")
    sheet = ln.Record(name="TagSheet", is_type=True).save()
    beta = ln.ULabel(name="beta").save()
    ln.Record(name="row1", type=sheet, features={"tag": beta}).save()
    _ulabel_to_record_map_plan.reset()

    with pytest.raises(MigrationError, match="relocates scalar values only"):
        with transaction.atomic():
            compute_changes(_ulabel_to_record_map_plan, mode=ApplyMode.APPLY, lock=lock)

    feat = ln.Feature.get(name="tag", is_type=False)
    assert feat.dtype_as_str == "cat[ULabel]"
    assert feat.links_recordulabel.count() == 1


def test_a_migration_leaving_links_outside_the_new_filter_fails(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The post-check used to count only the store the *other* storage class
    # uses, so a `cat -> cat` change passed whatever the migration did.
    from django.db import transaction

    lock = _seed_on_a(ln, tmp_path)
    _noop_to_b_plan.reset()

    with pytest.raises(MigrationError, match="left 1 value"):
        with transaction.atomic():
            compute_changes(_noop_to_b_plan, mode=ApplyMode.APPLY, lock=lock)

    feat = ln.Feature.get(name="vocab", is_type=False)
    assert feat.dtype_as_str == f"cat[Record[{_type_uid(ln, 'A')}]]"


def test_plan_dry_run_reports_links_outside_the_new_filter(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed_on_a(ln, tmp_path)
    _noop_to_b_plan.reset()

    ctx = compute_changes(
        _noop_to_b_plan, mode=ApplyMode.PLAN, lock=lock, preview_migrations=True
    )

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any(
        "dry-run: noop would leave 1 value(s) stranded" in d for d in change.details
    ), change.details


def test_links_into_a_subtype_of_the_new_type_are_admitted(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # lamindb validates `cat[Record[P]]` subtype-inclusively, so moving the
    # filter up to an ancestor type strands nothing and needs no relocation.
    _subtype_plan.reset()
    applied = compute_changes(_subtype_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _subtype_plan, tmp_path / "sub.lock")
    sheet = ln.Record(name="VocabSheet", is_type=True).save()
    gamma = ln.Record.get(name="gamma")
    ln.Record(name="row1", type=sheet, features={"vocab": gamma}).save()
    _up_to_parent_plan.reset()

    compute_changes(_up_to_parent_plan, mode=ApplyMode.APPLY, lock=lock)

    feat = ln.Feature.get(name="vocab", is_type=False)
    assert feat.dtype_as_str == f"cat[Record[{_type_uid(ln, 'P')}]]"
    assert feat.links_recordrecord.get().value == gamma


@pytest.mark.parametrize(
    ("setting", "value", "message"),
    [
        ("updates", ("unknown",), "updates must be among"),
        ("index_name_conflict", "skip", "index_name_conflict must be one of"),
        ("relocation", "invalid", "relocation must be one of"),
    ],
)
def test_migration_rejects_unknown_resolution(setting, value, message) -> None:
    with pytest.raises(ValueError, match=message):
        DataMigration(name="fix metadata", migrate=lambda ctx: None, **{setting: value})


def test_lamindb_dtype_adapter_handles_unknown_dtypes_and_resolves_index_names(
    ln,
) -> None:
    from lamindb_setup.evolve.core._lamindb_api import DEFAULT_MODELS

    assert DEFAULT_MODELS.parse_dtype("cat[NotARegistry]") is None
    assert DEFAULT_MODELS.itype_str(object()) is None
    assert DEFAULT_MODELS.cat_filter_lookups("source__uid=sample") == {
        "source__uid": "sample"
    }
    assert DEFAULT_MODELS.cat_filter_lookups("broken=") is None
    index = ln.Feature(name="sample id", dtype=str)
    assert DEFAULT_MODELS.index_value_as_record_name("A01", index) == "A01"
    assert DEFAULT_MODELS.index_value_as_record_name(None, index) is None
    with pytest.raises(TypeError, match="sample id"):
        DEFAULT_MODELS.index_value_as_record_name(12, index)


def test_migration_contexts_expose_affected_links_and_adopt_stranded_exports() -> None:
    from types import SimpleNamespace
    from unittest.mock import Mock

    from lamindb_setup.evolve.authoring._migrations import (
        FlexibleChangeContext,
        RecordRemovalContext,
        SheetExportContext,
    )

    record = object()
    links = Mock(return_value=["link"])
    assert RecordRemovalContext(record, value_links=links).links() == ["link"]
    links.assert_called_once_with(record)
    schema = object()
    columns = Mock(return_value=["column"])
    assert FlexibleChangeContext(
        schema, True, False, recorded_nonmembers=columns
    ).nonmember_columns() == ["column"]
    columns.assert_called_once_with(schema)

    branch = SimpleNamespace(id=3)
    parked = Mock(pk=7, is_latest=True, branch_id=2)
    export = SimpleNamespace(artifact=SimpleNamespace(pk=6))
    ctx = SheetExportContext(
        schema,
        [export],
        branch_factory=lambda: branch,
        reexport=lambda *args, **kwargs: parked,
        current_branch=lambda: SimpleNamespace(id=1),
    )
    assert ctx.write(export) is parked
    parked.save.assert_called_once_with()
    assert parked.branch is branch and ctx.written == [parked]


def test_unsupported_categorical_link_shape_falls_back_to_standard_writer(
    ln, monkeypatch
):
    from lamindb.models import _feature_manager
    from lamindb_setup.evolve.core._lamindb_api import DEFAULT_MODELS

    monkeypatch.setattr(
        _feature_manager,
        "get_categorical_link_info",
        lambda *args, **kwargs: (ln.Record, "value", None),
    )
    assert DEFAULT_MODELS.categorical_link(ln.Record, ln.ULabel, instance=None) is None
