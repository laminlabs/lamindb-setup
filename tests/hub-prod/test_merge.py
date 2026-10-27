"""Tests for ln.setup.merge."""

import lamindb as ln
import pytest


@pytest.fixture
def merge_records(request):
    source = ln.Branch(name=request.node.name).save()
    feature = ln.Feature(name=request.node.name, dtype=str, branch=source).save()
    label = ln.ULabel(name=request.node.name, branch=source).save()
    try:
        yield source, ln.Branch.get(name="main"), feature, label
    finally:
        feature.delete(permanent=True)
        label.delete(permanent=True)
        source.delete(permanent=True)


@pytest.mark.parametrize(
    "restriction", ["space", "record", "block", "branch", "locked"]
)
def test_merge_preflight_checks_rls_before_writes(restriction, request):
    from django.db import connection

    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL row-level security is required")
    with connection.cursor() as cursor:
        cursor.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
        if not cursor.fetchone()[0]:
            pytest.skip("An isolated PostgreSQL superuser connection is required")

    source, target, feature, label = request.getfixturevalue("merge_records")
    restricted = feature
    if restriction == "block":
        record = ln.Record(name="merge-preflight-block", branch=source).save()
        restricted = ln.models.RecordBlock(
            record=record, content="restricted", kind="readme", branch=source
        ).save()
    elif restriction == "branch":
        restricted = source
    table = connection.ops.quote_name(restricted._meta.db_table)
    role = connection.ops.quote_name(f"merge_preflight_{source.uid}")
    condition = f"id <> {restricted.pk}"
    grant_condition = f"id = {restricted.pk}"
    if restriction == "space":
        condition = f"space_id <> {restricted.space_id}"
    elif restriction == "block":
        parent_table = connection.ops.quote_name(ln.Record._meta.db_table)
        parent = f"SELECT 1 FROM {parent_table} parent WHERE parent.id = record_id"
        condition = f"EXISTS ({parent} AND parent.space_id <> {record.space_id})"
        grant_condition = f"EXISTS ({parent} AND parent.id = {record.pk})"
    elif restriction == "locked":
        ln.Feature.objects.filter(pk=feature.pk).update(is_locked=True)
        condition = "NOT is_locked"
    updates = []

    def capture_updates(execute, sql, params, many, context):
        if sql.lstrip().upper().startswith("UPDATE "):
            updates.append(sql)
        return execute(sql, params, many, context)

    with connection.cursor() as cursor:
        cursor.execute(f"CREATE ROLE {role} NOLOGIN")
        cursor.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
        cursor.execute(f"GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA public TO {role}")
        cursor.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        cursor.execute(
            f"CREATE POLICY merge_preflight_read ON {table} FOR SELECT TO {role} USING (true)"
        )
        cursor.execute(
            f"CREATE POLICY merge_preflight_write ON {table} FOR UPDATE TO {role} "
            f"USING ({condition}) WITH CHECK (true)"
        )
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SET ROLE {role}")
        with connection.execute_wrapper(capture_updates):
            with pytest.raises(PermissionError, match="write access") as error:
                ln.setup.merge(source, target=target)
        assert updates == []
        assert restricted._meta.label in str(error.value)
        if restriction != "block":
            assert f"space '{restricted.space.name}'" in str(error.value)
        for item in (feature, restricted, label):
            item.refresh_from_db()
        assert feature.branch_id == source.id
        assert label.branch_id == source.id
        if restriction != "branch":
            assert restricted.branch_id == source.id
        source.refresh_from_db()
        assert source.status == "standalone"

        # An individual grant can allow a record even without space write access.
        with connection.cursor() as cursor:
            cursor.execute("RESET ROLE")
            if restriction == "locked":
                ln.Feature.objects.filter(pk=feature.pk).update(is_locked=False)
            else:
                cursor.execute(
                    f"CREATE POLICY merge_preflight_grant ON {table} FOR UPDATE TO {role} "
                    f"USING ({grant_condition}) WITH CHECK (true)"
                )
            cursor.execute(f"SET ROLE {role}")
        ln.setup.merge(source, target=target)
        feature.refresh_from_db()
        label.refresh_from_db()
        source.refresh_from_db()
        assert feature.branch_id == target.id
        assert label.branch_id == target.id
        assert source.status == "merged"
        if restriction != "branch":
            restricted.refresh_from_db()
            assert restricted.branch_id == target.id
    finally:
        with connection.cursor() as cursor:
            cursor.execute("RESET ROLE")
            for policy in ("read", "write", "grant"):
                cursor.execute(
                    f"DROP POLICY IF EXISTS merge_preflight_{policy} ON {table}"
                )
            cursor.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
            cursor.execute(f"DROP OWNED BY {role}")
            cursor.execute(f"DROP ROLE {role}")
        if restriction == "block":
            record.delete(permanent=True)
        if restriction == "locked":
            ln.Feature.objects.filter(pk=feature.pk).update(is_locked=False)
            feature.refresh_from_db()


@pytest.mark.parametrize("registry", ["Feature", "RecordBlock"])
@pytest.mark.parametrize("hidden_branch", ["source", "target"])
def test_merge_handles_hidden_records(registry, hidden_branch, request):
    from django.db import IntegrityError, connection

    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL row-level security is required")
    source, target, feature, label = request.getfixturevalue("merge_records")
    branch = source if hidden_branch == "source" else target
    if registry == "Feature":
        restricted = feature
        restricted.branch = branch
        restricted.save()
    else:
        record = ln.Record(name="merge-hidden-block", branch=branch).save()
        restricted = ln.models.RecordBlock(
            record=record, content="hidden", kind="readme", branch=branch
        ).save()
    table = connection.ops.quote_name(restricted._meta.db_table)
    role = connection.ops.quote_name(f"merge_hidden_{source.uid}")
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE ROLE {role} NOLOGIN")
        cursor.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
        cursor.execute(f"GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA public TO {role}")
        cursor.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        cursor.execute(
            f"CREATE POLICY merge_hidden_read ON {table} FOR SELECT TO {role} "
            f"USING (id <> {restricted.pk})"
        )
        cursor.execute(
            f"CREATE POLICY merge_hidden_write ON {table} FOR UPDATE TO {role} "
            "USING (true) WITH CHECK (true)"
        )
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SET ROLE {role}")
        assert not type(restricted).objects.filter(pk=restricted.pk).exists()
        if hidden_branch == "source":
            with pytest.raises(IntegrityError, match="source records remain"):
                ln.setup.merge(source, target=target)
        else:
            ln.setup.merge(source, target=target)
        with connection.cursor() as cursor:
            cursor.execute("RESET ROLE")
        source.refresh_from_db()
        assert source.status == (
            "standalone" if hidden_branch == "source" else "merged"
        )
        for item in (feature, label, restricted):
            item.refresh_from_db()
            assert item.branch_id == branch.id
    finally:
        with connection.cursor() as cursor:
            cursor.execute("RESET ROLE")
            cursor.execute(f"DROP POLICY merge_hidden_read ON {table}")
            cursor.execute(f"DROP POLICY merge_hidden_write ON {table}")
            cursor.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
            cursor.execute(f"DROP OWNED BY {role}")
            cursor.execute(f"DROP ROLE {role}")
        if registry == "RecordBlock":
            record.delete(permanent=True)


def test_merge_preflight_requires_hidden_record_guard(request):
    from django.db import connection

    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL row-level security is required")
    source, target, feature, label = request.getfixturevalue("merge_records")
    role = connection.ops.quote_name(f"merge_unguarded_{source.uid}")
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE ROLE {role} NOLOGIN")
        cursor.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
        cursor.execute(f"GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA public TO {role}")
        cursor.execute("ALTER TABLE lamindb_feature ENABLE ROW LEVEL SECURITY")
        cursor.execute(
            "ALTER TABLE lamindb_branch DISABLE TRIGGER lamindb_require_empty_merged_branch"
        )
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SET ROLE {role}")
        with pytest.raises(PermissionError, match="migrate the instance"):
            ln.setup.merge(source, target=target)
    finally:
        with connection.cursor() as cursor:
            cursor.execute("RESET ROLE")
            cursor.execute("ALTER TABLE lamindb_feature DISABLE ROW LEVEL SECURITY")
            cursor.execute(
                "ALTER TABLE lamindb_branch ENABLE TRIGGER lamindb_require_empty_merged_branch"
            )
            cursor.execute(f"DROP OWNED BY {role}")
            cursor.execute(f"DROP ROLE {role}")
    source.refresh_from_db()
    feature.refresh_from_db()
    label.refresh_from_db()
    assert source.status == "standalone"
    assert feature.branch_id == source.id
    assert label.branch_id == source.id


@pytest.mark.parametrize("failure_stage", ["reconciliation", "branch_status"])
def test_merge_rolls_back_finalization_errors(monkeypatch, failure_stage):
    from lamindb.models import _is_versioned

    source = ln.Branch(name=f"merge-finalization-{failure_stage}").save()
    target = ln.Branch.get(name="main")
    original = ln.Transform(
        key=f"merge-finalization-{failure_stage}", kind="pipeline", branch=target
    ).save()
    revision = ln.Transform(revises=original, kind="pipeline", branch=source).save()
    reconcile = _is_versioned.reconcile_is_latest_within_branch
    save = ln.Branch.save

    def fail_reconciliation(model, **kwargs):
        result = reconcile(model, **kwargs)
        if model is ln.Transform:
            raise RuntimeError("merge finalization failed")
        return result

    def fail_status_save(self, *args, **kwargs):
        result = save(self, *args, **kwargs)
        if self.pk == source.pk:
            raise RuntimeError("merge finalization failed")
        return result

    try:
        with monkeypatch.context() as patch:
            if failure_stage == "reconciliation":
                patch.setattr(
                    _is_versioned,
                    "reconcile_is_latest_within_branch",
                    fail_reconciliation,
                )
            else:
                patch.setattr(ln.Branch, "save", fail_status_save)
            with pytest.raises(RuntimeError, match="merge finalization failed"):
                ln.setup.merge(source, target=target)
        source.refresh_from_db()
        original.refresh_from_db()
        revision.refresh_from_db()
        assert source.status == "standalone"
        assert original.is_latest
        assert revision.is_latest
        assert revision.branch_id == source.id
    finally:
        revision.delete(permanent=True)
        original.delete(permanent=True)
        source.delete(permanent=True)


def test_merge_respects_callers_transaction(merge_records):
    from django.db import transaction

    source, target, feature, label = merge_records
    with pytest.raises(RuntimeError, match="outer rollback"):
        with transaction.atomic():
            ln.setup.merge(source, target=target)
            raise RuntimeError("outer rollback")
    source.refresh_from_db()
    for item in (feature, label):
        item.refresh_from_db()
        assert item.branch_id == source.id
    assert source.status == "standalone"


@pytest.mark.parametrize("restricted_model", ["Feature", "RecordBlock"])
def test_merge_rolls_back_silently_skipped_updates(restricted_model, merge_records):
    from django.db import connection

    source, target, feature, label = merge_records
    if restricted_model == "Feature":
        restricted = feature
    else:
        record = ln.Record(name="merge-skipped-block", branch=source).save()
        restricted = ln.models.RecordBlock(
            record=record, content="restricted", kind="readme", branch=source
        ).save()
    table = connection.ops.quote_name(restricted._meta.db_table)
    # Like an RLS UPDATE policy, the trigger silently skips a readable record.
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute(
                "CREATE FUNCTION test_merge_skip() RETURNS trigger LANGUAGE plpgsql AS $$ "
                f"BEGIN IF OLD.id = {restricted.pk} THEN RETURN NULL; END IF; "
                "RETURN NEW; END; $$"
            )
            cursor.execute(
                f"CREATE TRIGGER test_merge_skip BEFORE UPDATE OF branch_id ON {table} "
                "FOR EACH ROW EXECUTE FUNCTION test_merge_skip()"
            )
        else:
            cursor.execute(
                f"CREATE TEMP TRIGGER test_merge_skip BEFORE UPDATE OF branch_id ON {table} "
                f"WHEN OLD.id = {restricted.pk} BEGIN SELECT RAISE(IGNORE); END"
            )
    try:
        with pytest.raises(PermissionError, match="write access"):
            ln.setup.merge(source, target=target)
        for item in (feature, restricted, label):
            item.refresh_from_db()
            assert item.branch_id == source.id
        source.refresh_from_db()
        assert source.status == "standalone"
    finally:
        with connection.cursor() as cursor:
            cursor.execute(
                f"DROP TRIGGER test_merge_skip ON {table}"
                if connection.vendor == "postgresql"
                else "DROP TRIGGER test_merge_skip"
            )
            if connection.vendor == "postgresql":
                cursor.execute("DROP FUNCTION test_merge_skip()")
        if restricted_model == "RecordBlock":
            record.delete(permanent=True)


def test_merge_branch_into_main():
    """Merge a branch into main: create branch, add ULabel, switch to main, merge."""
    branch = ln.Branch(name="test_merge_branch").save()
    annotation = ln.models.BranchBlock(
        branch=branch, content="Keep this README", kind="readme"
    ).save()
    assert branch.status == "standalone"
    ln.setup.switch(branch.name)
    assert ln.setup.settings.branch == branch
    assert ln.setup.settings.branch.name == "test_merge_branch"

    ulabel = ln.ULabel(name="test_merge_record").save()
    assert ulabel.branch == branch
    assert ulabel.created_on == branch  # created_on set to creation branch

    ln.setup.switch("main")
    assert ln.setup.settings.branch.name == "main"
    assert ln.setup.settings.branch.status == "standalone"
    assert ln.ULabel.filter(name="test_merge_record").count() == 0

    ln.setup.merge("test_merge_branch")
    assert ln.ULabel.filter(name="test_merge_record").count() == 1
    ulabel = ln.ULabel.get(name="test_merge_record")
    assert ulabel.branch.name == "main"
    # created_on still points to the branch on which the record was created
    assert ulabel.created_on == branch
    assert ulabel.created_on.name == "test_merge_branch"
    # merged branch has status "merged"
    branch.refresh_from_db()
    assert branch.status == "merged"
    annotation.refresh_from_db()
    assert annotation.branch_id == branch.id
    # this is a merge call to check that branch.describe() works because it
    # has a custom describe method
    branch.describe(return_str=True)

    # Clean up
    ulabel.delete(permanent=True)
    branch.delete(permanent=True)
    ln.setup.switch("main")


def test_merge_without_switch_via_explicit_target():
    main_branch = ln.Branch.get(name="main")
    source_branch = ln.Branch(name="test_merge_explicit_target_source").save()
    ln.setup.switch(source_branch.name)
    ulabel = ln.ULabel(name="test_merge_explicit_target_record").save()
    assert ln.setup.settings.branch.name == source_branch.name
    assert ulabel.branch == source_branch

    ln.setup.merge(source_branch.name, target=main_branch.name)

    ulabel.refresh_from_db()
    assert ln.setup.settings.branch.name == source_branch.name
    assert ulabel.branch.name == main_branch.name
    assert ulabel.created_on == source_branch

    ulabel.delete(permanent=True)
    source_branch.delete(permanent=True)
    ln.setup.switch(main_branch.name)


def test_merge_with_branch_objects():
    main_branch = ln.Branch.get(name="main")
    source_branch = ln.Branch(name="test_merge_target_object_source").save()
    ln.setup.switch(source_branch.name)
    ulabel = ln.ULabel(name="test_merge_target_object_record").save()
    assert ulabel.branch == source_branch

    ln.setup.merge(source_branch, target=main_branch)

    ulabel.refresh_from_db()
    assert ulabel.branch == main_branch
    assert ulabel.created_on == source_branch

    ulabel.delete(permanent=True)
    source_branch.delete(permanent=True)
    ln.setup.switch(main_branch.name)


def test_branch_status_values():
    """Branch status maps codes onto standalone/draft/review/merged/closed."""
    main_branch = ln.Branch.get(name="main")
    assert main_branch.status == "standalone"
    archive_branch = ln.Branch.get(name="archive")
    assert archive_branch.status == "standalone"
    trash_branch = ln.Branch.get(name="trash")
    assert trash_branch.status == "standalone"
    # User-created branch is standalone by default.
    branch = ln.Branch(name="test_status_branch").save()
    assert branch.status == "standalone"
    branch.status = "draft"
    branch.save()
    branch.refresh_from_db()
    assert branch.status == "draft"
    branch.status = "review"
    branch.save()
    branch.refresh_from_db()
    assert branch.status == "review"
    branch.status = "closed"
    branch.save()
    branch.refresh_from_db()
    assert branch.status == "closed"
    branch.delete(permanent=True)


def test_draft_review_and_close_merge_request_status():
    branch = ln.Branch(name="test_mr_draft_review_close").save()
    assert branch.status == "standalone"

    branch.status = "draft"
    branch.save()
    branch.refresh_from_db()
    assert branch.status == "draft"

    branch.status = "review"
    branch.save()
    branch.refresh_from_db()
    assert branch.status == "review"

    branch.status = "closed"
    branch.save()
    branch.refresh_from_db()
    assert branch.status == "closed"

    branch.delete(permanent=True)


def test_merge_nonexistent_branch_raises():
    """Merge a non-existent branch raises ObjectDoesNotExist."""
    with pytest.raises(ln.errors.ObjectDoesNotExist) as exc_info:
        ln.setup.merge("nonexistent_branch_xyz")
    assert "not found" in str(exc_info.value).lower()


def test_merge_nonexistent_target_branch_raises():
    source_branch = ln.Branch(name="test_merge_target_not_found_source").save()
    with pytest.raises(ln.errors.ObjectDoesNotExist) as exc_info:
        ln.setup.merge(source_branch.name, target="nonexistent_target_branch_xyz")
    assert "not found" in str(exc_info.value).lower()
    source_branch.delete(permanent=True)


def test_merge_reconciles_is_latest_for_versioned_records():
    main_branch = ln.Branch.get(name="main")
    ln.setup.switch(main_branch.name)

    transform_v1 = ln.Transform(
        key="test-merge-is-latest",
        source_code="main-v1",
        kind="pipeline",
    ).save()
    branch = ln.Branch(name="test_merge_latest_branch").save()
    ln.setup.switch(branch.name)
    transform_v2 = ln.Transform(
        key="test-merge-is-latest",
        revises=transform_v1,
        source_code="feature-v2",
        kind="pipeline",
    ).save()
    transform_v1.refresh_from_db()
    assert transform_v1.is_latest
    assert transform_v2.is_latest

    ln.setup.switch(main_branch.name)
    ln.setup.merge(branch.name)

    family = ln.Transform.objects.filter(
        uid__startswith=transform_v1.uid[:-4], branch_id=1
    )
    assert family.filter(is_latest=True).count() == 1
    assert family.get(is_latest=True).uid == transform_v2.uid

    for record in family:
        record.delete(permanent=True)
    branch.delete(permanent=True)


def test_merge_updates_recordblock_branch():
    main_branch = ln.Branch.get(name="main")
    ln.setup.switch(main_branch.name)

    source_branch = ln.Branch(name="test_merge_recordblock_branch").save()
    ln.setup.switch(source_branch.name)
    record = ln.Record(name="recordblock-merge-record").save()
    block = ln.models.RecordBlock(
        record=record,
        content="recordblock merge content",
        kind="readme",
        branch=source_branch,
        created_on=source_branch,
    ).save()
    assert block.branch == source_branch
    assert block.created_on == source_branch

    ln.setup.switch(main_branch.name)
    ln.setup.merge(source_branch.name)

    block.refresh_from_db()
    assert block.branch.name == "main"
    assert block.created_on == source_branch

    record.delete(permanent=True)
    source_branch.delete(permanent=True)


def test_merge_with_identical_source_and_target_is_noop():
    source_branch = ln.Branch(name="test_merge_explicit_self_target").save()
    ln.setup.switch(source_branch.name)
    ulabel = ln.ULabel(name="test_merge_explicit_self_target_record").save()

    ln.setup.merge(source_branch.name, target=source_branch.name)

    ulabel.refresh_from_db()
    source_branch.refresh_from_db()
    assert ulabel.branch == source_branch
    assert ulabel.created_on == source_branch
    assert source_branch.status == "standalone"

    ulabel.delete(permanent=True)
    source_branch.delete(permanent=True)
    ln.setup.switch("main")
