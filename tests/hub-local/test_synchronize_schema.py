import lamindb_setup as ln_setup
import pytest
from lamindb_setup._schema_metadata import synchronize_schema


@pytest.fixture
def setup_instance():
    ln_setup.init(storage="./testdb", schema="bionty,wetlab")
    yield
    ln_setup.delete("testdb", force=True)


def test_synchronize_new_schema(setup_instance):
    is_new, schema = synchronize_schema()

    assert is_new is True

    assert len(schema["module_set_info"]) == 1
    assert schema["module_set_info"][0]["id"] == 0
    assert schema["module_set_info"][0]["name"] == "core"

    assert len(schema["json"].keys()) == 1
    assert "core" in schema["json"]

    assert schema["json"]["core"]["artifact"]["fields"]["id"] == {
        "type": "AutoField",
        "column": "id",
        "field_name": "id",
        "model_name": "artifact",
        "schema_name": "core",
        "select_term": '"lnschema_core_artifact"."id"',
        "is_link_table": False,
        "relation_type": None,
        "related_field_name": None,
        "related_model_name": None,
        "related_schema_name": None,
    }

    assert schema["json"]["core"]["artifact"]["fields"]["created_by"] == {
        "type": "ForeignKey",
        "column": "created_by_id",
        "field_name": "created_by",
        "model_name": "artifact",
        "schema_name": "core",
        "select_term": '(SELECT JSON_OBJECT(\'id\', U0."id", \'uid\', U0."uid", \'handle\', U0."handle", \'name\', U0."name", \'created_at\', U0."created_at", \'updated_at\', U0."updated_at") AS "data" FROM "lnschema_core_user" U0 INNER JOIN "lnschema_core_artifact" U1 ON (U0."id" = U1."created_by_id") WHERE U1."id" = ("lnschema_core_artifact"."id") LIMIT 5) AS "created_by"',
        "is_link_table": False,
        "relation_type": "many-to-one",
        "related_field_name": "artifact",
        "related_model_name": "user",
        "related_schema_name": "core",
    }
