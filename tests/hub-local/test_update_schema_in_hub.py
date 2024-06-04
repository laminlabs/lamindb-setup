import lamindb_setup as ln_setup
import pytest
from lamindb_setup._schema_metadata import _dict_to_uuid, update_schema_in_hub


@pytest.fixture
def setup_instance():
    ln_setup.init(
        storage="./test_storage",
        schema="bionty,wetlab",
        db="postgresql://postgres:postgres@127.0.0.1:54322/postgres",
        name="testdb",
    )
    ln_setup.register()
    yield
    ln_setup.delete("testdb", force=True)


def test_update_schema_in_hub(setup_instance):
    is_new, schema_uuid, schema = update_schema_in_hub()

    assert is_new is True
    assert _dict_to_uuid(schema["json"]) == schema_uuid

    assert len(schema["module_set_info"]) == 3
    module_set_info = sorted(
        schema["module_set_info"], key=lambda module: module["name"]
    )
    assert module_set_info[0]["id"] == 0
    assert module_set_info[0]["name"] == "bionty"
    assert module_set_info[1]["id"] == 0
    assert module_set_info[1]["name"] == "core"
    assert module_set_info[2]["id"] == 0
    assert module_set_info[2]["name"] == "wetlab"

    assert len(schema["json"].keys()) == 3
    assert "core" in schema["json"]
    assert "bionty" in schema["json"]
    assert "wetlab" in schema["json"]

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
        "through": None,
    }

    assert schema["json"]["core"]["artifact"]["fields"]["created_by"] == {
        "type": "ForeignKey",
        "column": "created_by_id",
        "field_name": "created_by",
        "model_name": "artifact",
        "schema_name": "core",
        "select_term": '(SELECT JSONB_BUILD_OBJECT((\'id\')::text, U0."id", (\'uid\')::text, U0."uid", (\'handle\')::text, U0."handle", (\'name\')::text, U0."name", (\'created_at\')::text, U0."created_at", (\'updated_at\')::text, U0."updated_at") AS "data" FROM "lnschema_core_user" U0 INNER JOIN "lnschema_core_artifact" U1 ON (U0."id" = U1."created_by_id") WHERE U1."id" = ("lnschema_core_artifact"."id") LIMIT 5) AS "created_by"',
        "is_link_table": False,
        "relation_type": "many-to-one",
        "related_field_name": "artifact",
        "related_model_name": "user",
        "related_schema_name": "core",
        "through": None,
    }

    assert schema["json"]["bionty"]["gene"]["fields"]["pathways"] == {
        "type": "ManyToManyField",
        "column": None,
        "field_name": "pathways",
        "model_name": "gene",
        "schema_name": "bionty",
        "select_term": 'ARRAY(SELECT JSONB_BUILD_OBJECT((\'created_at\')::text, U0."created_at", (\'updated_at\')::text, U0."updated_at", (\'id\')::text, U0."id", (\'uid\')::text, U0."uid", (\'name\')::text, U0."name", (\'ontology_id\')::text, U0."ontology_id", (\'abbr\')::text, U0."abbr", (\'synonyms\')::text, U0."synonyms", (\'description\')::text, U0."description") AS "data" FROM "lnschema_bionty_pathway" U0 INNER JOIN "lnschema_bionty_pathway_genes" U1 ON (U0."id" = U1."pathway_id") WHERE U1."gene_id" = ("lnschema_bionty_gene"."id") LIMIT 5) AS "pathways"',
        "is_link_table": False,
        "relation_type": "many-to-many",
        "related_field_name": "genes",
        "related_model_name": "pathway",
        "related_schema_name": "bionty",
        "through": {
            "link_table_name": "lnschema_bionty_pathway_genes",
            "lnschema_bionty_gene": "pathway_id",
            "lnschema_bionty_pathway": "gene_id",
        },
    }

    assert schema["json"]["wetlab"]["well"]["fields"]["artifacts"] == {
        "type": "ManyToManyField",
        "column": None,
        "field_name": "artifacts",
        "model_name": "well",
        "schema_name": "wetlab",
        "select_term": 'ARRAY(SELECT JSONB_BUILD_OBJECT((\'version\')::text, U0."version", (\'created_at\')::text, U0."created_at", (\'updated_at\')::text, U0."updated_at", (\'id\')::text, U0."id", (\'uid\')::text, U0."uid", (\'description\')::text, U0."description", (\'key\')::text, U0."key", (\'suffix\')::text, U0."suffix", (\'accessor\')::text, U0."accessor", (\'size\')::text, U0."size", (\'hash\')::text, U0."hash", (\'hash_type\')::text, U0."hash_type", (\'n_objects\')::text, U0."n_objects", (\'n_observations\')::text, U0."n_observations", (\'visibility\')::text, U0."visibility", (\'key_is_virtual\')::text, U0."key_is_virtual") AS "data" FROM "lnschema_core_artifact" U0 INNER JOIN "wetlab_well_artifacts" U1 ON (U0."id" = U1."artifact_id") WHERE U1."well_id" = ("wetlab_well"."id") LIMIT 5) AS "artifacts"',
        "is_link_table": False,
        "relation_type": "many-to-many",
        "related_field_name": "wells",
        "related_model_name": "artifact",
        "related_schema_name": "core",
        "through": {
            "wetlab_well": "well_id",
            "link_table_name": "wetlab_well_artifacts",
            "lnschema_core_artifact": "artifact_id",
        },
    }


def test_dict_to_uuid():
    dict_1 = {
        "a": 1,
        "b": 1,
        "c": {
            "d": 1,
            "e": {"f": 1, "g": 1},
        },
    }

    dict_2 = {
        "c": {
            "e": {
                "g": 1,
                "f": 1,
            },
            "d": 1,
        },
        "b": 1,
        "a": 1,
    }

    dict_3 = {
        "a": 1,
        "b": 1,
        "c": {
            "d": 1,
            "e": {"f": 1, "g": 2},
        },
    }

    assert _dict_to_uuid(dict_1) == _dict_to_uuid(dict_2)
    assert _dict_to_uuid(dict_1) != _dict_to_uuid(dict_3)
