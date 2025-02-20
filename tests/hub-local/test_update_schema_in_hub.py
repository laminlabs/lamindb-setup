import lamindb_setup as ln_setup
import pytest
from lamindb_setup._schema_metadata import _dict_to_uuid, update_schema_in_hub


@pytest.fixture
def setup_instance():
    ln_setup.init(
        storage="./test_storage",
        modules="bionty,wetlab",
        db="postgresql://postgres:postgres@127.0.0.1:54322/postgres",
        name="test-update-modules",
    )
    ln_setup.register()
    yield
    ln_setup.delete("test-update-modules", force=True)


def test_update_schema_in_hub(setup_instance):
    is_new, schema_uuid, schema = update_schema_in_hub()

    # TODO: construct a test in which is_new is true
    # currently it's false because the modules is already registered
    # via `lamin register` earlier in the test
    assert not is_new
    assert _dict_to_uuid(schema["schema_json"]) == schema_uuid

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

    assert len(schema["schema_json"].keys()) == 3
    assert "core" in schema["schema_json"]
    assert "bionty" in schema["schema_json"]
    assert "wetlab" in schema["schema_json"]

    assert not schema["schema_json"]["core"]["artifact"]["is_link_table"]
    assert schema["schema_json"]["core"]["artifactulabel"]["is_link_table"]

    assert schema["schema_json"]["core"]["artifact"]["fields"]["id"] == {
        "type": "AutoField",
        "column_name": "id",
        "through": None,
        "field_name": "id",
        "model_name": "artifact",
        "schema_name": "core",
        "is_link_table": False,
        "is_primary_key": True,
        "is_editable": True,  # Primary key are read-only even if is_editable is True
        "relation_type": None,
        "related_field_name": None,
        "related_model_name": None,
        "related_schema_name": None,
    }

    assert schema["schema_json"]["core"]["artifact"]["fields"]["otype"] == {
        "type": "CharField",
        "column_name": "otype",
        "through": None,
        "field_name": "otype",
        "model_name": "artifact",
        "schema_name": "core",
        "is_link_table": False,
        "is_primary_key": False,
        "is_editable": False,
        "relation_type": None,
        "related_field_name": None,
        "related_model_name": None,
        "related_schema_name": None,
    }

    assert schema["schema_json"]["core"]["artifact"]["fields"]["created_by"] == {
        "type": "ForeignKey",
        "column_name": "created_by_id",
        "through": {
            "left_key": "created_by_id",
            "right_key": "id",
            "link_table_name": None,
        },
        "field_name": "created_by",
        "model_name": "artifact",
        "schema_name": "core",
        "is_link_table": False,
        "is_primary_key": False,
        "is_editable": False,
        "relation_type": "many-to-one",
        "related_field_name": "created_artifacts",
        "related_model_name": "user",
        "related_schema_name": "core",
    }

    assert schema["schema_json"]["bionty"]["gene"]["fields"]["pathways"] == {
        "type": "ManyToManyField",
        "column_name": None,
        "through": {
            "left_key": "gene_id",
            "right_key": "pathway_id",
            "link_table_name": "bionty_pathway_genes",
        },
        "field_name": "pathways",
        "model_name": "gene",
        "schema_name": "bionty",
        "is_link_table": False,
        "is_primary_key": False,
        "is_editable": False,
        "relation_type": "many-to-many",
        "related_field_name": "genes",
        "related_model_name": "pathway",
        "related_schema_name": "bionty",
    }

    assert schema["schema_json"]["wetlab"]["well"]["fields"]["artifacts"] == {
        "type": "ManyToManyField",
        "column_name": None,
        "through": {
            "left_key": "well_id",
            "right_key": "artifact_id",
            "link_table_name": "wetlab_artifactwell",
        },
        "field_name": "artifacts",
        "model_name": "well",
        "schema_name": "wetlab",
        "is_link_table": False,
        "is_primary_key": False,
        "is_editable": False,
        "relation_type": "many-to-many",
        "related_field_name": "wells",
        "related_model_name": "artifact",
        "related_schema_name": "core",
    }

    assert schema["schema_json"]["core"]["transform"]["fields"]["predecessors"] == {
        "type": "ManyToManyField",
        "column_name": None,
        "through": {
            "left_key": "from_transform_id",
            "right_key": "to_transform_id",
            "link_table_name": "lamindb_transform_predecessors",
        },
        "field_name": "predecessors",
        "model_name": "transform",
        "schema_name": "core",
        "is_link_table": False,
        "is_primary_key": False,
        "is_editable": False,
        "relation_type": "many-to-many",
        "related_field_name": "successors",
        "related_model_name": "transform",
        "related_schema_name": "core",
    }

    assert schema["schema_json"]["core"]["transform"]["fields"]["successors"] == {
        "type": "ManyToManyField",
        "column_name": None,
        "through": {
            "left_key": "to_transform_id",
            "right_key": "from_transform_id",
            "link_table_name": "lamindb_transform_predecessors",
        },
        "field_name": "successors",
        "model_name": "transform",
        "schema_name": "core",
        "is_link_table": False,
        "is_primary_key": False,
        "is_editable": False,
        "relation_type": "many-to-many",
        "related_field_name": "predecessors",
        "related_model_name": "transform",
        "related_schema_name": "core",
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
