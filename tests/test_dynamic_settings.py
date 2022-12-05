from lndb_setup import init, settings
from lndb_setup._settings_store import InstanceSettingsStore


def test_dynamic_settings():
    init(storage="mydata_2", dbconfig="sqlite", schema="bionty")

    settings_store_mydata_2 = InstanceSettingsStore(
        storage_root=str(settings.instance.storage_root),
        storage_region=str(settings.instance.storage_region),
        schema_=settings.instance._schema,
        dbconfig_=settings.instance._dbconfig,
    )

    init(storage="mydata_3", dbconfig="sqlite")

    assert settings.instance.name == "mydata_3"
    assert settings._instance(settings_store_mydata_2).name == "mydata_2"
    assert settings._instance().name == "mydata_3"
