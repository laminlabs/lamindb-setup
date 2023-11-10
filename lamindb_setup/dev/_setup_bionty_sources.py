from ._settings_instance import InstanceSettings
from django.db.utils import OperationalError, ProgrammingError


def write_bionty_sources(isettings: InstanceSettings) -> None:
    """Write bionty sources to BiontySource table."""
    if "bionty" not in isettings.schema:
        return None
    import shutil
    from bionty.dev._handle_sources import parse_sources_yaml
    from bionty import settings as bt_settings
    import bionty as bt
    from lnschema_bionty.models import BiontySource

    shutil.copy(bt_settings.current_sources, bt_settings.lamindb_sources)

    all_sources = parse_sources_yaml(bt_settings.local_sources)
    all_sources_dict = all_sources.to_dict(orient="records")

    def _get_currently_used(key: str):
        return (
            bt.display_currently_used_sources().reset_index().set_index(["entity", key])
        )

    try:
        currently_used = _get_currently_used("organism")
        key = "organism"
    except KeyError:
        currently_used = _get_currently_used("species")
        key = "species"

    all_records = []
    for kwargs in all_sources_dict:
        act = currently_used.loc[(kwargs["entity"], kwargs[key])].to_dict()
        if (act["source"] == kwargs["source"]) and (
            act["version"] == kwargs["version"]
        ):
            kwargs["currently_used"] = True

        # when the database is not yet migrated but setup is updated
        # won't need this once lamindb is released with the new pin
        if hasattr(BiontySource, "species") and "organism" in kwargs:
            kwargs["species"] = kwargs.pop("organism")
        elif hasattr(BiontySource, "organism") and "species" in kwargs:
            kwargs["organism"] = kwargs.pop("species")
        record = BiontySource(**kwargs)
        all_records.append(record)

    BiontySource.objects.bulk_create(all_records, ignore_conflicts=True)


def load_bionty_sources(isettings: InstanceSettings):
    """Write currently_used bionty sources to LAMINDB_VERSIONS_PATH in bionty."""
    if "bionty" not in isettings.schema:
        return None

    from bionty.dev._handle_sources import parse_currently_used_sources
    from bionty import settings as bt_settings
    from bionty.dev._io import write_yaml
    from lnschema_bionty.models import BiontySource

    try:
        # need try except because of integer primary key migration
        active_records = BiontySource.objects.filter(currently_used=True).all().values()
        write_yaml(
            parse_currently_used_sources(active_records), bt_settings.lamindb_sources
        )
    except (OperationalError, ProgrammingError):
        pass


def delete_bionty_sources_yaml():
    """Delete LAMINDB_SOURCES_PATH in bionty."""
    try:
        from bionty import settings as bt_settings

        bt_settings.lamindb_sources.unlink(missing_ok=True)
    except ModuleNotFoundError:
        pass
