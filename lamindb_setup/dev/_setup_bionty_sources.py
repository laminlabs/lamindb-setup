from ._settings_instance import InstanceSettings


def write_bionty_sources(isettings: InstanceSettings) -> None:
    """Write bionty sources to BiontySource table."""
    if "bionty" not in isettings.schema:
        return None
    import shutil
    from bionty.dev._handle_sources import (
        CURRENT_SOURCES,
        LAMINDB_SOURCES,
        LOCAL_SOURCES,
        parse_sources_yaml,
    )

    shutil.copy(CURRENT_SOURCES, LAMINDB_SOURCES)

    import bionty as bt
    from lnschema_bionty.models import BiontySource

    all_sources = parse_sources_yaml(LOCAL_SOURCES)
    all_sources_dict = all_sources.to_dict(orient="records")

    currently_used = (
        bt.display_currently_used_sources()
        .reset_index()
        .set_index(["entity", "species"])
    )

    all_records = []
    for kwargs in all_sources_dict:
        act = currently_used.loc[(kwargs["entity"], kwargs["species"])].to_dict()
        if (act["source"] == kwargs["source"]) and (
            act["version"] == kwargs["version"]
        ):
            kwargs["currently_used"] = True

        record = BiontySource(**kwargs)
        all_records.append(record)

    BiontySource.objects.bulk_create(all_records, ignore_conflicts=True)


def load_bionty_sources(isettings: InstanceSettings):
    """Write currently_used bionty sources to LAMINDB_VERSIONS_PATH in bionty."""
    if "bionty" not in isettings.schema:
        return None
    from bionty.dev._handle_sources import (
        LAMINDB_SOURCES,
        parse_currently_used_sources,
    )
    from bionty.dev._io import write_yaml
    from lnschema_bionty.models import BiontySource

    active_records = BiontySource.objects.filter(currently_used=True).all().values()
    write_yaml(parse_currently_used_sources(active_records), LAMINDB_SOURCES)


def delete_bionty_sources_yaml():
    """Delete LAMINDB_SOURCES_PATH in bionty."""
    try:
        from bionty.dev._handle_sources import LAMINDB_SOURCES

        LAMINDB_SOURCES.unlink(missing_ok=True)
    except ModuleNotFoundError:
        pass
