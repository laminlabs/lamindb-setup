from django.db import transaction
from lamin_logger import logger

from ._settings_instance import InstanceSettings


def write_bionty_sources(isettings: InstanceSettings):
    """Write bionty sources to BiontySource table."""
    if "bionty" in isettings.schema:
        import shutil

        from bionty.dev._handle_sources import (
            CURRENT_SOURCES_PATH,
            LAMINDB_SOURCES_PATH,
        )

        shutil.copy(CURRENT_SOURCES_PATH, LAMINDB_SOURCES_PATH)

        import bionty as bt
        from lnschema_bionty.models import BiontySource

        all_sources = bt.display_available_sources().reset_index()
        all_sources_dict = all_sources.to_dict(orient="records")

        currently_used = (
            bt.display_currently_used_sources()
            .reset_index()
            .set_index(["entity", "species"])
        )

        all_records = []
        for kwargs in all_sources_dict:
            act = currently_used.loc[(kwargs["entity"], kwargs["species"])].to_dict()
            if (act["source_key"] == kwargs["source_key"]) and (
                act["version"] == kwargs["version"]
            ):
                kwargs["currently_used"] = True

            record = BiontySource(**kwargs)
            all_records.append(record)

        with transaction.atomic():
            for record in all_records:
                record.save()


def load_bionty_sources(isettings: InstanceSettings):
    """Write currently_used bionty sources to LAMINDB_VERSIONS_PATH in bionty."""
    if "bionty" in isettings.schema:
        from bionty.dev._handle_sources import (
            LAMINDB_SOURCES_PATH,
            parse_currently_used_sources,
        )
        from bionty.dev._io import write_yaml
        from lnschema_bionty.models import BiontySource

        active_records = BiontySource.objects.filter(currently_used=True).all().values()

        write_yaml(parse_currently_used_sources(active_records), LAMINDB_SOURCES_PATH)
        logger.hint("Configured default bionty sources from BiontySource table")


def delete_bionty_sources_yaml():
    """Delete LAMINDB_SOURCES_PATH in bionty."""
    from bionty.dev._handle_sources import LAMINDB_SOURCES_PATH

    LAMINDB_SOURCES_PATH.unlink(missing_ok=True)
