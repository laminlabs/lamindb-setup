from django.db import transaction
from lamin_logger import logger

from ._settings_instance import InstanceSettings


def write_bionty_sources(isettings: InstanceSettings):
    """Write bionty sources to BiontySource table."""
    if "bionty" in isettings.schema:
        import bionty as bt
        from lnschema_bionty.models import BiontySource

        all_versions = bt.display_available_sources().reset_index()
        all_versions_dict = all_versions.to_dict(orient="records")

        active_versions = (
            bt.display_currently_used_sources()
            .reset_index()
            .set_index(["entity", "species"])
        )

        all_records = []
        for kwargs in all_versions_dict:
            act = active_versions.loc[(kwargs["entity"], kwargs["species"])].to_dict()
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
        from bionty.dev._handle_versions import (
            LAMINDB_SOURCES_PATH,
            parse_currently_used_sources,
        )
        from bionty.dev._io import write_yaml
        from lnschema_bionty.models import BiontySource

        active_records = BiontySource.objects.filter(currently_used=True).all().values()

        write_yaml(parse_currently_used_sources(active_records), LAMINDB_SOURCES_PATH)
        logger.hint("Configured default bionty sources from BiontySource table")
