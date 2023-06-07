from django.db import transaction
from lamin_logger import logger

from ._settings_instance import InstanceSettings


def write_bionty_versions(isettings: InstanceSettings):
    """Write bionty versions to BiontyVersions and CurrentBiontyVersions tables."""
    if "bionty" in isettings.schema:
        import bionty as bt
        from lnschema_bionty.models import BiontyVersions, CurrentBiontyVersions

        all_versions = bt.display_available_versions().reset_index()
        all_versions_dict = all_versions.to_dict(orient="records")

        current_versions = (
            bt.display_active_versions().reset_index().set_index(["entity", "species"])
        )
        all_records = []
        current_records = []

        for kwargs in all_versions_dict:
            record = BiontyVersions(**kwargs)
            all_records.append(record)
            current = current_versions.loc[
                (kwargs["entity"], kwargs["species"])
            ].to_dict()
            if (current["source_key"] == kwargs["source_key"]) and (
                current["version"] == kwargs["version"]
            ):
                current_records.append(CurrentBiontyVersions(bionty_version=record))

        with transaction.atomic():
            for record in all_records + current_records:
                record.save()


def load_bionty_versions(isettings: InstanceSettings):
    """Write CurrentBiontyVersions to LAMINDB_VERSIONS_PATH in bionty."""
    if "bionty" in isettings.schema:
        from bionty.dev._handle_versions import (
            LAMINDB_VERSIONS_PATH,
            parse_current_versions,
        )
        from bionty.dev._io import write_yaml
        from lnschema_bionty.models import BiontyVersions

        current_df = BiontyVersions.objects.exclude(currentbiontyversions=None).df()
        current_dict = parse_current_versions(current_df.to_dict(orient="records"))

        write_yaml(current_dict, LAMINDB_VERSIONS_PATH)
        logger.hint(
            "Configured default bionty references using CurrentBiontyVersions table"
        )
