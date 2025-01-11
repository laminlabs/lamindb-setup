from __future__ import annotations

import os
from typing import TYPE_CHECKING

from django.db.utils import OperationalError, ProgrammingError
from lamin_utils import logger

from ._settings import settings as setup_settings

if TYPE_CHECKING:
    from ._settings_instance import InstanceSettings

# bionty.Source -> bionty.base
RENAME = {"name": "source", "description": "source_name"}


def write_bionty_sources(isettings: InstanceSettings) -> None:
    """Write bionty sources to Source table."""
    if "bionty" not in isettings.modules:
        return None
    import shutil

    import bionty
    import bionty.base as bionty_base
    from bionty._bionty import list_biorecord_models
    from bionty.base.dev._handle_sources import parse_sources_yaml
    from bionty.models import Source

    bionty_models = list_biorecord_models(bionty)

    shutil.copy(
        bionty_base.settings.current_sources, bionty_base.settings.lamindb_sources
    )

    all_sources = parse_sources_yaml(bionty_base.settings.local_sources)
    all_sources_dict = all_sources.to_dict(orient="records")

    def _get_currently_used(key: str):
        return (
            bionty_base.display_currently_used_sources()
            .reset_index()
            .set_index(["entity", key])
        )

    currently_used = _get_currently_used("organism")

    all_records = []
    for kwargs in all_sources_dict:
        act = currently_used.loc[(kwargs["entity"], kwargs["organism"])].to_dict()
        if (act["source"] == kwargs["source"]) and (
            act["version"] == kwargs["version"]
        ):
            kwargs["currently_used"] = True

        # when the database is not yet migrated but setup is updated
        # won't need this once lamindb is released with the new pin
        kwargs["run"] = None  # can't yet access tracking information
        kwargs["in_db"] = False
        for db_field, base_col in RENAME.items():
            kwargs[db_field] = kwargs.pop(base_col)
        if kwargs["entity"] in bionty_models:
            kwargs["entity"] = f"bionty.{kwargs['entity']}"
        record = Source(**kwargs)
        all_records.append(record)

    Source.objects.bulk_create(all_records, ignore_conflicts=True)


def load_bionty_sources(isettings: InstanceSettings | None = None):
    """Write currently_used bionty sources to LAMINDB_VERSIONS_PATH in bionty."""
    if isettings is None:
        if setup_settings._instance_settings is not None:
            isettings = setup_settings.instance
        else:
            logger.warning(
                f"Ignoring bionty setup because running in LAMINDB_MULTI_INSTANCE mode = {os.environ.get('LAMINDB_MULTI_INSTANCE')}"
            )
            # not setting up bionty sources
            return None
    if isettings is not None:
        if "bionty" not in isettings.modules:
            # no need to setup anything
            return None

    import bionty.base as bionty_base
    from bionty.base.dev._handle_sources import parse_currently_used_sources
    from bionty.base.dev._io import write_yaml
    from bionty.models import Source

    try:
        # need try except because of integer primary key migration
        active_records = (
            Source.objects.filter(currently_used=True).order_by("id").all().values()
        )
        for kwargs in active_records:
            for db_field, base_col in RENAME.items():
                kwargs[base_col] = kwargs.pop(db_field)
            # TODO: non-bionty modules?
            kwargs["entity"] = kwargs["entity"].replace("bionty.", "")
        write_yaml(
            parse_currently_used_sources(active_records),
            bionty_base.settings.lamindb_sources,
        )
    except (OperationalError, ProgrammingError):
        pass


def delete_bionty_sources_yaml():
    """Delete LAMINDB_SOURCES_PATH in bionty."""
    try:
        import bionty.base as bionty_base

        bionty_base.settings.lamindb_sources.unlink(missing_ok=True)
    except ModuleNotFoundError:
        pass
