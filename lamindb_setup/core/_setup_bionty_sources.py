from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._settings_instance import InstanceSettings


def write_bionty_sources(isettings: InstanceSettings) -> None:
    """Write public bionty sources to bt.Source table."""
    if "bionty" not in isettings.modules:
        return None

    import bionty
    import bionty.base as bionty_base
    from bionty._biorecord import list_biorecord_models
    from bionty.base.dev._handle_sources import (
        parse_currently_used_sources,
        records_from_sources_yaml,
    )
    from bionty.models import Source

    bionty_models = list_biorecord_models(bionty)

    all_sources = records_from_sources_yaml(bionty_base.settings.public_sources)
    currently_used = parse_currently_used_sources(all_sources)

    all_records = []
    for source in all_sources:
        kwargs = dict(source)
        current = currently_used[kwargs["entity"]][kwargs["organism"]]
        if current.get(kwargs["name"]) == kwargs["version"]:
            kwargs["currently_used"] = True

        kwargs["run"] = None  # can't yet access tracking information
        kwargs["in_db"] = False
        if kwargs["entity"] in bionty_models:
            kwargs["entity"] = f"bionty.{kwargs['entity']}"
        record = Source(**kwargs)
        all_records.append(record)

    Source.objects.bulk_create(all_records, ignore_conflicts=True)
