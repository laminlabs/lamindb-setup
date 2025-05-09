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
    from bionty.base.dev._handle_sources import parse_sources_yaml
    from bionty.models import Source

    bionty_models = list_biorecord_models(bionty)

    all_sources = parse_sources_yaml(bionty_base.settings.public_sources)
    all_sources_dict = all_sources.to_dict(orient="records")

    currently_used = (
        bionty_base.display_currently_used_sources(mute=True)
        .reset_index()
        .set_index(["entity", "organism"])
    )

    all_records = []
    for kwargs in all_sources_dict:
        act = currently_used.loc[(kwargs["entity"], kwargs["organism"])].to_dict()
        if (act["name"] == kwargs["name"]) and (act["version"] == kwargs["version"]):
            kwargs["currently_used"] = True

        kwargs["run"] = None  # can't yet access tracking information
        kwargs["in_db"] = False
        if kwargs["entity"] in bionty_models:
            kwargs["entity"] = f"bionty.{kwargs['entity']}"
        record = Source(**kwargs)
        all_records.append(record)

    Source.objects.bulk_create(all_records, ignore_conflicts=True)
