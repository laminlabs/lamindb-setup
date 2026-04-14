from __future__ import annotations

from typing import TYPE_CHECKING

from lamindb_setup.core import _settings_load as settings_load

if TYPE_CHECKING:
    from pathlib import Path


def _write_instance_env(path: Path, owner: str, name: str, instance_id: str) -> None:
    path.write_text(
        "\n".join(
            [
                f"lamindb_instance_owner={owner}",
                f"lamindb_instance_name={name}",
                "lamindb_instance_storage_root=s3://bucket",
                "lamindb_instance_storage_region=us-east-1",
                "lamindb_instance_db=null",
                "lamindb_instance_schema_str=bionty",
                f"lamindb_instance_id={instance_id}",
                "lamindb_instance_git_repo=null",
                "lamindb_instance_keep_artifacts_local=False",
            ]
        )
    )


def test_load_instance_settings_precedence_env_local_global(
    tmp_path: Path, monkeypatch
) -> None:
    env_file = tmp_path / "env.env"
    local_file = tmp_path / "local.env"
    global_file = tmp_path / "global.env"
    marker_file = tmp_path / "marker"
    _write_instance_env(
        env_file, "envowner", "envname", "11111111111111111111111111111111"
    )
    _write_instance_env(
        local_file, "localowner", "localname", "22222222222222222222222222222222"
    )
    _write_instance_env(
        global_file, "globalowner", "globalname", "33333333333333333333333333333333"
    )
    marker_file.write_text("localowner/localname")

    def _instance_file(name: str, owner: str) -> Path:
        lookup = {
            ("envowner", "envname"): env_file,
            ("localowner", "localname"): local_file,
            ("globalowner", "globalname"): global_file,
        }
        return lookup[(owner, name)]

    monkeypatch.setattr(settings_load, "instance_settings_file", _instance_file)
    monkeypatch.setattr(
        settings_load, "current_instance_settings_file", lambda: global_file
    )
    monkeypatch.setattr(
        settings_load, "find_local_current_instance_file", lambda: marker_file
    )

    monkeypatch.setenv("LAMIN_CURRENT_INSTANCE", "envowner/envname")
    resolved = settings_load.load_instance_settings()
    assert resolved.slug == "envowner/envname"

    monkeypatch.delenv("LAMIN_CURRENT_INSTANCE")
    resolved = settings_load.load_instance_settings()
    assert resolved.slug == "localowner/localname"

    monkeypatch.setattr(settings_load, "find_local_current_instance_file", lambda: None)
    resolved = settings_load.load_instance_settings()
    assert resolved.slug == "globalowner/globalname"


def test_load_instance_settings_falls_back_to_global_if_local_marker_invalid(
    tmp_path: Path, monkeypatch
) -> None:
    global_file = tmp_path / "global.env"
    marker_file = tmp_path / "marker"
    _write_instance_env(
        global_file, "globalowner", "globalname", "44444444444444444444444444444444"
    )
    marker_file.write_text("invalid-marker-format")

    monkeypatch.delenv("LAMIN_CURRENT_INSTANCE", raising=False)
    monkeypatch.setattr(
        settings_load, "current_instance_settings_file", lambda: global_file
    )
    monkeypatch.setattr(
        settings_load, "find_local_current_instance_file", lambda: marker_file
    )

    resolved = settings_load.load_instance_settings()
    assert resolved.slug == "globalowner/globalname"


def test_load_instance_settings_falls_back_to_global_if_local_marker_missing_cache(
    tmp_path: Path, monkeypatch
) -> None:
    global_file = tmp_path / "global.env"
    marker_file = tmp_path / "marker"
    _write_instance_env(
        global_file, "globalowner", "globalname", "55555555555555555555555555555555"
    )
    marker_file.write_text("localowner/localname")

    def _instance_file(name: str, owner: str) -> Path:
        if (owner, name) == ("globalowner", "globalname"):
            return global_file
        return tmp_path / f"{owner}--{name}.env"

    monkeypatch.delenv("LAMIN_CURRENT_INSTANCE", raising=False)
    monkeypatch.setattr(settings_load, "instance_settings_file", _instance_file)
    monkeypatch.setattr(
        settings_load, "current_instance_settings_file", lambda: global_file
    )
    monkeypatch.setattr(
        settings_load, "find_local_current_instance_file", lambda: marker_file
    )

    resolved = settings_load.load_instance_settings()
    assert resolved.slug == "globalowner/globalname"
