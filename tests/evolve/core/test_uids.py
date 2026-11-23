from __future__ import annotations

import subprocess
import sys

import pytest
from lamindb_setup.evolve.core import UID, validate_lamindb_uid


def test_validate_uid_with_registry_length() -> None:
    parsed = validate_lamindb_uid(
        "Ab12Cd34Ef56Gh78", registry="lamindb.models.record.Record"
    )
    assert parsed.registry_name == "lamindb.models.record.Record"
    assert parsed.uid_length == 16
    assert parsed.uid_stem_length == 16


def test_validate_uid_versioned_registry() -> None:
    parsed = validate_lamindb_uid(
        "Ab12Cd34Ef56Gh780000", registry="lamindb.models.artifact.Artifact"
    )
    assert parsed.registry_name == "lamindb.models.artifact.Artifact"
    assert parsed.uid_length == 20
    assert parsed.uid_stem_length == 16


def test_validate_uid_bionty_registry_length() -> None:
    parsed = validate_lamindb_uid("Ab12Cd34Ef56Gh", registry="bionty.models.Tissue")
    assert parsed.registry_name == "bionty.models.Tissue"
    assert parsed.uid_length == 14
    assert parsed.uid_stem_length == 14


def test_validate_uid_rejects_non_base62() -> None:
    with pytest.raises(ValueError, match="Base62"):
        validate_lamindb_uid("Ab12Cd34Ef!!", registry="bionty.models.Feature")


def test_uid_stores_registry_and_length() -> None:
    value = UID("Ab12Cd34Ef56Gh78", registry="lamindb.models.record.Record")
    assert value.registry_name is None
    assert value.uid_length == 16
    assert value.stem == "Ab12Cd34Ef56Gh78"
    assert value.version is None


def test_uid_stores_version_for_versioned_registry() -> None:
    value = UID("Ab12Cd34Ef56Gh78000A", registry="lamindb.models.artifact.Artifact")
    assert str(value) == "Ab12Cd34Ef56Gh78000A"
    assert value.stem == "Ab12Cd34Ef56Gh78"
    assert value.version == "000A"


@pytest.mark.parametrize("value", ["!" * 12, "é" * 12])
def test_uid_without_registry_rejects_non_base62(value: str) -> None:
    with pytest.raises(ValueError, match="Base62"):
        UID(value)


def test_the_package_imports_without_lamin_utils() -> None:
    # lamin-utils is archived and no runtime dependency installs it anymore, so
    # neither this package nor the bionty release it requires may need it.
    # bionty loads `bionty.base` -- where 2.4 imported lamin_utils -- before it
    # checks the connected instance has the module, so that check may refuse
    # depending on the instance this process inherits; anything else may not.
    # (pertdb only imports its models after that check, so the tests that
    # connect to a pertdb instance cover its floor instead.)
    code = (
        "import sys\n"
        "sys.modules['lamin_utils'] = None\n"
        "import lamindb_setup.evolve\n"
        "from lamindb_setup.evolve.core import UID\n"
        "UID('Ab12Cd34Ef56Gh78')\n"
        "try:\n"
        "    import bionty.base\n"
        "except Exception as exc:\n"
        "    if type(exc).__name__ != 'ModuleWasntConfigured':\n"
        "        raise\n"
        "assert 'bionty.base' in sys.modules\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
