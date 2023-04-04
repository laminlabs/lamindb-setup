import importlib
from pathlib import Path


def get_package_dir(package_name: str) -> Path:
    if not Path(package_name).exists():
        module = importlib.import_module(package_name)
        schema_package_dir = Path(module.__file__).parent  # type: ignore
    else:
        schema_package_dir = Path(package_name)
    return schema_package_dir
