"""Setup core library.

Settings
--------

.. autoclass:: SetupSettings

.. autoclass:: UserSettings

.. autoclass:: InstanceSettings

.. autoclass:: StorageSettings

"""

from importlib import import_module

__all__ = [
    "django",
    "upath",
    "upload_sqlite_clone",
    "deprecated",
    "doc_args",
    "SetupSettings",
    "InstanceSettings",
    "StorageSettings",
    "UserSettings",
    "CanonicalSuffix",
]

_LAZY_ATTRS = {
    "django": (".django", None),
    "upath": (".upath", None),
    "upload_sqlite_clone": ("._clone", "upload_sqlite_clone"),
    "deprecated": ("._deprecated", "deprecated"),
    "doc_args": ("._docs", "doc_args"),
    "SetupSettings": ("._settings", "SetupSettings"),
    "InstanceSettings": ("._settings_instance", "InstanceSettings"),
    "StorageSettings": ("._settings_storage", "StorageSettings"),
    "UserSettings": ("._settings_user", "UserSettings"),
    "CanonicalSuffix": (".canonical_suffix", "CanonicalSuffix"),
}


def __getattr__(name: str):
    if name in _LAZY_ATTRS:
        module_name, attr_name = _LAZY_ATTRS[name]
        module = import_module(module_name, __name__)
        value = module if attr_name is None else getattr(module, attr_name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
