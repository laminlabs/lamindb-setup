import os
from pathlib import Path


def find_vscode_stubs_folder() -> Path | None:
    # Possible locations of VSCode extensions
    possible_locations = [
        Path.home() / ".vscode" / "extensions",  # Linux and macOS
        Path.home() / ".vscode-server" / "extensions",  # Remote development
        Path(os.environ.get("APPDATA", "")) / "Code" / "User" / "extensions",  # Windows
        Path("/usr/share/code/resources/app/extensions"),  # Some Linux distributions
    ]
    for location in possible_locations:
        if location.exists():
            # Look for Pylance extension folder
            pylance_folders = list(location.glob("ms-python.vscode-pylance-*"))
            if pylance_folders:
                # Sort to get the latest version
                latest_pylance = sorted(pylance_folders)[-1]
                stubs_folder = (
                    latest_pylance / "dist" / "bundled" / "stubs" / "django-stubs"
                )
                if stubs_folder.exists():
                    return stubs_folder

    return None


def prune_django_api(reverse=False):
    from django import db

    attributes = [
        "DoesNotExist",
        "MultipleObjectsReturned",
        "add_to_class",
        "adelete",
        "refresh_from_db",
        "asave",
        "clean",
        "clean_fields",
        "date_error_message",
        "full_clean",
        "get_constraints",
        "get_deferred_fields",
        "prepare_database_save",
        "save_base",
        "serializable_value",
        "unique_error_message",
        "validate_constraints",
        "validate_unique",
    ]
    if not reverse:
        attributes.append("a_refresh_from_db")
    else:
        attributes.append("arefresh_from_db")

    django_path = Path(db.__file__).parent.parent

    def prune_file(file_path):
        content = file_path.read_text()
        original_content = content

        for attr in attributes:
            old_name = f"_{attr}" if reverse else attr
            new_name = attr if reverse else f"_{attr}"
            content = content.replace(old_name, new_name)

        if not reverse:
            content = content.replace("Field_DoesNotExist", "FieldDoesNotExist")
            content = content.replace("Object_DoesNotExist", "ObjectDoesNotExist")

        if content != original_content:
            file_path.write_text(content)

    for file_path in django_path.rglob("*.py"):
        prune_file(file_path)

    pylance_path = find_vscode_stubs_folder()
    if pylance_path is not None:
        for file_path in pylance_path.rglob("*.pyi"):
            prune_file(file_path)
