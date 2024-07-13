from pathlib import Path


def prune_django_api(reverse=False):
    from django import db

    attributes = [
        "DoesNotExist",
        "MultipleObjectsReturned",
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

    for file_path in django_path.rglob("*.py"):
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
