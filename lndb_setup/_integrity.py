def check_before_init(instance_type, storage_type):
    if is_instance_name_already_used():
        # Fail
        return False

    if is_db_url_already_used():
        # Fail
        return False

    if storage_type == "cloud" and not is_storage_exists():
        # Fail, cloud storage does note exists
        return False

    if instance_type == "sqlite" and is_storage_already_used():
        # Fail, SQLite cannot use a shared storage
        return False

    if not is_storage_exists() and is_storage_already_used():
        # Cannot found storage directory
        # Ask to delete metadata
        return False

    if is_storage_exists() and not is_storage_already_used():
        # Cannot found storage metadata
        # Ask to delete directory
        return False

    if is_storage_already_used():
        # Warning, using a shared storage
        pass

    return True


def check_before_load():
    if not is_db_setup():
        return False

    if not is_storage_exists():
        return False

    if not is_db_in_sync_with_storages():
        return False

    return True


def is_instance_name_already_used():
    pass


def is_db_url_already_used():
    pass


def is_db_setup():
    pass


def is_storage_already_used():
    pass


def is_storage_exists():
    pass


def is_db_in_sync_with_storages():
    pass
