from __future__ import annotations

from datetime import datetime, timezone
from functools import wraps
from typing import TYPE_CHECKING, Optional, Union

from lamin_utils import logger

from .upath import UPath, create_mapper, infer_filesystem

if TYPE_CHECKING:
    from pathlib import Path
    from uuid import UUID

EXPIRATION_TIME = 24 * 60 * 60 * 7  # 7 days

MAX_MSG_COUNTER = 100  # print the msg after this number of iterations


# raise if an instance is already locked
# ignored by unlock_cloud_sqlite_upon_exception
class InstanceLockedException(Exception):
    pass


class empty_locker:
    has_lock = True

    @classmethod
    def lock(cls):
        pass

    @classmethod
    def unlock(cls):
        pass


class Locker:
    def __init__(self, user_uid: str, storage_root: UPath | Path, instance_id: UUID):
        logger.debug(
            f"init cloud sqlite locker: {user_uid}, {storage_root}, {instance_id}."
        )

        self._counter = 0

        self.user = user_uid
        self.instance_id = instance_id

        self.root = storage_root
        self.fs, root_str = infer_filesystem(storage_root)

        exclusion_path = storage_root / f".lamindb/_exclusion/{instance_id.hex}"

        self.mapper = create_mapper(self.fs, str(exclusion_path), create=True)

        priorities_path = str(exclusion_path / "priorities")
        if self.fs.exists(priorities_path):
            self.users = self.mapper["priorities"].decode().split("*")

            if self.user not in self.users:
                self.priority = len(self.users)
                self.users.append(self.user)
                # potential problem here if 2 users join at the same time
                # can be avoided by using separate files for each user
                # and giving priority by timestamp
                # here writing the whole list back because gcs
                # does not support the append mode
                self.mapper["priorities"] = "*".join(self.users).encode()
            else:
                self.priority = self.users.index(self.user)
        else:
            self.mapper["priorities"] = self.user.encode()
            self.users = [self.user]
            self.priority = 0

        self.mapper[f"numbers/{self.user}"] = b"0"
        self.mapper[f"entering/{self.user}"] = b"0"

        # clean up failures
        for user in self.users:
            for endpoint in ("numbers", "entering"):
                user_endpoint = f"{endpoint}/{user}"
                user_path = str(exclusion_path / user_endpoint)
                if not self.fs.exists(user_path):
                    continue
                if self.mapper[user_endpoint] == b"0":
                    continue
                period = (datetime.now() - self.modified(user_path)).total_seconds()
                if period > EXPIRATION_TIME:
                    logger.info(
                        f"the lock of the user {user} seems to be stale, clearing"
                        f" {endpoint}."
                    )
                    self.mapper[user_endpoint] = b"0"

        self._has_lock = None
        self._locked_by = None

    def modified(self, path):
        mtime = self.fs.modified(path)
        # always convert to the local timezone before returning
        # assume in utc if the time zone is not specified
        if mtime.tzinfo is None:
            mtime = mtime.replace(tzinfo=timezone.utc)
        return mtime.astimezone().replace(tzinfo=None)

    def _msg_on_counter(self, user):
        if self._counter == MAX_MSG_COUNTER:
            logger.warning(f"competing for the lock with the user {user}.")

        if self._counter <= MAX_MSG_COUNTER:
            self._counter += 1

    def _lock_unsafe(self):
        if self._has_lock:
            return None

        self._has_lock = True
        self._locked_by = self.user

        self.users = self.mapper["priorities"].decode().split("*")

        self.mapper[f"entering/{self.user}"] = b"1"

        numbers = [int(self.mapper[f"numbers/{user}"]) for user in self.users]
        number = 1 + max(numbers)
        self.mapper[f"numbers/{self.user}"] = str(number).encode()

        self.mapper[f"entering/{self.user}"] = b"0"

        for i, user in enumerate(self.users):
            if i == self.priority:
                continue

            while self.mapper[f"entering/{user}"] == b"1":
                self._msg_on_counter(user)

            c_number = int(self.mapper[f"numbers/{user}"])

            if c_number == 0:
                continue

            if (number > c_number) or (number == c_number and self.priority > i):
                self._has_lock = False
                self._locked_by = user
                self.mapper[f"numbers/{self.user}"] = b"0"
                return None

    def lock(self):
        try:
            self._lock_unsafe()
        except BaseException as e:
            self.unlock()
            self._clear()
            raise e

    def unlock(self):
        self.mapper[f"numbers/{self.user}"] = b"0"
        self._has_lock = None
        self._locked_by = None
        self._counter = 0

    def _clear(self):
        self.mapper[f"entering/{self.user}"] = b"0"

    @property
    def has_lock(self):
        if self._has_lock is None:
            logger.info("the lock has not been initialized, trying to obtain the lock.")
            self.lock()

        return self._has_lock


_locker: Locker | None = None


def get_locker(isettings) -> Locker:
    from ._settings import settings

    global _locker

    user_uid = settings.user.uid
    storage_root = isettings.storage.root

    if (
        _locker is None
        or _locker.user != user_uid
        or _locker.root is not storage_root
        or _locker.instance_id != isettings._id
    ):
        _locker = Locker(user_uid, storage_root, isettings._id)

    return _locker


def clear_locker():
    global _locker

    _locker = None


# decorator
def unlock_cloud_sqlite_upon_exception(ignore_prev_locker: bool = False):
    """Decorator to unlock a cloud sqlite instance upon an exception.

    Ignores `InstanceLockedException`.

    Args:
        ignore_prev_locker: `bool` - Do not unlock if locker hasn't changed.
    """

    def wrap_with_args(func):
        # https://stackoverflow.com/questions/1782843/python-decorator-handling-docstrings
        @wraps(func)
        def wrapper(*args, **kwargs):
            prev_locker = _locker
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                if isinstance(exc, InstanceLockedException):
                    raise exc
                if ignore_prev_locker and _locker is prev_locker:
                    raise exc
                if _locker is not None and _locker._has_lock:
                    _locker.unlock()
                raise exc

        return wrapper

    return wrap_with_args
