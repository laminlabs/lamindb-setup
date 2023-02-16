from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

import fsspec
from cloudpathlib import CloudPath
from dateutil.parser import isoparse  # type: ignore
from lamin_logger import logger

EXPIRATION_TIME = 1800  # 30 min

MAX_MSG_COUNTER = 1000  # print the msg after this number of iterations


class empty_locker:
    @classmethod
    def lock(cls):
        pass

    @classmethod
    def unlock(cls):
        pass


class Locker:
    def __init__(self, user_id: str, storage_root: Union[CloudPath, Path]):
        logger.debug(f"Init cloud sqlite locker: {user_id}, {storage_root}.")

        self._counter = 0

        self.user = user_id

        root = storage_root
        protocol = fsspec.utils.get_protocol(str(root))

        if protocol == "s3":
            fs_kwargs = {"cache_regions": True}
        else:
            fs_kwargs = {}

        self.fs = fsspec.filesystem(protocol, **fs_kwargs)

        exclusion_path = root / "exclusion"
        self.mapper = fsspec.FSMap(str(exclusion_path), self.fs, create=True)

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
                    self.mapper[user_endpoint] = b"0"

        self._locked = False

    def modified(self, path):
        if "gcs" not in self.fs.protocol:
            mtime = self.fs.modified(path)
        else:
            stat = self.fs.stat(path)
            if "updated" in stat:
                mtime = stat["updated"]
                mtime = isoparse(mtime)
            else:
                return None
        # always convert to the local timezone before returning
        # assume in utc if the time zone is not specified
        if mtime.tzinfo is None:
            mtime = mtime.replace(tzinfo=timezone.utc)
        return mtime.astimezone().replace(tzinfo=None)

    def _msg_on_counter(self):
        if self._counter == MAX_MSG_COUNTER:
            logger.info(
                "Another user is doing a write operation to the database, "
                "please wait or stop the code execution."
            )

        if self._counter <= MAX_MSG_COUNTER:
            self._counter += 1

    def _lock_unsafe(self):
        if self._locked:
            return None

        self.users = self.mapper["priorities"].decode().split("*")

        self.mapper[f"entering/{self.user}"] = b"1"

        numbers = [int(self.mapper[f"numbers/{user}"]) for user in self.users]
        number = 1 + max(numbers)
        self.mapper[f"numbers/{self.user}"] = str(number).encode()

        self.mapper[f"entering/{self.user}"] = b"0"

        for i, user in enumerate(self.users):
            if i == self.priority:
                continue

            while int(self.mapper[f"entering/{user}"]):
                self._msg_on_counter()
            while True:
                c_number = int(self.mapper[f"numbers/{user}"])
                if c_number == 0:
                    break
                if number < c_number:
                    break
                if number == c_number and self.priority < i:
                    break
                self._msg_on_counter()

        self._locked = True

    def lock(self):
        try:
            self._lock_unsafe()
        except BaseException as e:
            self.unlock()
            raise e

    def unlock(self):
        self.mapper[f"numbers/{self.user}"] = b"0"

        self._locked = False
        self._counter = 0

    def _clear(self):
        self.unlock()
        self.mapper[f"entering/{self.user}"] = b"0"


_locker: Optional[Locker] = None


def get_locker() -> Locker:
    from ._settings import settings

    global _locker

    if _locker is None:
        _locker = Locker(settings.user.id, settings.instance.storage.root)

    return _locker
