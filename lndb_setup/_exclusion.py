from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

import fsspec
from cloudpathlib import CloudPath
from dateutil.parser import isoparse  # type: ignore

EXPIRATION_TIME = 1800  # 30 min


class Locker:
    def __init__(self, user_id: str, storage_root: Union[CloudPath, Path]):
        print("init locker", user_id, storage_root)
        self.user = user_id

        root = storage_root
        protocol = fsspec.utils.get_protocol(str(root))

        self.fs = fsspec.filesystem(protocol)

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
        try:
            mtime = self.fs.modified(path)
        except NotImplementedError:
            # todo: check more protocols
            # here only for gs
            mtime = self.fs.stat(path)["updated"]
            mtime = isoparse(mtime)
        # always convert to the local timezone before returning
        # assume in utc if the time zone is not specified
        if mtime.tzinfo is None:
            mtime = mtime.replace(tzinfo=timezone.utc)
        return mtime.astimezone().replace(tzinfo=None)

    def lock(self):
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
                pass
            while True:
                c_number = int(self.mapper[f"numbers/{user}"])
                if c_number == 0:
                    break
                if number < c_number:
                    break
                if number == c_number and self.priority < i:
                    break

        self._locked = True

    def unlock(self):
        self.mapper[f"numbers/{self.user}"] = b"0"

        self._locked = False


_locker: Optional[Locker] = None


def get_locker() -> Locker:
    from ._settings import settings

    global _locker

    if _locker is None:
        _locker = Locker(settings.user.id, settings.instance.storage.root)

    return _locker
