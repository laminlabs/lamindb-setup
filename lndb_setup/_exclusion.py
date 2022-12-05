from datetime import datetime
from typing import Optional

import fsspec
from dateutil.parser import isoparse  # type: ignore

from ._settings import settings

EXPIRATION_TIME = 1800  # 30 min


class Lock:
    def __init__(self, user: Optional[str] = None):
        if user is None:
            user = settings.user.id
        self.user = user

        root = settings.instance.storage_root
        protocol = fsspec.utils.get_protocol(str(root))

        self.fs = fsspec.filesystem(protocol)

        exclusion_path = root / "exclusion"
        self.mapper = fsspec.FSMap(str(exclusion_path), self.fs, create=True)

        priorities_path = str(exclusion_path / "priorities")
        if self.fs.exists(priorities_path):
            self.users = self.mapper["priorities"].decode().split("*")

            if user not in self.users:
                self.priority = len(self.users)
                self.users.append(user)
                # potential problem here if 2 users join at the same time
                # can be avoided by using separate files for each user
                # and giving priority by timestamp
                # here writing the whole list back because gcs
                # does not support the append mode
                self.mapper["priorities"] = "*".join(self.users).encode()
            else:
                self.priority = self.users.index(user)
        else:
            self.mapper["priorities"] = user.encode()
            self.users = [user]
            self.priority = 0

        self.mapper[f"numbers/{user}"] = b"0"
        self.mapper[f"entering/{user}"] = b"0"

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

    def modified(self, path):
        try:
            mtime = self.fs.modified(path)
        except NotImplementedError:
            # todo: check more protocols
            # here only for gs
            mtime = self.fs.stat(path)["updated"]
            mtime = isoparse(mtime)
        # always convert to the local timezone before returning
        return mtime.astimezone().replace(tzinfo=None)

    def lock(self):
        if len(self.users) < 2:
            return None

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

    def unlock(self):
        if len(self.users) < 2:
            return None
        self.mapper[f"numbers/{self.user}"] = b"0"
