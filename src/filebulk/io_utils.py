import hashlib
import shutil


def io_copy(r, w) -> None:
    shutil.copyfileobj(r, w)


class Md5Sink:
    def __init__(self):
        self.hasher = hashlib.md5()
        self.write = self.hasher.update

    @property
    def digest(self) -> bytes:
        return self.hasher.digest()

    @property
    def hexdigest(self) -> str:
        return self.hasher.hexdigest()
