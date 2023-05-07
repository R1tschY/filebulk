import os
import re
import sqlite3
import textwrap
from dataclasses import dataclass
import dataclasses
import fnmatch
from itertools import groupby, starmap
from operator import attrgetter, itemgetter
from pathlib import Path
from typing import Type, Any, Union, Dict, Iterable, Set, List

from filebulk.io_utils import Md5Sink, io_copy


def _whitecard_to_like(wc: str) -> str:
    return wc.replace("%", "%%").replace("_", "__").replace("*", "%").replace("?", "_")


@dataclass
class Entry:
    filePath: str
    fileSize: int
    md5Hash: str

    @property
    def path(self) -> Path:
        return Path(self.filePath)


SQLITE_TYPE_MAPPING = {
    int: "INTEGER",
    float: "REAL",
    str: "TEXT",
    bytes: "BLOB"
}


def _create_table_from(conn: sqlite3.Connection, cls: Type):
    columns = ", ".join([
        f"{field.name.lower()} {SQLITE_TYPE_MAPPING[field.type]}" for field in dataclasses.fields(cls)
    ])

    conn.execute(textwrap.dedent(f"""
        CREATE TABLE {cls.__name__.lower()} (
            {columns}
        )
    """))


def _insert_sql_from(cls: Type):
    fields = dataclasses.fields(cls)
    columns = ", ".join([
        field.name.lower() for field in fields
    ])

    return textwrap.dedent(f"""
        INSERT INTO {cls.__name__.lower()} (
            {columns}
        )
        VALUES ({', '.join(['?'] * len(fields))})
    """)


def _select_from(conn: sqlite3.Connection, cls: Type):
    columns = ", ".join([
        field.name.lower() for field in dataclasses.fields(cls)
    ])
    return starmap(Entry, conn.execute(textwrap.dedent(f"""
        SELECT {columns} FROM {cls.__name__.lower()}
    """)))


class Filter:
    def __init__(self, includes: List[str], excludes: List[str]):
        exprs = []
        if includes:
            self.includes = re.compile("|".join([fnmatch.translate(include) for include in includes]))
            exprs.append("(" + ") OR (".join([
                f"filePath LIKE '{_whitecard_to_like(include)}' COLLATE utf8_general_ci" for include in includes
            ]) + ")")
        else:
            self.includes = None

        if excludes:
            self.excludes = re.compile("|".join([fnmatch.translate(exclude) for exclude in excludes]))
            exprs.append("(" + ") AND (".join([
                f"filePath NOT LIKE '{_whitecard_to_like(exclude)}' COLLATE utf8_general_ci" for exclude in excludes
            ]) + ")")
        else:
            self.excludes = None

        if exprs:
            self.where = "(" + ") AND (".join(exprs) + ")"
        else:
            self.where = "TRUE"

        print(f"Filter SQL: {self.where}")

    def test(self, path: str) -> bool:
        if self.excludes and self.excludes.match(path):
            return False

        return not self.includes or self.includes.match(path)

    @property
    def sql(self):
        return self.where


class Index:
    conn: sqlite3.Connection
    root: Path

    @classmethod
    def from_file(cls, path: Path, root: Path = None):
        if root is None:
            root = path.parent
        return cls(sqlite3.connect(os.fspath(path)), root)

    @classmethod
    def new(cls, path: Union[str, os.PathLike], root: Path):
        index = cls.from_file(os.fspath(path), root)
        _create_table_from(index.conn, Entry)
        index.conn.execute("CREATE INDEX entry_hash_index ON entry (md5Hash);")
        return index

    def entry_for_path(self, filePath: Union[str, os.PathLike]):
        md5 = Md5Sink()
        with open(filePath, "rb") as fp:
            io_copy(fp, md5)

        return Entry(
            filePath=os.path.relpath(filePath, self.root),
            fileSize=os.path.getsize(filePath),
            md5Hash=md5.hexdigest
        )

    def __init__(self, conn: sqlite3.Connection, root: Path):
        self.conn = conn
        self.root = root
        self.insert_sql = _insert_sql_from(Entry)
        self.tuple_getter = attrgetter(*[
            field.name for field in dataclasses.fields(Entry)
        ])

    def __enter__(self):
        self.conn.__enter__()
        return self

    def __exit__(self, *args):
        self.conn.__exit__(*args)

    def add(self, e: Entry):
        self.conn.execute(self.insert_sql, self.tuple_getter(e))

    def duplicates(self) -> Dict[str, Iterable[str]]:
        result = self.conn.execute("""
            SELECT md5hash, filepath FROM entry
            WHERE md5hash IN
                (SELECT md5hash FROM entry GROUP BY md5hash HAVING count(filepath) > 1)
            ORDER BY md5hash
        """)

        return {
            key: [item[1] for item in values]
            for key, values in groupby(result, key=itemgetter(0))
        }

    def duplicate_entries(self) -> Dict[str, List[Entry]]:
        result = self.conn.execute("""
            SELECT filepath, filesize, md5hash FROM entry
            WHERE md5hash IN
                (SELECT md5hash FROM entry GROUP BY md5hash HAVING count(filepath) > 1)
            ORDER BY md5hash
        """)

        return {
            key: list(starmap(Entry, values))
            for key, values in groupby(result, key=itemgetter(0))
        }

    def entries(self) -> Iterable[Entry]:
        return _select_from(self.conn, Entry)

    def unique_hashes(self, filter: Filter) -> Set[str]:
        return {hash for hash, in self.conn.execute(f"SELECT DISTINCT md5hash FROM entry WHERE {filter.sql}")}

    def find_filepaths_for_hash(self, hash: str) -> List[str]:
        return [path for path, in self.conn.execute("""
            SELECT filepath FROM entry WHERE md5hash = ?
        """, (hash,))]



