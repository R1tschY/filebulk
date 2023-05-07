import argparse
import hashlib
import os
import shutil
import sys
from collections import defaultdict
from itertools import groupby
from operator import itemgetter
from os import DirEntry
from pathlib import Path
from typing import Union, List, Dict, Iterable

from tqdm import tqdm

from filebulk.index import Index, Filter

COMMANDS = []


def command(fn):
    COMMANDS.append(fn)
    return fn


def index_for_dir(path: str) -> Index:
    index_file = Path(path) / "filebulk-index.db"
    return Index.from_file(index_file, Path(path))


def path_split_all(path: str):
    l = []
    while True:
        path, name = os.path.split(path)
        if not path:
            return l
        l.append(path)


def has_parent_with_same_hash(path, hash, dirs):
    parent = os.path.dirname(path)
    if parent:
        print(f"{path} {hash}: {parent} {dirs.get(parent)}: {dirs.get(parent) == hash}")
        return dirs.get(parent) == hash
    else:
        return False


def eval_dir_dups(index: Index):
    # Hash for every dir
    d = defaultdict(hashlib.md5)
    for entry in index.entries():
        if os.path.basename(entry.filePath).lower() != "thumbs.db":
            for pathPart in path_split_all(entry.filePath):
                d[pathPart].update(entry.md5Hash.encode("ascii"))

    # eval hashes
    d = {
        path: hash.hexdigest()
        for path, hash in d.items()
    }

    # filter single sub-folders
    d = {
        path: hash
        for path, hash in d.items()
        if not has_parent_with_same_hash(path, hash, d)
    }

    # Find duplicates
    return {
        k: paths
        for k, paths in [
            (hash, [path[0] for path in paths])
            for hash, paths in groupby(sorted(d.items(), key=itemgetter(1)), key=itemgetter(1))
        ]
        if len(paths) > 1
    }


def eval_missing(left: Index, right: Index, filter: Filter) -> Dict[str, List[str]]:
    left_hashes = left.unique_hashes(filter)
    right_hashes = right.unique_hashes(filter)
    missing = right_hashes - left_hashes

    print(f"LEFT: {len(left_hashes)}")
    print(f"RIGHT: {len(right_hashes)}")
    print(f"MISSING ON LEFT: {len(missing)}")
    if len(missing) > 5000:
        raise RuntimeError("Abort: too many hashes")
    return {
        hash: right.find_filepaths_for_hash(hash)
        for hash in missing
    }


def copy_dict_result(d: Dict[str, Iterable[str]], src_dir: Path, dest_dir: Path) -> None:
    for hash, dups in d.items():
        for dup in dups:
            src = src_dir / dup
            dest = dest_dir / dup
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)


def print_dict_result(d: Dict[str, Iterable[str]]) -> None:
    for i, (hash, dups) in enumerate(d.items()):
        if i > 500:
            print(f".. and {len(d) - 500} more elements ..")
            break

        print(f"== {hash} ==")
        for dup in dups:
            print(f"\t{dup}")

    print(f"-> {len(d)}")


def prefix_dedup(inp: List[str]):
    inp.sort()
    result = set()
    for value in inp:
        for entry in result:
            if value.startswith(entry):
                continue
        result.add(value)
    return result


@command
def dirdups(cmd_args) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dir")
    args = parser.parse_args(cmd_args)

    index = index_for_dir(args.dir)
    dirdups = eval_dir_dups(index)
    print_dict_result(dirdups)
    return 0


@command
def dups(cmd_args) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dir")
    args = parser.parse_args(cmd_args)

    index = index_for_dir(args.dir)
    all_dups = index.duplicates()
    print_dict_result(all_dups)
    return 0


@command
def dedupsize(cmd_args) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dir")
    args = parser.parse_args(cmd_args)

    index = index_for_dir(args.dir)
    all_dups = index.duplicate_entries()
    size = sum((dups[0].fileSize for hash, dups in all_dups.items()))
    print(f"{size} bytes")
    return 0


@command
def missing(cmd_args) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("left")
    parser.add_argument("right")
    parser.add_argument("--include", action='append')
    parser.add_argument("--exclude", action='append')
    parser.add_argument("--copy", default=None)
    args = parser.parse_args(cmd_args)

    left = index_for_dir(args.left)
    right = index_for_dir(args.right)

    missing = eval_missing(left, right, Filter(args.include, args.exclude))
    if args.copy:
        copy_dict_result(
            missing, Path(args.right).expanduser(), Path(args.copy).expanduser())

    print_dict_result(missing)
    return 0


@command
def index(cmd_args) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dir")
    parser.add_argument("--include", action='append')
    parser.add_argument("--exclude", action='append')
    args = parser.parse_args(cmd_args)

    root = args.dir
    index_file = os.path.join(root, "filebulk-index.db")
    if os.path.exists(index_file):
        os.remove(index_file)

    filter = Filter(args.include, args.exclude)
    with Index.new(index_file, root) as index:
        def index_dir(dir: Union[DirEntry, str]):
            with os.scandir(dir) as it:
                pbar = tqdm(
                    tuple(it),
                    desc=os.path.basename(dir),
                    leave=False,
                    miniters=1)
                for entry in pbar:
                    pbar.set_postfix_str(entry.name)

                    if not filter.test(entry.path):
                        continue

                    if entry.is_file():
                        index.add(index.entry_for_path(
                            filePath=entry.path))
                    elif entry.is_dir():
                        index_dir(entry)

        index_dir(root)

    return 0


def main():
    choices = [cmd.__name__ for cmd in COMMANDS]

    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=choices)
    parser.add_argument("args", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    sys.exit(COMMANDS[choices.index(args.command)](args.args))


if __name__ == '__main__':
    main()
