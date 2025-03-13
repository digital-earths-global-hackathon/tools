#! /usr/bin/env python
# -*- coding: utf-8 -*-
# BSD 3-Clause License

import os
import shutil
import glob
from pathlib import Path
from typing import List
import filecmp
import zarr
import json


def clone_dataset(source_dirs: List[Path], target_dir: Path):
    target = Path(target_dir)
    target.mkdir(parents=True)
    source_dirs = [Path(d).resolve() for d in source_dirs]
    files, dirs = get_files(source_dirs)
    files = sort_duplicates(files)
    dirs = sort_duplicates(dirs)
    do_cloning(target, files, dirs)
    merge_zattrs(source_dirs, target)
    zarr.consolidate_metadata(target)


def merge_zattrs(source_dirs: List[Path], target_dir: Path):
    attrs = {}
    for source_dir in source_dirs:
        with open(Path(source_dir, ".zattrs")) as f:
            attrs.update(json.load(f))
    with open(Path(target_dir, ".zattrs"), "w") as f:
        json.dump(attrs, f)


def do_cloning(target, files, dirs):
    [shutil.copy(f, target) for f in files]
    for d in dirs:
        os.makedirs(Path(target, os.path.basename(d)))
        ff, dd = get_source_files_and_dirs(d)
        t = Path(target, os.path.basename(d))
        for f in ff:
            shutil.copy(f, t)
        for _d in dd:
            os.symlink(_d, Path(t, os.path.basename(_d)))


def get_files(source_dirs: List[Path]):
    files = {}
    dirs = {}
    for source_dir in source_dirs:
        files[source_dir], dirs[source_dir] = get_source_files_and_dirs(source_dir)
    return files, dirs


def to_be_ignored(f):
    ignore_files = [".zmetadata", ".zattrs"]
    if f in ignore_files:
        return True
    return False


def sort_duplicates(items: dict):
    new_names = {}
    for source_dir, source_files in items.items():
        for f in source_files:
            f = str(Path(f).relative_to(source_dir))
            if not to_be_ignored(f):
                if f not in new_names:
                    new_names[f] = source_dir
                else:
                    check_duplicate(f"{source_dir}/{f}", f"{new_names[f]}/{f}")
    return [Path(d) / Path(f) for f, d in new_names.items()]


def check_duplicate(f1, f2):
    if Path(f1).is_dir():
        comp = run_dircomp(f1, f2)
    else:
        comp = filecmp.cmp
    if not comp(f1, f2):
        raise ValueError(f"Files {f1} and {f2} are different but should be the same.")


def run_dircomp(f1, f2):
    dcmp = filecmp.dircmp(f1, f2)
    for x in [dcmp.left_only, dcmp.right_only, dcmp.diff_files, dcmp.funny_files]:
        if x:
            raise ValueError(
                f"Files ['{f1}','{f2}']/{x} are different but should be the same."
            )


def get_source_files_and_dirs(path: Path):
    all = glob.glob(f"{path}/.*") + glob.glob(f"{path}/*")
    files = [x for x in all if os.path.isfile(x)]
    dirs = [x for x in all if os.path.isdir(x)]
    return files, dirs


def parse_args():
    import argparse

    parser = argparse.ArgumentParser(description="Clone a dataset to a new location")
    parser.add_argument("source_dirs", type=Path, help="Source directories", nargs="+")
    parser.add_argument("target_dir", type=Path, help="Target directory")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    clone_dataset(args.source_dirs, args.target_dir)
