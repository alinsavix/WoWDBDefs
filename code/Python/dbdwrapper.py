#!/usr/bin/env python3
# simplify the results of parsing a dbd w/ the dbd parser library
import dataclasses
import hashlib
import os
import pickle
import re
import subprocess
import sys
from collections import UserDict, UserList, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import (Any, DefaultDict, Dict, List, Optional, Set,
                    Tuple, Type, TypeVar, Union)

import dbd
from ppretty import ppretty

BuildIdOrTuple = Union['BuildId', Tuple[int, int, int, int]]
DbdBuildOrRange = Union[dbd.build_version, Tuple[dbd.build_version, dbd.build_version]]

def get_file_hash(file: Union[str, Path]) -> str:
    file = Path(file)
    with file.open("rb") as f:
        h = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            h.update(chunk)
            chunk = f.read(8192)

    return h.hexdigest()


#  returns:   commit,hash, List[dirty], List[untracked]
def get_git_revision(defs_dir: Union[str, Path]) -> Tuple[str, List[str], List[str]]:
    """Get the current git version and a list of dirty and unknown tables, for metadata"""
    defs_dir = Path(defs_dir)
    if not defs_dir.exists() or not defs_dir.is_dir():
        raise ValueError(f"Invalid defs_dir: {defs_dir}")

    try:
        revstr = subprocess.check_output(
            ["git", "rev-parse", "--short=10", "HEAD"], cwd=defs_dir)
        rev = revstr.strip().decode("utf-8")

        dirty = []
        unknown = []

        dirtystr = subprocess.check_output(
            ["git", "status", "--untracked-files=no", "--porcelain"], cwd=defs_dir)

        for line in dirtystr.strip().decode("utf-8").split('\n'):
            if not line.endswith(".dbd"):
                continue

            if line[1] == "?":
                unknown.append(line[3:])
            elif line[1] == "M":
                dirty.append(line[3:])

        return rev, dirty, unknown

    except Exception as e:
        return None, [], []


# identifier for a specific column in a specific table
@dataclass(init=True, repr=True, eq=True, frozen=True)
class DbdColumnId:
    """
    An identifer for a specific table and column in various structures,
    for use as a key in various dictionaries
    """
    table: str
    column: str

    def __str__(self):
        return f"{self.table}.{self.column}"


# a stupid function to make it easier to replace FKs that point at SoundEntries
# with FKs that point at SoundKit instead, since that table was renamed at some
# point and the DBD format doesn't allow for versioning of that information.
def table_fk_namecheck(table: str) -> str:
    if table == "SoundEntries":
        return "SoundKit"

    # else
    return table


# Data structures for DBD data
# Most are direct analogues to the data structures returned by the dbd parser,
# but with less of the cruft that comes from that, and a bit of restructuring
# of the information to make it easier to work with. Most of these have a
# from_dbd() method that directly converts data from the matching parser
# structure.
@dataclass(init=True, repr=True, eq=True, frozen=True)
class BuildId:
    """
    A data class that encapsulates a WoW build number, in
    <major>.<minor>.<patch>.<build> format. Maps to the values used
    by dbd.definitions.builds[build_version]

    :raises ValueError: The build string supplied
    """
    major: int
    minor: int
    patch: int
    build: int

    @classmethod
    def from_dbd(cls, src: dbd.build_version) -> 'BuildId':
        return cls(
            major=src.major,
            minor=src.minor,
            patch=src.patch,
            build=src.build
        )

    @classmethod
    def from_tuple(cls, src: Tuple[int, int, int, int]) -> 'BuildId':
        return cls(*src)

    @classmethod
    def from_string(cls, src: str) -> 'BuildId':
        if not re.match(r'^\d+\.\d+\.\d+\.\d+$', src):
            raise ValueError(f"Invalid build id string: {src}")

        major, minor, patch, build = src.split('.')
        return cls(
            major=int(major),
            minor=int(minor),
            patch=int(patch),
            build=int(build)
        )

    #
    # dunder methods to allow for comparing BuildIds
    #
    @staticmethod
    def build_compare(buildid1: 'BuildId', buildid2: 'BuildId') -> int:
        if buildid1.major != buildid2.major:
            return buildid1.major - buildid2.major
        if buildid1.minor != buildid2.minor:
            return buildid1.minor - buildid2.minor
        if buildid1.patch != buildid2.patch:
            return buildid1.patch - buildid2.patch
        return buildid1.build - buildid2.build

    def __lt__(self, other: BuildIdOrTuple) -> bool:
        if not isinstance(other, BuildId):
            other = BuildId(*other)

        return BuildId.build_compare(self, other) < 0

    def __gt__(self, other: BuildIdOrTuple) -> bool:
        if not isinstance(other, BuildId):
            other = BuildId(*other)

        return BuildId.build_compare(self, other) > 0

    def __le__(self, other: BuildIdOrTuple) -> bool:
        if not isinstance(other, BuildId):
            other = BuildId(*other)

        return BuildId.build_compare(self, other) <= 0

    def __ge__(self, other: BuildIdOrTuple) -> bool:
        if not isinstance(other, BuildId):
            other = BuildId(*other)

        return BuildId.build_compare(self, other) >= 0

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}.{self.build}"


@dataclass(init=True, repr=True, frozen=True)
class BuildIdRange:
    """
    A data class that encapsulates a range of WoW build numbers. Includes a
    __contains__ method to allow for "if builid in buildrange" type logic.
    """
    min: BuildId
    max: BuildId

    def __contains__(self, item: BuildId) -> bool:
        return self.min <= item <= self.max

    def __str__(self) -> str:
        return f"{self.min}-{self.max}"

    @classmethod
    def from_dbd(cls, build: DbdBuildOrRange) -> 'BuildIdRange':
        if isinstance(build, dbd.build_version):
            bb = BuildId.from_dbd(build)
            r = cls(bb, bb)
        elif isinstance(build, tuple):
            bb = BuildId.from_dbd(build[0])
            bbb = BuildId.from_dbd(build[1])
            r = cls(bb, bbb)

        return r


class DbdBuilds(UserDict['BuildIdRange', 'DbdVersionedCols']):
    """
    A dict of build-specific DBD data, indexed by BuildId/BuildIdRange
    values. Do a lookup using a BuildId to find a build range into which
    that BuildId fits; do a lookup using a BuildIdRange to find an exact
    match to that range, if there is one.
    """
    # FIXME: consider using 'bisect' for faster lookups (see previous link)
    def __contains__(self, key: object) -> bool:
        # if it's a range, behave like a normal dict, and find only an
        # exact match to the range.
        if isinstance(key, BuildIdRange):
            if key in self.data:
                return True
            else:
                return False

        # if a specific version (or a string that might be one), check
        # to see if there's a match.
        if isinstance(key, str) or isinstance(key, BuildId):
            try:
                self.__getitem__(key)
                return True
            except KeyError:
                return False

        # else
        return False

    def __getitem__(self, key: object) -> 'DbdVersionedCols':
        # FIXME: wrong
        if isinstance(key, str):
            key = BuildId.from_dbd(dbd.build_version(key))

        # search through the dict, find a range that contains the specific
        # build requested.
        if isinstance(key, BuildId):
            for k, v in self.data.items():
                if key in k:
                    return v

        raise KeyError(key)

    @classmethod
    def from_dbd(cls, src: List[dbd.definitions], definitions: 'DbdColumnDefs') -> 'DbdBuilds':
        dbd_builds = cls()

        for dbd_def in src:
            # get our versioned column list once per def
            c = DbdVersionedCols.from_dbd(dbd_def.entries, definitions)

            # Now add all the builds, pointing at this specific def
            for build in dbd_def.builds:
                b = BuildIdRange.from_dbd(build)
                dbd_builds[b] = c

        # print(f"returning: {dbd_builds}")
        return dbd_builds


@dataclass(init=True, repr=True)
class DbdColumnDef:
    """
    Data class for the "global" definition of a single data column, matching
    with the top level .columns structure in the dbd parser output. Information
    found here is global to all builds.

    """
    name: str
    type: str  # Literal["string", "locstring", "int", "float"]
    is_confirmed_name: bool
    comment: Optional[str] = None
    fk: Optional['DbdColumnId'] = None

    @classmethod
    def from_dbd(cls, src: dbd.column_definition):
        return cls(
            name=src.name,
            type=src.type,
            is_confirmed_name=src.is_confirmed_name,
            comment=src.comment,
            fk=None if not src.foreign else DbdColumnId(
                table=table_fk_namecheck(str(src.foreign.table)),
                column=str(src.foreign.column)
            )
        )


class DbdColumnDefs(UserDict[str, DbdColumnDef]):
    """
    Data class holding an entire set of global column definitions (i.e. a
    table), indexed by column name.
    """
    @classmethod
    def from_dbd(cls, src: List[dbd.column_definition]) -> 'DbdColumnDefs':
        defs = cls()
        for d in src:
            defs[d.name] = DbdColumnDef.from_dbd(d)

        return defs


@dataclass(init=True, repr=True)
class DbdVersionedCol:
    """
    Data class for a single column definition for a specific build. This is
    where most of the "meat" of the data is.
    """
    name: str
    definition: 'DbdColumnDef'  # FIXME: Just roll this data in?
    annotation: Set[str] = dataclasses.field(default_factory=set)
    array_size: Optional[int] = None
    comment: Optional[str] = None
    int_width: Optional[int] = None
    is_unsigned: bool = True
    extra: Set[str] = dataclasses.field(default_factory=set)  # for user use

    @classmethod
    def from_dbd(cls, src: dbd.definition_entry, definition: 'DbdColumnDef') -> 'DbdVersionedCol':
        return cls(
            name=src.column,
            definition=definition,
            annotation=set(src.annotation),
            array_size=src.array_size,
            comment=src.comment,
            int_width=src.int_width,
            is_unsigned=src.is_unsigned
        )


class DbdVersionedCols(UserDict[str, DbdVersionedCol]):
    """
    Data class holding an entire set of versioned (build-specific) column
    definitions (i.e. a table), indexed by column name.
    """
    @classmethod
    def from_dbd(cls, src: List[dbd.definition_entry], definitions: DbdColumnDefs) -> 'DbdVersionedCols':
        cols = cls()
        for d in src:
            cols[d.column] = DbdVersionedCol.from_dbd(d, definitions[d.column])

        return cols


FKReferers = Dict[DbdColumnId, DbdVersionedCol]
FKReferents = Dict[DbdColumnId, FKReferers]


class DbdVersionedView(UserDict[str, 'DbdVersionedCols']):
    def get_fk_cols(self) -> FKReferents:
        """
        Look through all of a view's tables and find columns that are used as
        a reference for a foreign key, so that we can add an index on them
        later.

        :return: A dict containing the definitions of referring columns, indexed
        by referent.
        :rtype: FKReferents
        """
        fkreferents: DefaultDict[DbdColumnId, FKReferers] = defaultdict(dict)

        for table, data in self.data.items():
            for column, coldata in data.items():
                if coldata.definition.fk:
                    fkt = coldata.definition.fk.table
                    fkc = coldata.definition.fk.column

                    # at this point we have referent: fkt,fkc  and referer: table,column
                    referer_key = DbdColumnId(table, column)
                    referent_key = DbdColumnId(fkt, fkc)

                    # Only store the info if the thing we're referencing exists
                    if fkt in self.data and fkc in self.data[fkt]:
                        coldef = self.data[table][column]
                        fkreferents[referent_key][referer_key] = coldef

                    # else:
                    #     if fkt not in ["FileData", "SoundEntries"] and args.warn_missing_fk:
                    #         print(
                    #             f"WARNING: Foreign key for {table}.{column} references non-existent table or colfumn {fkt}.{fkc}", file=sys.stderr)

        return fkreferents


@dataclass(init=True, repr=True)
class DbdFileData:
    """
    Data class holding all of the parsed data from a single dbd file, for all
    build versions, plus the global definitions. This maps fairly directly
    to the top level data structure created by the dbd parser itself.
    """
    columns: 'DbdColumnDefs'
    definitions: 'DbdBuilds'

    @classmethod
    def from_dbd(cls, src: dbd.dbd_file):
        definitions = DbdColumnDefs.from_dbd(src.columns)
        return cls(
            columns=definitions,
            definitions=DbdBuilds.from_dbd(src.definitions, definitions)
        )


@dataclass
class DbdTableMeta:
    hash: str
    is_dirty: bool
    is_untracked: bool


@dataclass
class DbdMeta:
    rev: Optional[str]
    is_dirty: bool
    build: BuildId
    parsetime: int  # FIXME: what's the right type for this?


class DbdDirectory(UserDict[str, DbdFileData]):
    """
    Data class holding the parsed data for an entire directory full of dbd
    files (most commonly "all the dbd files") for all builds, indexed by
    table name.
    """
    def get_view(self, build: BuildIdOrTuple) -> DbdVersionedView:
        """
        Get a single view of the data for a specific BuildId. Tables and
        columns that do not exist in the requested build are not included.
        This is the data structure most software will work with directly,
        since most software only cares about a single build at once, and
        this is the simplest way to get at build-specific data without a lot
        of fluff.

        :param build: A BuildId structure or a tuple of (major, minor, patch, build)
        :type build: BuildIdOrTuple
        :return: A 'view' of the DBD data for the requested build.
        :rtype: DbdVersionedView
        """
        view = DbdVersionedView()

        if isinstance(build, tuple):
            build = BuildId.from_tuple(build)

        for table, tabledef in self.data.items():
            builds = tabledef.definitions
            if build in builds:
                view[table] = builds[build]

        return view


def load_dbd_file(filename: str) -> 'DbdFileData':
    """
    Parse a single DBD file using the DBD parser, and return the resulting
    data in a useful form.

    :param filename: filename of the DBD file to parse
    :type filename: str
    :return: The parsed data for a single dbd file
    :rtype: DbdFileData
    """
    dbf = dbd.parse_dbd_file(filename)
    return DbdFileData.from_dbd(dbf)


def load_dbd_directory(path: str) -> DbdDirectory:
    """
    Parse an entire directory of DBD files using the DBD parser, and return
    the resulting data in a useful form.

    :param path: The directory from which to parse all DBD files
    :type path: str
    :return: The parsed data for a directory of DBD files
    :rtype: DbdDirectory
    """
    dbds = DbdDirectory()

    for file in os.listdir(path):
        if file.endswith(".dbd"):
            dbds[file[:-len(".dbd")]] = load_dbd_file(os.path.join(path, file))

    return dbds


def load_dbd_directory_cached(path: str, skip_cache: bool = False,
                              refresh_cache: bool = False, silent: bool = False) -> 'DbdDirectory':
    """
    Load a directory of DBD files, and cache the result in a file. The cache file
    is placed in the DBD directory under the name ".dbd.pickle". If the cache is
    already present, and if skip_cache is False, the cache file is loaded and
    returned instead of re-parsing all the dbd files (for about a 50x speedup).
    The cache will *not* be automatically refreshed if stale; to force a refresh,
    use the refresh_cache parameter. A fresh cache will not be created if
    skip_cache is true.

    :param path: The directory from which DBD files will be parsed/loaded
    :type path: str
    :param skip_cache: don't load or write the on-disk cache, and re-parse all
    files instead, defaults to False
    :type skip_cache: bool, optional
    :param refresh_cache: force a refresh of the on-disk cache, re-parsing all
    files and caching the results, defaults to False
    :type refresh_cache: bool, optional
    :param silent: don't output status messages about loading/parsing/caching,
    defaults to False
    :type silent: bool, optional
    :return: The parsed data for a directory of DBD files
    :rtype: DbdDirectory
    """
    def optional_print(msg: str):
        if not silent:
            print(msg, file=sys.stderr)

    dbds = None
    pickle_path = os.path.join(path, ".dbd.pickle")

    if os.path.exists(pickle_path) and not skip_cache:
        if refresh_cache:
            optional_print("NOTICE: Refreshing DBD definition cache, not using existing")
        else:
            optional_print("NOTICE: Reading cached DBD definitions from disk")

            with open(pickle_path, "rb") as f:
                try:
                    dbds = pickle.load(f)
                except Exception as e:
                    optional_print("WARNING: failed to read DBD definition cache from disk")

    if dbds is None:
        optional_print(
            "NOTICE: No (valid) DBD definition cache available, directly parsing dbd definitions")

        dbds = load_dbd_directory(path)
        if not skip_cache:
            with open(pickle_path, "wb") as f:
                pickle.dump(dbds, f)

    return dbds


if __name__ == "__main__":
    # print(b == b)
    # print(b == (1, 2, 3, 4))
    # print(b < (1, 2, 3, 4))
    # print(b <= (1, 2, 3, 4))
    # print(b < (1, 2, 3, 5))
    # print(b > (1, 2, 3, 5))
    # print(b > (1, 2, 3, 4))
    # print(b >= (1, 2, 3, 4))
    # print(b > (1, 2, 3, 3))

    # c = BuildId(1, 2, 3, 5)
    # print(b == c)
    # print(b < c)

    # print(b == "bob")

    # dbf = dbd.parse_dbd_file("../../defs.mini/Spell.dbd")
    # f = DbdFileData.from_dbd(dbf)
    # f = parse_dbd_file("../../defs.mini/Spell.dbd")
    d = dbd.parse_dbd_directory("../../defs.mini")
    b = BuildId(3, 1, 2, 9768)

    v = d.get_view(b)
    print(ppretty(v))

    sys.exit(0)
