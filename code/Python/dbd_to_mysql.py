#! /usr/bin/env python3
# This is a kind of janky script to generte a mostly reasonable MySQL schema
# for given DBDs and build, including appropriate indexes and foreign keys.
# The way the bundled dbd library structures things makes this a bit more
# irritating than it could otherwise be.
#
# General usage: ./dbd_to_mysql.py | mysql -u username
#
# I'm not particularly proud of most of this code.  --A

import argparse
import csv
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple

import dbdwrapper as dbd
from dbdwrapper import DbdColumnId
from dbdanalyze import load_analysis, AnalysisData
from ppretty import ppretty

# from ppretty import ppretty


def errout(msg: str) -> None:
    print(msg, file=sys.stderr)


# column type def (maybe temporary)
@dataclass
class CTD:
    type: str
    is_unsigned: bool
    int_width: Optional[int]


# a ColDataRef is a dict that maps a given table.name to a definition
# FIXME: Maybe we should have that as a top-level data structure instead?
FKReferers = Dict[DbdColumnId, dbd.DbdVersionedCol]
FKReferents = Dict[DbdColumnId, FKReferers]


def analysis_check_unsigned(analysis: AnalysisData, columns: List[DbdColumnId]) -> bool:
    for col in columns:
        col_analysis = analysis.for_column(col)
        if col_analysis is None:
            continue

        if col_analysis.num_negative != 0:
            return False

    return True


# negative_one_is_null = set([
#     DbdColumnId("Faction", "ID"),
#     DbdColumnId("AnimKitBoneSet", "ID"),
#     DbdColumnId("AreaTable", "ID"),
#     DbdColumnId("CreatureType", "ID"),
#     DbdColumnId("Map", "ID"),
#     DbdColumnId("AnimationData", "ID"),
# ])
# negative_one_is_null = set()

# do our best to make sure referrers and referents are consistent in our
# schema, so mysql doesn't explode. The rules are (I think):
#
#  - if a column isn't a referent, take it as provided
#
#  - else (if a column is a referrent):
#    - if the types agree between all referers, use that type as the referrent
#    - if the types disagree, but all referers are unsigned or NEG_IS_NULL
#       - make the referent unsigned and not null
#       - make the referers unsigned and null/not_null
def fk_fixup_inner(table_name: str, table_data: dbd.DbdVersionedCols,
                   view: dbd.DbdVersionedView, fkcols: FKReferents, analysis: AnalysisData,
                   show_fixups: bool = False) -> None:
    def optional_print(msg: str):
        if show_fixups:
            print(msg, file=sys.stderr)

    for column_name, column_data in table_data.items():
        # my type and id
        referent_type = CTD(column_data.definition.type,
                            column_data.is_unsigned, column_data.int_width)
        referent_col = DbdColumnId(table_name, column_data.name)

        # testing
        # if referent_col in fkcols and referent_col in analysis:
        #     if "NEG_IS_NULL" in analysis[referent_col].tags:
        #         print(f"referent '{referent_col}' is NEG_IS_NULL")

        #     for referer_col, referer_coldata in fkcols[referent_col].items():
        #         if referer_col in analysis and "NEG_IS_NULL" in analysis[referer_col].tags:
        #             print(f"referer '{referer_col}' -> '{referent_col}' is NEG_IS_NULL")

        if referent_col not in fkcols:
            continue

        # else
        # if this column is referenced by another table/column's foreign key..
        # survey the number of referers that are signed/unsigned, and the
        # maximum integer width seen, so that we can derive the proper
        # integer type for the column in us (the referent)
        refs_signed = refs_unsigned = 0

        assert column_data.int_width is not None
        refs_maxbits = column_data.int_width

        a_referent = analysis.for_column(referent_col)
        if not a_referent:
            print(
                f"WARNING: referent '{referent_col}' has no analysis data", file=sys.stderr)

        mismatches: List[str] = []
        all_cols: List[str] = []
        for referer_col, referer_coldata in fkcols[referent_col].items():
            a_referer = analysis.for_column(referer_col)
            if not a_referer:
                print(
                    f"WARNING: referer '{referer_col}' -> '{referent_col}' is not analyzed", file=sys.stderr)
                continue

            # if we're supposed to ignore this FK, ignore this FK
            if "IGNORE_FK" in a_referer.tags:
                continue

            # if referer_coldata.is_unsigned:
            #     refs_unsigned += 1
            # elif referer_col in analysis and "NEG_IS_NULL" in analysis[referer_col].tags:
            #     referer_coldata.is_unsigned = True
            #     refs_unsigned += 1
            # else:
            #     refs_signed += 1
            if "UNSIGNED" in a_referer.tags or "NEG_IS_NULL" in a_referer.tags:
                referer_coldata.is_unsigned = True
                refs_unsigned += 1
            else:
                referer_coldata.is_unsigned = False
                refs_signed += 1

            assert referer_coldata.int_width is not None
            refs_maxbits = max(refs_maxbits, referer_coldata.int_width)

            referer_type = CTD(referer_coldata.definition.type,
                               referer_coldata.is_unsigned, referer_coldata.int_width)

            all_cols.append(
                f"          {referer_type}   referer: {referer_col.table}.{referer_col.column}")

            if referent_type != referer_type:
                mismatches.append(
                    f"          {referer_type}   referer: {referer_col.table}.{referer_col.column}")

        # no mismatches, carry on
        if len(mismatches) == 0:
            continue

        # deal with mismatches
        # special case -- known case where the referrer is -1 when it's not
        # referencing a row for its FK, so we can just set it to NULL
        # if referent_col in negative_one_is_null:
        #     optional_print(
        #         f"FIXUP: referent {table_name}.{column_name} -> unsigned: True, bits: {refs_maxbits} (special case)")

        #     # Make our id unsigned
        #     column_data.is_unsigned = False  # FIXME: make actually unsigned!

        #     # and make our bit width consistent all the way around
        #     column_data.int_width = refs_maxbits
        #     for referer_col, referer_coldata in fkcols[referent_col].items():
        #         optional_print(f"      {referer_col.table}.{referer_col.column}")
        #         referer_coldata.is_unsigned = False  # FIXME: hax
        #         referer_coldata.int_width = refs_maxbits
        # else
        if refs_signed > 0 and refs_unsigned == 0:
            # everything is signed
            optional_print(
                f"FIXUP: referent {table_name}.{column_name} -> signed: False, bits: {refs_maxbits}")

            # Make our id signed
            column_data.is_unsigned = False

            # and make our bit width consistent all the way around
            column_data.int_width = refs_maxbits
            for referer_col, referer_coldata in fkcols[referent_col].items():
                optional_print(f"      {referer_col.table}.{referer_col.column}")
                referer_coldata.int_width = refs_maxbits

        # everything is unsigned
        elif refs_unsigned > 0 and refs_signed == 0:
            optional_print(
                f"FIXUP: referent {table_name}.{column_name} -> unsigned: True, bits: {refs_maxbits}")

            # Make our id signed
            column_data.is_unsigned = True

            # and make our bit width consistent all the way around
            column_data.int_width = refs_maxbits
            for referer_col, referer_coldata in fkcols[referent_col].items():
                optional_print(f"      {referer_col.table}.{referer_col.column}")
                referer_coldata.int_width = refs_maxbits

        # The tables as listed in the DBDefs aren't consistent... how 'bout
        # in the analysis?
        elif analysis_check_unsigned(analysis, list(fkcols[referent_col].keys())):
            optional_print(
                f"FIXUP: referent {table_name}.{column_name} -> unsigned: False, bits: {refs_maxbits} (from analysis)")

            # Make our id unsigned
            column_data.is_unsigned = True

            # and make our bit width consistent all the way around
            column_data.int_width = refs_maxbits
            for referer_col, referer_coldata in fkcols[referent_col].items():
                optional_print(f"      {referer_col.table}.{referer_col.column}")
                referer_coldata.is_unsigned = True
                referer_coldata.int_width = refs_maxbits

        # special case -- signed, but only because of -1 values, which
        # can be null (and we can take care of on input)
        else:
            # we have mismatches we can't fix, bitch about it
            print(
                f"MISMATCH: {referent_type}   referent: {table_name}.{column_data.name} (signed: {refs_signed}  unsigned: {refs_unsigned})", file=sys.stderr)

            for col in all_cols:
                print(col, file=sys.stderr)


def fk_fixup(view: dbd.DbdVersionedView, fkcols: FKReferents, analysis: AnalysisData,
             show_fixups: bool = False) -> None:
    for table_name, table_data in view.items():
        fk_fixup_inner(table_name, table_data, view, fkcols, analysis, show_fixups)


int_sizemap = {
    8: "TINYINT",
    16: "SMALLINT",
    32: "INT",
    64: "BIGINT",
}

# int_signmap = {
#     False: "",
#     True: " UNSIGNED",
# }

def coltype_one(args: argparse.Namespace, dbname: str, tablename: str, colname: str,
                column: dbd.DbdVersionedCol, analysis: AnalysisData) -> Tuple[str, str, Optional[str]]:
    # string to write the column definition into
    defstr: Optional[str] = None

    # FIXME: move comment/annotation handling to outer function
    # create annotation strings (for adding comments)
    annotations = ""
    if len(column.annotation) > 0:
        annotations = "(annotations: " + ", ".join(sorted(column.annotation)) + ")"

    # create comments for a the column. Theoretically both the column def and
    # the build def could have a comment, but most/all seem to be on the
    # global column def, so ... just use that.
    comments = ""
    if column.definition.comment is not None:
        comments = column.definition.comment.replace("\\", "\\\\")
        comments = comments.replace("'", "\\'")

        if len(comments) > 500:
            comments = "see comment in .dbd file"

    sql_comment_string = ""
    if annotations or comments:
        if annotations and comments:  # this feels sloppy
            sql_comment_string = f" /*! COMMENT '{comments} {annotations}' */"
        else:
            sql_comment_string = f" /*! COMMENT '{comments}{annotations}' */"

    a_column = analysis.for_column(DbdColumnId(tablename, colname))
    if a_column is None:
        print(f"WARNING: no analysis for {tablename}.{colname}", file=sys.stderr)

    nullstr = ""
    if a_column is not None and "NOT_NULL" in a_column.tags:
        nullstr = " NOT NULL"

    # create the type string
    if column.definition.type == "int":
        assert column.int_width is not None

        # FIXME: maybe add a comment w/ the original width
        int_string = int_sizemap.get(column.int_width, "INT")
        if column.is_unsigned:
            defstr = f"{int_string} UNSIGNED{nullstr}{sql_comment_string}"
        else:
            defstr = f"{int_string}{nullstr}{sql_comment_string}"

    elif column.definition.type == "float":
        defstr = f"FLOAT{nullstr}{sql_comment_string}"

    elif column.definition.type in ["string", "locstring"]:
        defstr = f"MEDIUMTEXT{nullstr}{sql_comment_string}"

    else:
        raise ValueError(f"Unknown column type: {column.definition.type}")

    # The column def
    column_return = f"  `{colname}` {defstr}"

    # The index def (which may or may not be used)
    if column.definition.type == "int" or column.definition.type == "float":
        index_return = f"CREATE INDEX `{tablename}_{colname}_idx` ON `{tablename}` (`{colname}`);"
    elif not args.no_fulltext:  # string
        index_return = f"CREATE /*! FULLTEXT */ INDEX `{tablename}_{colname}_idx` ON `{tablename}` (`{colname}`);"
    else:
        index_return = f"CREATE INDEX `{tablename}_{colname}_idx` ON `{tablename}` (`{colname}` /*! (64) */);"

    # make our FK string, if there's a FK.
    #
    # Unlike the others, we don't generate the constraint string at all (rather
    # than creating it and not using if it isn't needed) because if there's not
    # actually a FK, we can't even create a valid FK creation string.
    fk_return = None
    if column.definition.fk:
        fk = column.definition.fk
        # fk_return = f"  ADD CONSTRAINT `{tablename}_{colname}` FOREIGN KEY (`{colname}`) REFERENCES `{dbname}`.`{fk.table}` (`{fk.column}`)"
        fk_return = f"  ADD CONSTRAINT `{tablename}_{colname}` FOREIGN KEY (`{colname}`) REFERENCES `{fk.table}` (`{fk.column}`)"

    return (column_return, index_return, fk_return)


def coltype_strings(args: argparse.Namespace, dbname: str, tablename: str,
                    column: dbd.DbdVersionedCol, analysis: AnalysisData) -> Tuple[List[str], List[str], List[str]]:
    """
    Generate the type string for a given column, based on DBD data

    :param column: A versioned column struct from DBD data
    :type column: dbd.DbdVersionedCol
    :return: A tuple of lists of strings, with possible column definitions, index
    definitions, and foreign key definitions for the column in question. These
    will always be generated, but should only be used by the caller if/when they
    are appropriate and needed
    :rtype: Tuple[List[str], List[str], List[str]]
    """

    if column.array_size is None or column.array_size == 1:
        # single-element arrays are just a single column
        defstr, idxstr, fkstr = coltype_one(
            args, dbname, tablename, column.name, column, analysis)
        return [defstr], [idxstr], [fkstr] if fkstr is not None else []

    # else
    defarr = []
    idxarr = []
    fkarr = []
    for i in range(column.array_size):
        defstr, idxstr, fkstr = coltype_one(
            args, dbname, tablename, f"{column.name}[{i}]", column, analysis)
        defarr.append(defstr)
        idxarr.append(idxstr)
        if fkstr is not None:
            fkarr.append(fkstr)

    return defarr, idxarr, fkarr


def dumpdbd(args: argparse.Namespace, dbname: str, tablename: str,
            all_data: dbd.DbdVersionedView, table_data: dbd.DbdVersionedCols,
            fkcols: FKReferents, analysis: AnalysisData) -> List[str]:
    """
    Take the parsed data, the build-specific view, and a list of foreign keys,
    and generate a bunch of MySQL `CREATE TABLE` statements. Returns a list of
    statements to be executed in an `ALTER TABLE` after all of the tables are
    created, since you can't do things like create foreign keys until
    the tables they reference exist.

    :param dbname: [description]
    :type dbname: str
    :param table: [description]
    :type table: str
    :param all_data: [description]
    :type all_data: dbd.DbdVersionedView
    :param table_data: [description]
    :type table_data: dbd.DbdVersionedCols
    :param fkcols: [description]
    :type fkcols: FKColumnRefs
    :return: [description]
    :rtype: List[str]
    """

    create_lines: List[str] = []  # lines for things we need to create
    index_lines: List[str] = []  # lines for indexes
    deferred: List[str] = []  # lines to execute in an `ALTER` at the very end

    # So that we can find our PK as we iterate through the table
    id_col = None

    # cycle through every column in our view and generate SQL
    for _, column in table_data.items():
        referent_type = CTD(column.definition.type, column.is_unsigned, column.int_width)
        referent_col = DbdColumnId(tablename, column.name)

        col_create, col_index, col_fk = coltype_strings(
            args, dbname, tablename, column, analysis)

        # is this column our id column?
        if "id" in column.annotation:
            id_col = column.name

            # make the id column first -- id cols can't be arrays, so just
            # use the first element of the returned list
            create_lines.insert(0, col_create[0])
        else:
            # not id, just append to the list
            create_lines.extend(col_create)

        # If this column is referenced by another table/column's foreign key,
        # generate an index for it (unless this column is already the PK).
        # Indexes get kept until the end so that we can stuff them at the
        # bottom of the `CREATE` block
        did_index = False
        if referent_col in fkcols and column.name != id_col:
            did_index = True
            index_lines.extend(col_index)

        # Just index all the string fields, since it's useful
        if column.definition.type in ["string", "locstring"]:
            did_index = True
            index_lines.extend(col_index)

        analysis_data = analysis.for_column(referent_col)

        # If we have a FK referencing another table, set that up too
        if column.definition.fk is not None and "IGNORE_FK" not in analysis_data.tags:
            fk_table = str(column.definition.fk.table)
            fk_col = str(column.definition.fk.column)

            # FileData as a table no longer exists
            # FIXME: Maybe we should make it exist, to make queries against
            # it easier?
            if fk_table == "FileData":
                continue

            # if we have a FK, but the table pointed to doesn't exist, and we're
            # not already indexed (because we have an index, or we're the PK), make
            # an index for use as a possible grouping key
            if fk_table not in all_data:
                if column.name != id_col and not did_index:
                    # index_lines.append("-- indexed as a group key")
                    index_lines.extend(col_index)
            else:
                # deferred.append("-- normal FK")
                if not args.no_fks:
                    deferred.extend(col_fk)


    # Occasional things might not have a PK annotated, so make sure we still
    # have a PK if not
    if id_col is None:
        create_lines.insert(0, "  _id INT UNSIGNED NOT NULL")
        create_lines.append("  PRIMARY KEY (_id)")
    else:
        create_lines.append(f"  PRIMARY KEY({id_col})")

    # Add in any index creation we had stored for now
    # create_lines.extend(index_lines)

    # Generate the actual `CREATE` statement
    # FIXME: include comment w/ layout hash(s), git source info, and file comments
    print(f"\nCREATE TABLE /*! IF NOT EXISTS */ `{tablename}` (")
    # print(f"\nCREATE TABLE IF NOT EXISTS `{dbname}`.`{tablename}` (")
    print(",\n".join(create_lines))
    print(") /*! ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci */;")

    if len(index_lines) > 0:
        print("\n".join(index_lines))

    return deferred


def get_git_revision() -> Tuple[Optional[str], bool]:
    """Get the current git version and a dirty flag, for metadata"""

    try:
        revstr = subprocess.check_output(["git", "rev-parse", "--short=10", "HEAD"])
        rev = revstr.strip().decode("utf-8")

        isdirty = False
        dirtystr = subprocess.check_output(
            ["git", "status", "--untracked-files=no", "--porcelain"]).strip()
        if dirtystr:
            isdirty = True

        return rev, isdirty
    except Exception as e:
        return None, False


def build_string_regex(arg_value, pat=re.compile(r"^\d+\.\d+\.\d+\.\d+$")) -> str:
    if not pat.match(arg_value):
        raise argparse.ArgumentTypeError("invalid build string (try e.g. '9.1.5.41488')")

    return arg_value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--definitions", "--defs", dest="definitions", type=str, action='store',
        default="../../definitions", help="location of .dbd files")
    parser.add_argument(
        "--analysis", dest="analysis_file", type=str, action='store',
        default="analysis.csv", help="extra column analysis data")
    parser.add_argument(
        "--build", dest="build", type=build_string_regex, default="9.2.0.42423",
        help="full build number to use for parsing")
    parser.add_argument(
        "--dbname", dest="dbname", type=str, default="wowdbd",
        help="name of MySQL database to generate create statements for")
    parser.add_argument(
        "--no-cache", dest="no_cache", action='store_true', default=False,
        help="don't use or create cached data file")
    parser.add_argument(
        "--refresh-cache", dest="refresh_cache", action='store_true', default=False,
        help="re-parse data and refresh cached data file")
    parser.add_argument(
        "--warn-missing-fk", dest="warn_missing_fk", action='store_true', default=False,
        help="warn about missing FK referents")
    parser.add_argument(
        "--show-fixups", dest="show_fixups", action='store_true', default=False,
        help="show datatype and signedness fixups made to columns")
    parser.add_argument(
        "--no-git", dest="no_git", action='store_true', default=False,
        help="don't include git revision in metadata")
    parser.add_argument(
        "--no-fulltext", dest="no_fulltext", action='store_true', default=False,
        help="don't create fulltext indexes for string columns")
    parser.add_argument(
        "--no-fks", "--no-foreign-keys", dest="no_fks", action='store_true', default=False,
        help="don't create foreign key constraints")

    # --only is disabled for now, since using it will cause FKs to be wrong
    # if it's used to try to generate an updated schema w/o parsing everything
    # parser.add_argument(
    #     "--only", dest="only", type=str, action='append',
    #     help="parse only these tables")

    args = parser.parse_args()

    # dbds = {}
    # if args.only:
    #   for table in args.only:
    #     dbds[table] = dbd.parse_dbd_file(os.path.join(args.definitions, "{}{}".format(table, dbd.file_suffix)))
    # else:
    #   dbds = dbd.parse_dbd_directory(args.definitions)

    dbds = dbd.load_dbd_directory_cached(
        args.definitions, skip_cache=args.no_cache, refresh_cache=args.refresh_cache)
    # print(f"meta: {dbds.meta}")

    build = dbd.BuildId.from_string(args.build)
    view = dbds.get_view(build)
    fkcols = view.get_fk_cols()
    analysis = load_analysis(args.analysis_file)
    fk_fixup(view, fkcols, analysis, show_fixups=args.show_fixups)

    # if "FileData" not in view:
    #     print("No FileData table found")

    # print(f"keys: {dbds.keys()}")
    # print(ppretty(dbds))
    # sys.exit(0)
    metasql = (
        "\nCREATE TABLE IF NOT EXISTS `_dbd_meta` (\n"
        "  `rev` CHAR(40),\n"
        "  `dirty` TINYINT UNSIGNED,\n"
        "  `build` VARCHAR(16) NOT NULL,\n"
        "  `parsedate` DATETIME\n"
        ") /*! ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci */;\n"
        "\n"
    )

    if dbds.meta.rev is None:
        print("WARNING: Couldn't find git revision, full dbd metadata will be unavailable", file=sys.stderr)

    if dbds.meta.rev is not None:
        metasql += (
            f"INSERT INTO `_dbd_meta` (`rev`, `dirty`, `build`, `parsedate`)\n"
            f"  VALUES ('{dbds.meta.rev}', {1 if dbds.meta.is_dirty else 0}, '{build}', FROM_UNIXTIME({dbds.meta.parsetime}));\n\n"
        )
    else:
        metasql += (
            "INSERT INTO `_dbd_meta` (`rev`, `dirty`, `build`, `parsedate`)\n"
            f"  VALUES (NULL, NULL, '{build}', NULL);\n\n"
        )

    metasql += (
        "\nCREATE TABLE IF NOT EXISTS `_dbd_table_meta` (\n"
        "  `table` VARCHAR(64) NOT NULL,\n"
        "  `dirty` TINYINT UNSIGNED NOT NULL,\n"
        "  `untracked` TINYINT UNSIGNED NOT NULL,\n"
        "  `hash` CHAR(32) NOT NULL,\n"
        "  PRIMARY KEY(`table`)\n"
        ") /*! ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci */;\n"
        "\n"
    )

    metasql += (
        "\nCREATE TABLE IF NOT EXISTS `_dbd_data_meta` (\n"
        "  `table` VARCHAR(64) NOT NULL,\n"
        "  `path` VARCHAR(256) NOT NULL,\n"
        "  `hash` CHAR(32) NOT NULL,\n"
        "  PRIMARY KEY(`table`)\n"
        ") /*! ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci */;\n"
        "\n"
    )


    print("-- these will error on sqlite, but otherwise fine")
    print(f"DROP DATABASE IF EXISTS `{args.dbname}`;")
    print(f"CREATE DATABASE `{args.dbname}`;")
    print(f"USE `{args.dbname}`;")
    print(metasql)

    # deferred statements to add to `ALTER` at the end
    deferred = {}

    for tablename, data in sorted(view.items()):
        deferred[tablename] = dumpdbd(args, args.dbname, tablename,
                                      view, data, fkcols, analysis)

        tablemeta = dbds[tablename].meta
        assert tablemeta is not None
        print(
            f"INSERT INTO `_dbd_table_meta` (`table`, `dirty`, `untracked`, `hash`)\n"
            f"  VALUES ('{tablename}', {1 if tablemeta.is_dirty else 0}, {1 if tablemeta.is_untracked else 0}, '{tablemeta.hash}');\n\n"
        )

    for table, lines in deferred.items():
        if len(lines) > 0:
            # print(f"\nALTER TABLE `{args.dbname}`.`{table}`")
            print(f"\nALTER TABLE `{table}`")
            print(",\n".join(lines))
            print(";")

    return 0


if __name__ == "__main__":
    sys.exit(main())
