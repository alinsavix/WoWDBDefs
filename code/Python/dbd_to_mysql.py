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
import pickle
import re
import sys
from dataclasses import dataclass
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple

import dbdwrapper
from ppretty import ppretty

# from ppretty import ppretty


def errout(msg: str) -> None:
    print(msg, file=sys.stderr)

@dataclass(init=True, repr=True, eq=True, frozen=True)
class FKColumn:
    table: str
    column: str


# column type def (maybe temporary)
@dataclass
class CTD:
    type: str
    is_unsigned: bool
    int_width: Optional[int]


# a ColDataRef is a dict that maps a given table.name to a definition
# FIXME: Maybe we should have that as a top-level data structure instead?
FKReferers = Dict[FKColumn, dbdwrapper.DbdVersionedCol]
FKReferents = Dict[FKColumn, FKReferers]


AnalysisData = Dict[FKColumn, Dict[str, Any]]
def get_analysis(filename: str) -> AnalysisData:
    data: Dict[FKColumn, Any] = {}
    with open(filename, newline="") as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            if "table" in row and len(row["table"]) > 0:
                colid = FKColumn(row["table"], row["column"])
                data[colid] = row
    return data


def analysis_check_unsigned(analysis: AnalysisData, columns: List[FKColumn]) -> bool:
    for col in columns:
        if col not in analysis:
            continue
        if "num_neg" not in analysis[col]:
            continue
        if analysis[col]["num_neg"] != "0":
            return False
    return True


# FIXME: should this be in dbdwrapper?
def get_fk_cols(args: argparse.Namespace, view: dbdwrapper.DbdVersionedView) -> FKReferents:
    """
    Look through all our tables and find columns that are used as a reference
    for a foreign key, so that we can add an index on them later.

    :param dbds: The data structure from a dbd directory parsed by dbdwrapper
    :type dbds: dbdwrapper.DbdDirectory
    :return: A set containing the names of all FKs, in the format of `table.column`
    :rtype: Set[str]
    """
    from collections import defaultdict
    fkreferents: DefaultDict[FKColumn, FKReferers] = defaultdict(dict)

    for table, data in view.items():
        for column, coldata in data.items():
            if coldata.definition.fk:
                fkt = coldata.definition.fk.table
                fkc = coldata.definition.fk.column

                # at this point we have referrent: fkt,fkc  and referer: table,column
                referer_key = FKColumn(table, column)
                referent_key = FKColumn(fkt, fkc)

                # Only store the info if the thing we're referencing exists
                if fkt in view and fkc in view[fkt]:
                    # print(f"ref {table}.{column} -> {fkt}.{fkc}", file=sys.stderr)
                    coldef = view[table][column]
                    fkreferents[referent_key][referer_key] = coldef
                    # print(
                    #     f"    stored to fkreferents[{referent_key}][{referer_key}]", file=sys.stderr)
                    # print(f"    data stored is: {coldef}", file=sys.stderr)
                else:
                    # print(f"nonexist ref {table}.{column} -> {fkt}.{fkc}", file=sys.stderr)
                    if fkt not in ["FileData", "SoundEntries"] and not args.no_warn_missing_fk:
                        print(
                            f"WARNING: Foreign key for {table}.{column} references non-existent table or colfumn {fkt}.{fkc}", file=sys.stderr)

    # from ppretty import ppretty
    # print(f"refs thing: {ppretty(fkreferents)}")
    return fkreferents


negative_one_is_null = set([
    FKColumn("Faction", "ID"),
    FKColumn("AnimKitBoneSet", "ID"),
    FKColumn("AreaTable", "ID"),
    FKColumn("CreatureType", "ID"),
    FKColumn("Map", "ID"),
    FKColumn("AnimationData", "ID"),
])
# negative_one_is_null = set()

def fk_fixup_inner(table_name: str, table_data: dbdwrapper.DbdVersionedCols,
                   view: dbdwrapper.DbdVersionedView, fkcols: FKReferents, analysis: AnalysisData) -> None:
    # print(f"THANG: {table_name}")

    for column_name, column_data in table_data.items():
        # only care about making sure the id column matches the referer types
        # if "id" not in column_data.annotation:
        #     continue

        # If this column is referenced by another table/column's foreign key,
        # generate an index for it (unless this column is already the PK).
        # Indexes get kept until the end so that we can stuff them at the
        # bottom of the `CREATE` block

        # my type and id
        referent_type = CTD(column_data.definition.type,
                            column_data.is_unsigned, column_data.int_width)
        referent_col = FKColumn(table_name, column_data.name)

        # if referent_col not in fkcols:
        #     print(f"THANG: {referent_col} not in fkcols")

        # if this column is referenced by another table/column's foreign key,
        if referent_col in fkcols:
            refs_signed = refs_unsigned = 0

            assert column_data.int_width is not None
            refs_maxbits = column_data.int_width

            mismatches: List[str] = []
            all_cols: List[str] = []
            for referer_table, referer_coldata in fkcols[referent_col].items():
                if referer_coldata.is_unsigned:
                    # print(
                    #     f"unsigned: {referer_table.table}.{referer_table.column}", file=sys.stderr)
                    refs_unsigned += 1
                else:
                    # print(
                    #     f"signed: {referer_table.table}.{referer_table.column}", file=sys.stderr)
                    refs_signed += 1

                assert referer_coldata.int_width is not None
                refs_maxbits = max(refs_maxbits, referer_coldata.int_width)

                referer_type = CTD(referer_coldata.definition.type,
                                   referer_coldata.is_unsigned, referer_coldata.int_width)

                all_cols.append(
                    f"          {referer_type}   referer: {referer_table.table}.{referer_table.column}")
                if referent_type != referer_type:
                    mismatches.append(
                        f"          {referer_type}   referer: {referer_table.table}.{referer_table.column}")

            # no mismatches, carry on
            if len(mismatches) == 0:
                continue

            # print(f"THING THING: {table_name}.{column_data.name}")
            # if (table_name, column_data.name) == ("SpellVisualMissile", "ID"):
            #     print("THING:")
            #     print(ppretty(view[table_name][column_data.name]))

            # deal with mismatches
            if referent_col in negative_one_is_null:
                print(
                    f"FIXUP: referent {table_name}.{column_name} -> unsigned: True, bits: {refs_maxbits} (special case)", file=sys.stderr)

                # Make our id unsigned
                column_data.is_unsigned = False  # hacked

                # and make our bit width consistent all the way around
                column_data.int_width = refs_maxbits
                for referer_table, referer_coldata in fkcols[referent_col].items():
                    print(f"      {referer_table.table}.{referer_table.column}",
                          file=sys.stderr)
                    referer_coldata.is_unsigned = False  # FIXME: hax
                    referer_coldata.int_width = refs_maxbits

            elif refs_signed > 0 and refs_unsigned == 0:
                print(
                    f"FIXUP: referent {table_name}.{column_name} -> signed: False, bits: {refs_maxbits}", file=sys.stderr)

                # Make our id signed
                column_data.is_unsigned = False

                # and make our bit width consistent all the way around
                column_data.int_width = refs_maxbits
                for referer_table, referer_coldata in fkcols[referent_col].items():
                    print(f"      {referer_table.table}.{referer_table.column}",
                          file=sys.stderr)
                    referer_coldata.int_width = refs_maxbits

            elif refs_unsigned > 0 and refs_signed == 0:
                print(
                    f"FIXUP: referent {table_name}.{column_name} -> unsigned: True, bits: {refs_maxbits}", file=sys.stderr)

                # Make our id signed
                column_data.is_unsigned = True

                # and make our bit width consistent all the way around
                column_data.int_width = refs_maxbits
                for referer_table, referer_coldata in fkcols[referent_col].items():
                    print(f"      {referer_table.table}.{referer_table.column}",
                          file=sys.stderr)
                    referer_coldata.int_width = refs_maxbits

            # The tables as listed in the DBDefs aren't consistent... how 'bout
            # in the analysis?
            elif analysis_check_unsigned(analysis, list(fkcols[referent_col].keys())):
                print(
                    f"FIXUP: referent {table_name}.{column_name} -> unsigned: False, bits: {refs_maxbits} (from analysis)", file=sys.stderr)

                # Make our id unsigned
                column_data.is_unsigned = True

                # and make our bit width consistent all the way around
                column_data.int_width = refs_maxbits
                for referer_table, referer_coldata in fkcols[referent_col].items():
                    print(f"      {referer_table.table}.{referer_table.column}",
                          file=sys.stderr)
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


def fk_fixup(view: dbdwrapper.DbdVersionedView, fkcols: FKReferents, analysis: AnalysisData) -> None:
    for table_name, table_data in view.items():
        fk_fixup_inner(table_name, table_data, view, fkcols, analysis)


int_sizemap = {
    8: "TINYINT",
    16: "SMALLINT",
    # 8: "INT",
    # 16: "INT",
    32: "INT",
    64: "BIGINT",
}

int_signmap = {
    False: "",
    True: " UNSIGNED",
}

def coltype_string(dbname: str, tablename: str, column: dbdwrapper.DbdVersionedCol) -> Tuple[List[str], List[str], List[str]]:
    """
    Generate the type string for a given column, based on DBD data

    :param column: A versioned column struct from DBD data
    :type column: dbdwrapper.DbdVersionedCol
    :return: A string that can be used in a column of a `CREATE TABLE` statement
    :rtype: str
    """

    # string to write the column definition into, so that we can array it if we
    # need to
    defstr: Optional[str] = None

    #
    # create annotation strings (for adding comments)
    annotations = ""
    if len(column.annotation) > 0:
        annotations = "(annotations: " + ", ".join(sorted(column.annotation)) + ")"

    #
    # create comments for a the column
    # Theoretically column def or entry def could have a comment,
    # but most/all seem to be on the global column def, so ... just use that.
    comments = ""
    if column.definition.comment is not None:
        comments = column.definition.comment.replace("\\", "\\\\")
        comments = comments.replace("'", "\\'")

        if len(comments) > 500:
            comments = "see comment in .dbd file"

    sql_comment_string = ""
    if annotations or comments:
        if annotations and comments:  # feels sloppy
            sql_comment_string = f" COMMENT '{comments} {annotations}'"
        else:
            sql_comment_string = f" COMMENT '{comments}{annotations}'"

    #
    # create the type string
    if column.definition.type == "int":
        assert column.int_width is not None
        # FIXME: add a comment w/ the original width
        int_string = int_sizemap.get(column.int_width, "INT")
        if column.is_unsigned:
            # return f"  `{column.name}` {int_string} UNSIGNED{sql_comment_string}"
            defstr = f"{int_string} UNSIGNED{sql_comment_string}"
        else:
            defstr = f"{int_string}{sql_comment_string}"

    elif column.definition.type == "float":
        defstr = f"FLOAT{sql_comment_string}"

    elif column.definition.type in ["string", "locstring"]:
        defstr = f"MEDIUMTEXT{sql_comment_string}"

    else:
        raise ValueError(f"Unknown column type: {column.definition.type}")

    #
    # make our list of 'create' strings, with or without arrays
    if column.array_size is None or column.array_size < 2:
        column_return = [f"  `{column.name}` {defstr}"]
    else:
        column_return = [
            f"  `{column.name}[{i}]` {defstr}" for i in range(0, column.array_size)]

    #
    # make our list of index strings, with or without arrays
    if column.definition.type == "int" or column.definition.type == "float":
        if column.array_size is None or column.array_size < 2:
            index_return = [f"  INDEX `{column.name}_idx` (`{column.name}`)"]
        else:
            index_return = [
                f"  INDEX `{column.name}_{i}_idx` (`{column.name}[{i}]`)" for i in range(0, column.array_size)
            ]
    else:  # string
        if column.array_size is None or column.array_size < 2:
            index_return = [f"  FULLTEXT `{column.name}_idx` (`{column.name}`)"]
        else:
            index_return = [
                f"  FULLTEXT `{column.name}_{i}_idx` (`{column.name}[{i}]`)" for i in range(0, column.array_size)
            ]

    #
    # make our list of FK strings, with or without arrays, if there's a FK.
    #
    # Unlike the others, we don't generate the constraint string at all (rather
    # than creating it and not using if it isn't needed) because if there's not
    # actually a FK, we can't even create a valid FK creation string.
    fk_return = []
    if column.definition.fk:
        fk = column.definition.fk
        if column.array_size is None or column.array_size < 2:
            fk_return = [
                f"  ADD CONSTRAINT `{tablename}_{column.name}` FOREIGN KEY (`{column.name}`) REFERENCES `{dbname}`.`{fk.table}` (`{fk.column}`)"
            ]
        else:
            fk_return = [
                f"  ADD CONSTRAINT `{tablename}_{column.name}_{i}` FOREIGN KEY (`{column.name}[{i}]`) REFERENCES `{dbname}`.`{fk.table}` (`{fk.column}`)" for i in range(0, column.array_size)
            ]

    return (column_return, index_return, fk_return)

    # f"  INDEX `{column.name}_idx` (`{column.name}`)"
    # f"  FULLTEXT `{column.name}_idx` (`{column.name}`)"
    #   ADD CONSTRAINT `CharShipment_ContainerID` FOREIGN KEY (`ContainerID`) REFERENCES `wowdbd`.`CharShipmentContainer` (`ID`),



def dumpdbd(dbname: str, tablename: str, all_data: dbdwrapper.DbdVersionedView,
            table_data: dbdwrapper.DbdVersionedCols, fkcols: FKReferents) -> List[str]:
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
    :type all_data: dbdwrapper.DbdVersionedView
    :param table_data: [description]
    :type table_data: dbdwrapper.DbdVersionedCols
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
        # id column?
        if "id" in column.annotation:
            id_col = column.name

        referent_type = CTD(column.definition.type, column.is_unsigned, column.int_width)
        referent_col = FKColumn(tablename, column.name)
        col_create, col_index, col_fk = coltype_string(dbname, tablename, column)

        # We always want to create the column
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

        # If we have a FK referencing another table, set that up too
        if column.definition.fk is not None:
            fk_table = str(column.definition.fk.table)
            fk_col = str(column.definition.fk.column)

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
                deferred.extend(col_fk)


    # Occasional things might not have a PK annotated, so make sure we still
    # have a PK if not
    if id_col is None:
        create_lines.insert(0, "  _id INT UNSIGNED NOT NULL")
        create_lines.append("  PRIMARY KEY (_id)")
    else:
        create_lines.append(f"  PRIMARY KEY({id_col})")

    # Add in any index creation we had stored for now
    create_lines.extend(index_lines)

    # Generate statements for appropriate foreign keys, which will be returned
    # from this function to be added at the end after all the tables have been
    # created.
    # for _, column in table_data.items():
    #     if column.definition.fk is not None:
    #         fk_table = str(column.definition.fk.table)
    #         fk_col = str(column.definition.fk.column)

    # Don't complain about the FileData table not existing, since
    # everything just uses FDIDs directly now, but the FK annotation
    # still exists because it's a part of the defs structure that isn't
    # versioned
    # if fk_table == "FileData":
    #     continue

    # This was an across the board change, just Make It Work™
    # if fk_table == "SoundEntries":
    #     fk_table = "SoundKit"

    # If we don't have the referenced table, index this for use as a
    # possible grouping key, if it's not already indexed
    # if referent_col not in fkcols and fk_table not in all_data:
    #     print(f"no referer to {tablename}.{column.name}", file=sys.stderr)
    #     create_lines.extend(col_index)
    # index_lines.append(f"  INDEX `{column.name}_group_idx` (`{column.name}`)")
    #     errout(
    #         f"WARNING: Foreign key for {table}.{column.name} references non-existent table {fk_table}")
    # continue
    # else:
    #     print(
    #         f"processed referer to {tablename}.{column.name} --> {col_fk}", file=sys.stderr)
    #     deferred.extend(col_fk)

    # Make sure the dsestination column of the FK exists in this build
    # if fk_col not in all_data[fk_table]:
    #     errout(
    #         f"WARNING: Foreign key {table}.{column.name} references non-existent column {fk_col}.{c}")
    #     continue

    # deferred.append(
    #     f"  ADD CONSTRAINT `{tablename}_{column.name}` FOREIGN KEY (`{column.name}`) REFERENCES `{dbname}`.`{fk_table}` (`{fk_col}`)")
    # deferred.extend(col_fk)

    # Generate the actual `CREATE` statement
    # FIXME: include comment w/ layout hash(s), git source info, and file comments
    print(f"\nCREATE TABLE IF NOT EXISTS `{dbname}`.`{tablename}` (")
    print(",\n".join(create_lines))
    print(");")

    return deferred


def build_string_regex(arg_value, pat=re.compile(r"^\d+\.\d+\.\d+\.\d+$")) -> str:
    if not pat.match(arg_value):
        raise argparse.ArgumentTypeError("invalid build string (try e.g. '9.1.5.41488')")

    return arg_value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--definitions", dest="definitions", type=str, action='store',
        default="../../definitions", help="location of .dbd files")
    parser.add_argument(
        "--build", dest="build", type=build_string_regex, default="9.1.5.41488",
        help="full build number to use for parsing")
    parser.add_argument(
        "--dbname", dest="dbname", type=str, default="wowdbd",
        help="name of MySQL database to generate create statements for")
    parser.add_argument(
        "--no-pickle", dest="no_pickle", action='store_true', default=False,
        help="don't use or create pickled data file")
    parser.add_argument(
        "--no-warn-missing-fk", dest="no_warn_missing_fk", action='store_true', default=False,
        help="don't warn about missing FK referents")

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

    dbds = dbdwrapper.load_directory_cached(args.definitions)
    build = dbdwrapper.BuildId.from_string(args.build)
    view = dbds.get_view(build)
    fkcols = get_fk_cols(args, view)  # get foreign key columns
    analysis = get_analysis("analysis.csv")
    fk_fixup(view, fkcols, analysis)

    # deferred statements to add to `ALTER` at the end
    deferred = {}

    # No in-place updates -- just drop and recreate the entire database
    print(f"DROP DATABASE IF EXISTS {args.dbname};")
    print(f"CREATE DATABASE {args.dbname};")

    for table, data in view.items():
        deferred[table] = dumpdbd(args.dbname, table, view, data, fkcols)

    for table, lines in deferred.items():
        if len(lines) > 0:
            print(f"\nALTER TABLE `{args.dbname}`.`{table}`")
            print(",\n".join(lines))
            print(";")

    return 0


if __name__ == "__main__":
    sys.exit(main())
