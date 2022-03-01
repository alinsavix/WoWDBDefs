#!/usr/bin/env python3
# requires python >= 3.9 (...sorry)
"""
dbdanalyze: make an analysis of the contents of a WoW db2 database dump
(provided by the user in csv format) and make some inferences about the
data found therein, intended to make properly wrangling that data in
various types of database easier.
"""
import argparse
import csv
import dataclasses
import os
import sys
import re
from collections import UserDict, Counter
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import inspect

import dbdwrapper as dbd
from dbdwrapper import DbdColumnId
from ppretty import ppretty

IGNORE_FK = frozenset([
    DbdColumnId("Achievement", "Faction"),
    DbdColumnId("LFGDungeons", "Faction"),
])


class DbdAnalysisTags(Set[str]):
    def __str__(self):
        return " ".join(sorted(self))

class DbdAnalysisIssues(Set[str]):
    def __str__(self):
        return " ".join(sorted(self))

def nonemin(a: Optional[Union[int, float]],
            b: Optional[Union[int, float]]) -> Optional[Union[int, float]]:
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)

def nonemax(a: Optional[Union[int, float]],
            b: Optional[Union[int, float]]) -> Optional[Union[int, float]]:
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


@dataclass
class DbdColumnAnalysis:
    num_rows: int = 0

    num_null: int = 0
    num_int: int = 0
    num_float: int = 0
    num_str: int = 0

    num_dupe: int = 0
    num_zero: int = 0
    num_negative: int = 0
    num_neg_vals: int = 0

    val_min: Union[int, float, None] = None
    val_max: Union[int, float, None] = None
    need_bits: Optional[int] = None

    len_min: Optional[int] = None
    len_max: Optional[int] = None

    seen_types: Set[str] = dataclasses.field(default_factory=set)
    tags: DbdAnalysisTags = dataclasses.field(default_factory=DbdAnalysisTags)
    issues: DbdAnalysisIssues = dataclasses.field(default_factory=DbdAnalysisIssues)

    fk: Optional[DbdColumnId] = None

    # FIXME: can this be simplified? very repetitive
    def __add__(self, d):
        analysis = DbdColumnAnalysis()
        analysis.num_rows = self.num_rows + d.num_rows

        analysis.num_null = self.num_null + d.num_null
        analysis.num_int = self.num_int + d.num_int
        analysis.num_float = self.num_float + d.num_float
        analysis.num_str = self.num_str + d.num_str

        analysis.num_dupe = self.num_dupe + d.num_dupe
        analysis.num_zero = self.num_zero + d.num_zero
        analysis.num_negative = self.num_negative + d.num_negative

        # a special case -- if we're using a negative value as a canary for
        # 'null', detect that by there only being one negative value used
        # across the entirety of the related columns.
        # FIXME: Can we do better?
        if self.num_neg_vals == 1 and d.num_neg_vals == 1 and self.val_min == d.val_min:
            analysis.num_neg_vals = 1
        else:
            analysis.num_neg_vals = self.num_neg_vals + d.num_neg_vals

        analysis.val_min = nonemin(self.val_min, d.val_min)
        analysis.val_max = nonemax(self.val_max, d.val_max)
        analysis.need_bits = nonemax(self.need_bits, d.need_bits)

        analysis.len_min = nonemin(self.len_min, d.len_min)
        analysis.len_max = nonemax(self.len_max, d.len_max)

        analysis.seen_types = self.seen_types.union(d.seen_types)
        analysis.tags = self.tags.union(d.tags)
        analysis.issues = self.issues.union(d.issues)

        analysis.fk = self.fk  # different columns can't have different FKs

        return analysis

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DbdColumnAnalysis":
        # This is stupid ugly to load this data, and we should really find
        # a better way, but this will suffice for now.
        def int_float_none(x: str) -> Union[int, float, None]:
            if not x:
                return None

            # if it's an int, use that
            try:
                return int(x)
            except ValueError:
                pass

            # otherwise return float (or it'll raise an exception)
            return float(x)

        return cls(
            num_rows=int(d["num_rows"]),

            num_null=int(d["num_null"]),
            num_int=int(d["num_int"]),
            num_float=int(d["num_float"]),
            num_str=int(d["num_str"]),

            num_dupe=int(d["num_dupe"]),
            num_zero=int(d["num_zero"]),
            num_negative=int(d["num_negative"]),
            num_neg_vals=int(d["num_neg_vals"]),

            val_min=int_float_none(d["val_min"]),
            val_max=int_float_none(d["val_max"]),
            need_bits=int(d["need_bits"]) if d["need_bits"] else None,

            len_min=int(d["len_min"]) if d["len_min"] else None,
            len_max=int(d["len_max"]) if d["len_max"] else None,

            # seen_types=set(d["seen_types"].split(" ")),
            tags=DbdAnalysisTags(d["tags"].split(" ")),
            issues=DbdAnalysisIssues(d["issues"].split(" ")),

            fk=DbdColumnId(*d["FK"].split(".")) if d["FK"] else None,
        )


class DbdTableAnalysis(UserDict[str, DbdColumnAnalysis]):
    pass


class AnalysisData(UserDict[DbdColumnId, DbdColumnAnalysis]):
    def for_column(self, colid: DbdColumnId) -> Optional[DbdColumnAnalysis]:
        a_colname = analysis_colname(colid.column)
        a_colid = DbdColumnId(colid.table, a_colname)
        a_colid_arr = DbdColumnId(colid.table, a_colname + "[0]")

        if a_colid in self:
            return self[a_colid]
        elif a_colid_arr in self:
            return self[a_colid_arr]
        else:
            return None

    def get_columns(self, table: str) -> Set[str]:
        return {k.column for k in self.keys() if k.table == table}

    def tablenames(self) -> Set[str]:
        return {colid.table for colid in self.keys()}


# We basically are going to split array columns into "first entry" and "every
# other entry", this will return the column name to use for generating/using
# the analysis.
def analysis_colname(colname: str) -> str:
    if not colname.endswith("]"):
        return colname

    if colname.endswith("[0]"):
        return colname

    # else

    return array_re.sub("", colname) + "[x]"


def compare_data(tablename: str, colname: str, analysis: DbdColumnAnalysis,
                 coldata: dbd.DbdVersionedCol, fkcols: dbd.FKReferents) -> DbdAnalysisIssues:

    diffs = DbdAnalysisIssues()

    if f"{tablename}.{colname}" in fkcols:
        diffs.add("REFERENT")

    if coldata.definition.type == "int":
        if "INT" not in analysis.tags:
            diffs.add("TYPE_SHOULD_BE_INT")

        if "UNIQUE" in analysis.tags and "id" not in coldata.annotation and f"{tablename}.{colname}" not in fkcols:
            diffs.add("POSSIBLE_KEY")

        if "UNSIGNED" in analysis.tags and not coldata.is_unsigned:
            diffs.add("POSSIBLE_UNSIGNED")

        elif "UNSIGNED" not in analysis.tags and coldata.is_unsigned:
            diffs.add("POSSIBLE_SIGNED")

    if coldata.definition.type == "float":
        if "FLOAT" not in analysis.tags:
            diffs.add("SHOULD_BE_FLOAT")

    if coldata.definition.type == "string" or coldata.definition.type == "locstring":
        if "STRING" not in analysis.tags:
            diffs.add("SHOULD_BE_STRING")

        if "UNIQUE" in analysis.tags and "id" not in coldata.annotation and f"{tablename}.{colname}" not in fkcols:
            diffs.add("POSSIBLE_KEY")

    return diffs


def generate_stats(column: List[str]) -> DbdColumnAnalysis:
    analysis = DbdColumnAnalysis()

    analysis.num_rows = len(column)

    without_nulls = [x for x in column if x]
    analysis.num_null = analysis.num_rows - len(without_nulls)

    value_counts = Counter(without_nulls)
    analysis.num_dupe = len(without_nulls) - len(value_counts)

    x: Union[int, float]
    for v, c in value_counts.items():
        try:
            x = int(v)
            assert type(x) == int

            if analysis.val_min is None:
                analysis.val_min = x
                analysis.val_max = x
            else:
                assert analysis.val_min is not None and analysis.val_max is not None
                analysis.val_min = min(analysis.val_min, x)
                analysis.val_max = max(analysis.val_max, x)

            if x < 0:
                analysis.num_neg_vals += 1
                analysis.num_negative += c

            if x == 0:
                analysis.num_zero += c

            analysis.seen_types.add("int")
            analysis.num_int += c
            continue
        except ValueError:
            pass

        # Not an int. Float?
        try:
            x = float(v)
            assert type(x) == float

            if analysis.val_min is None:
                analysis.val_min = x
                analysis.val_max = x
            else:
                assert analysis.val_min is not None and analysis.val_max is not None
                analysis.val_min = min(analysis.val_min, x)
                analysis.val_max = max(analysis.val_max, x)

            if x < 0:
                analysis.num_negative += c
                analysis.num_neg_vals += 1

            if x == 0:
                analysis.num_zero += c

            analysis.seen_types.add("float")
            analysis.num_float += c
            continue
        except ValueError:
            pass

        # not a float, must be a string
        assert type(v) == str
        if analysis.len_min is None:
            analysis.len_min = len(v)
            analysis.len_max = len(v)
        else:
            assert analysis.len_min is not None and analysis.len_max is not None
            analysis.len_min = min(analysis.len_min, len(v))
            analysis.len_max = max(analysis.len_max, len(v))

        analysis.seen_types.add("string")
        analysis.num_str += c

    if "int" in analysis.seen_types and "float" in analysis.seen_types:
        analysis.seen_types.discard("int")

    if "int" in analysis.seen_types:
        assert type(analysis.val_min) == int and type(analysis.val_max) == int
        analysis.need_bits = max(analysis.val_min.bit_length(), analysis.val_max.bit_length())

    return analysis


def infer_type(analysis: DbdColumnAnalysis) -> DbdAnalysisTags:
    tags = DbdAnalysisTags()

    if analysis.num_rows == 0:
        tags.add("NO_DATA")
        return tags

    if analysis.num_rows <= 2:
        tags.add("INSUFFICIENT_DATA")
        return tags

    if len(analysis.seen_types) > 1:
        tags.add("MIXED_TYPES")
    elif len(analysis.seen_types) == 0:
        tags.add("NO_TYPES")
    else:
        if "int" in analysis.seen_types:
            tags.add("INT")
        elif "float" in analysis.seen_types:
            tags.add("FLOAT")
        elif "string" in analysis.seen_types:
            tags.add("STRING")
        else:
            assert False, f"unknown type: {analysis.seen_types}"

    if analysis.num_null == 0:
        tags.add("NOT_NULL")
    else:
        tags.add("NULLABLE")

    if analysis.num_dupe == 0:
        tags.add("UNIQUE")

    if "int" in analysis.seen_types:
        if analysis.num_negative == 0:
            tags.add("UNSIGNED")

        if analysis.num_zero == 0:
            tags.add("NOT_ZERO")
        else:
            tags.add("HAS_ZERO")

    return tags


def print_analysis(tablename: str, colname: str, analysis: DbdColumnAnalysis) -> None:
    fkstr = ""
    if analysis.fk is not None:
        fkstr = str(analysis.fk)

    line = f"{tablename},{colname},{analysis.num_rows},{analysis.num_int},{analysis.num_float},{analysis.num_str},{analysis.num_null},{analysis.num_dupe},{analysis.num_zero},{analysis.num_negative},{analysis.num_neg_vals},{analysis.val_min},{analysis.val_max},{analysis.need_bits},{analysis.len_min},{analysis.len_max},{' '.join(sorted(analysis.tags))},{fkstr},{' '.join(sorted(analysis.issues))}"
    print(line.replace("None", ""))


# output some hand-crafted artisinal data for tables that don't exist locally
def print_missing(a: 'AnalysisData', tablename: str, view: dbd.DbdVersionedView) -> None:
    table = view[tablename]
    for colname, coldata in table.items():
        analysis = DbdColumnAnalysis()
        analysis.tags.add("NO_TABLE")
        analysis.issues = DbdAnalysisIssues(["NO_TABLE"])

        # We have no actual data, so fill in what we can from the dbd
        if coldata.definition.type == "int":
            analysis.tags.add("INT")
        elif coldata.definition.type == "float":
            analysis.tags.add("FLOAT")
        elif coldata.definition.type == "string" or coldata.definition.type == "locstring":
            analysis.tags.add("STRING")

        # FIXME: deduplicate
        fkstr = ""
        if tablename in view and colname in view[tablename]:
            if view[tablename][colname].definition.fk:
                analysis.fk = view[tablename][colname].definition.fk
                assert analysis.fk is not None
                fkstr = str(analysis.fk)


        # print_analysis(tablename, colname, analysis, fkstr)
        colid = DbdColumnId(tablename, colname)
        a[colid] = analysis


array_re = re.compile(r"\[[x0-9]+\]$")

def analyze_table(a: 'AnalysisData', directory: str, tablename: str,
                  view: dbd.DbdVersionedView, fkcols: dbd.FKReferents) -> None:
    file = tablename + ".csv"
    data: Dict[str, List[str]] = {}

    try:
        with open(os.path.join(directory, file), newline="") as csvfile:
            reader = csv.DictReader(csvfile, dialect='excel')
            for row in reader:
                for k in row.keys():
                    if k not in data:
                        data[k] = []
                    data[k].append(row[k])
    except FileNotFoundError:
        print(f"WARNING: No CSV available for {tablename}", file=sys.stderr)
        print_missing(a, tablename, view)
        return

    table_analysis = DbdTableAnalysis()

    for colname, values in data.items():
        # Assume the info for all columns in an array'd column are the same
        a_colname = analysis_colname(colname)
        analysis = generate_stats(values)

        # print(f"{a_colname} analysis: {analysis}")

        if a_colname in table_analysis:
            table_analysis[a_colname] += analysis
        else:
            table_analysis[a_colname] = analysis

    # got the analysis done, now learn about it
    for a_colname, analysis in table_analysis.items():
        analysis.tags = infer_type(analysis)

        colid = DbdColumnId(tablename, a_colname)

        b_colname = array_re.sub("", a_colname)
        if tablename in view and b_colname in view[tablename]:
            if view[tablename][b_colname].definition.fk:
                analysis.fk = view[tablename][b_colname].definition.fk
                assert analysis.fk is not None

                if colid in IGNORE_FK:
                    analysis.tags.add("IGNORE_FK")

                else:
                    # if an int column references a FK, and that column has only
                    # one distinct negative value, and that value is pretty close
                    # to zero, then treat negatives in that column as null, and
                    # make the column unsigned and nullable. (most tables use -1
                    # as the canary value for this, but at least one uses -2)
                    if "int" in analysis.seen_types:
                        assert analysis.val_min is not None
                        if analysis.num_neg_vals == 1 and analysis.val_min >= -2:
                            analysis.tags.discard("NOT_NULL")
                            analysis.tags.add("NEG_IS_NULL")
                            analysis.tags.add("UNSIGNED")

            analysis.issues = compare_data(
                tablename, b_colname, analysis, view[tablename][b_colname], fkcols)
        else:
            analysis.issues = DbdAnalysisIssues("NO_DBD_INFO")

        a[colid] = analysis

# for referer_col, referer_coldata in fkcols[referent_col].items():
def analyze_fk_nulls(analysis: 'AnalysisData'):
    for referer_col, referer_analysis in analysis.items():
        if referer_analysis.fk is None:
            continue

        if "IGNORE_FK" in referer_analysis.tags:
            continue

        referent_analysis = analysis.for_column(referer_analysis.fk)
        if referent_analysis is None:
            continue

        # if the referent isn't an int, skip the special logic
        if "int" not in referent_analysis.seen_types:
            continue

        # if the referent has actual zero values, referers can't be using
        # zeros as null
        if referent_analysis.num_zero > 0:
            continue

        if referer_analysis.num_zero == 0:
            continue

        # FIXME: Can we have both NEG_IS_NULL and ZERO_IS_NULL?
        if "NOT_NULL" in referer_analysis.tags:
            referer_analysis.tags.discard("NOT_NULL")

        # print(f"zero is null for: {referer}", file=sys.stderr)
        referer_analysis.tags.add("ZERO_IS_NULL")


def load_analysis(filename: str) -> AnalysisData:
    data = AnalysisData()
    with open(filename, newline="") as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            if "table" in row and len(row["table"]) > 0:
                colid = DbdColumnId(row["table"], row["column"])
                data[colid] = DbdColumnAnalysis.from_dict(row)

    return data


def build_string_regex(arg_value, pat=re.compile(r"^\d+\.\d+\.\d+\.\d+$")) -> str:
    if not pat.match(arg_value):
        raise argparse.ArgumentTypeError("invalid build string (try e.g. '9.1.5.41488')")

    return arg_value

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--definitions", "--defs", dest="definitions", type=str, action='store',
        default="../../definitions", help="location of .dbd files")
    # parser.add_argument(
    #     "--analysis", dest="analysis_file", type=str, action='store',
    #     default="analysis.csv", help="extra column analysis data")
    parser.add_argument(
        "--datadir", dest="datadir", type=str, action='store', default=None,
        help="location of DBD data .csv files")
    parser.add_argument(
        "--build", dest="build", type=build_string_regex, default="9.2.0.42423",
        help="full build number to use for parsing")

    args = parser.parse_args()
    build = dbd.BuildId.from_string(args.build)

    if args.datadir is None:
        args.datadir = f"dbd-{build.major}{build.minor}{build.patch}dbcs{build.build}"

    dbds = dbd.load_dbd_directory_cached("../../definitions", silent=False)
    view = dbds.get_view(build)
    fkcols = view.get_fk_cols()

    data = AnalysisData()
    for tablename in sorted(view.keys()):
        analyze_table(data, args.datadir, tablename, view, fkcols)

    analyze_fk_nulls(data)

    print("table,column,num_rows,num_int,num_float,num_str,num_null,num_dupe,num_zero,num_negative,num_neg_vals,val_min,val_max,need_bits,len_min,len_max,tags,FK,issues")

    prev_table = ""
    for colid, analysis in data.items():
        if colid.table != prev_table:
            # blank line between tables
            if prev_table:
                print()
            prev_table = colid.table

        print_analysis(colid.table, colid.column, analysis)
        # filename = os.fsdecode(file)
        # if filename.endswith(".csv"):
        #     tablename = filename.replace(".csv", "")
        #     analyze_table(directory, tablename, view, fkcols)
        #     continue

        # info = determine_type(values)

        # exist_count = info["num_rows"] - info["num_null"]
        # exist_pct = (exist_count / info["num_rows"]) * 100.0
        # print(f"info for {column}:")
        # print(f"  types seen: {len(info['seen_types'])} ({','.join(info['seen_types'])})")
        # print(f"  int: {info['num_int']}  float: {info['num_float']}  string: {info['num_str']}")
        # print(f"  total rows: {info['num_rows']}")
        # print(f"  rows with values: {exist_count} ({exist_pct:.2f}%)")
        # print(f"table_nacolumn_nameth duplicate values: {info['num_dupe']}")
        # print(f"  rows with zero values: {info['num_zero']}")
        # print(f"  rows with negative values: {info['num_negative']}")
        # print(f"  row min/max: {info['val_min']}/{info['val_max']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

    # z = load_analysis("analysis.csv")
    # print(ppretty(z))

    # x = inspect.signature(DbdColumnAnalysis)
    # print(ppretty(x))
    # print(ppretty(fields(DbdColumnAnalysis)))
    # print(inspect.signature(DbdColumnAnalysis).parameters.keys())
