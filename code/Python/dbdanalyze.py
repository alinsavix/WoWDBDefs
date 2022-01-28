#!/usr/bin/env python3

import csv
import dataclasses
import os
import sys
from collections import UserDict
from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union
import inspect

import dbdwrapper
from dbdwrapper import DbdColumnId
from ppretty import ppretty


class DbdAnalysisTags(Set[str]):
    def __str__(self):
        return " ".join(sorted(self))

class DbdAnalysisIssues(Set[str]):
    def __str__(self):
        return " ".join(sorted(self))

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

    val_min: Union[int, float, None] = None
    val_max: Union[int, float, None] = None
    need_bits: Optional[int] = None

    len_min: Optional[int] = None
    len_max: Optional[int] = None

    seen_types: Set[str] = dataclasses.field(default_factory=set)
    tags: DbdAnalysisTags = dataclasses.field(default_factory=DbdAnalysisTags)
    issues: DbdAnalysisIssues = dataclasses.field(default_factory=DbdAnalysisIssues)

    fk: Optional[DbdColumnId] = None

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


if TYPE_CHECKING:
    UserDict_DbdTableAnalysis = UserDict[str, DbdColumnAnalysis]
else:
    UserDict_DbdTableAnalysis = UserDict

class DbdTableAnalysis(UserDict_DbdTableAnalysis):
    pass


def compare_data(tablename: str, colname: str, analysis: DbdColumnAnalysis,
                 coldata: dbdwrapper.DbdVersionedCol, fkcols: Set[str]) -> DbdAnalysisIssues:

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

    without_duplicates = set(without_nulls)
    analysis.num_dupe = len(without_nulls) - len(without_duplicates)

    x: Union[int, float]
    for v in without_duplicates:
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
                analysis.num_negative += 1

            if x == 0:
                analysis.num_zero += 1

            analysis.seen_types.add("int")
            analysis.num_int += 1
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
                analysis.num_negative += 1

            if x == 0:
                analysis.num_zero += 1

            analysis.seen_types.add("float")
            analysis.num_float += 1
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
        analysis.num_str += 1

    if "int" in analysis.seen_types and "float" in analysis.seen_types:
        analysis.seen_types.remove("int")

    if "int" in analysis.seen_types:
        assert type(analysis.val_min) == int and type(analysis.val_max) == int
        analysis.need_bits = max(analysis.val_min.bit_length(), analysis.val_max.bit_length())

    return analysis


def infer_type(analysis: DbdColumnAnalysis) -> DbdAnalysisTags:
    tags = DbdAnalysisTags()

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

    if analysis.num_dupe == 0:
        tags.add("UNIQUE")

    if "int" in analysis.seen_types:
        if analysis.num_negative == 0:
            tags.add("UNSIGNED")

        if analysis.num_zero == 0:
            tags.add("NOT_ZERO")

    return tags


def analyze_table(directory: str, tablename: str, view: dbdwrapper.DbdVersionedView, fkcols: Set[str]) -> None:
    file = tablename + ".csv"
    data: Dict[str, List[str]] = {}

    with open(os.path.join(directory, file), newline="") as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            for k in row.keys():
                if k not in data:
                    data[k] = []
                data[k].append(row[k])


    table_analysis = DbdTableAnalysis()

    for colname, values in data.items():
        # Assume the info for all columns in an array'd column are the same
        if colname.endswith("[0]"):
            colname = colname.replace("[0]", "")
        elif colname.endswith("]"):
            continue

        analysis = generate_stats(values)
        analysis.tags = infer_type(analysis)
        analysis.issues = DbdAnalysisIssues("NO_DBD_INFO")

        fkstr = ""
        if tablename in view and colname in view[tablename]:
            if view[tablename][colname].definition.fk:
                analysis.fk = view[tablename][colname].definition.fk
                assert analysis.fk is not None
                fkstr = str(analysis.fk)

                # if a column references a FK, and that column has only one
                # distinct negative value, and that value is -1, then treat
                # -1 as null, and make the column unsigned and nullable.
                if analysis.num_negative == 1 and analysis.val_min == -1:
                    analysis.tags.remove("NOT_NULL")
                    analysis.tags.add("NEG_ONE_IS_NULL")
                    analysis.tags.add("UNSIGNED")

            analysis.issues = compare_data(
                tablename, colname, analysis, view[tablename][colname], fkcols)
            table_analysis[colname] = analysis

        line = f"{tablename},{colname},{analysis.num_rows},{analysis.num_int},{analysis.num_float},{analysis.num_str},{analysis.num_null},{analysis.num_dupe},{analysis.num_zero},{analysis.num_negative},{analysis.val_min},{analysis.val_max},{analysis.need_bits},{analysis.len_min},{analysis.len_max},{' '.join(analysis.tags)},{fkstr},{' '.join(analysis.issues)}"
        print(line.replace("None", ""))

    # add a blank line between tables
    print()


def from_dict_to_dataclass(cls, data):
    # print(ppretty(inspect.signature(DbdColumnAnalysis).parameters.items()))

    return cls(
        **{
            key: (data[key] if val.default == val.empty else data.get(key, val.default))
            for key, val in inspect.signature(DbdColumnAnalysis).parameters.items()
        }
    )


if TYPE_CHECKING:
    UserDict_AnalysisData = UserDict[DbdColumnId, DbdColumnAnalysis]
else:
    UserDict_AnalysisData = UserDict

class AnalysisData(UserDict_AnalysisData):
    pass


def load_analysis(filename: str) -> AnalysisData:
    data = AnalysisData()
    with open(filename, newline="") as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            if "table" in row and len(row["table"]) > 0:
                colid = DbdColumnId(row["table"], row["column"])
                data[colid] = DbdColumnAnalysis.from_dict(row)

    return data


def main():
    # table_name = sys.argv[1]
    directory = "dbd-920dbcs41257"

    dbds = dbdwrapper.load_dbd_directory_cached("../../definitions", silent=False)
    build = dbdwrapper.BuildId.from_string("9.1.5.41488")
    view = dbds.get_view(build)
    fkcols = view.get_fk_cols()

    print("table,column,num_rows,num_int,num_float,num_str,num_null,num_dupe,num_zero,num_negative,val_min,val_max,need_bits,len_min,len_max,tags,FK,issues")

    for file in sorted(os.listdir(directory)):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            tablename = filename.replace(".csv", "")
            analyze_table(directory, tablename, view, fkcols)
            continue

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


if __name__ == "__main__":
    sys.exit(main())

    # z = load_analysis("analysis.csv")
    # print(ppretty(z))

    # x = inspect.signature(DbdColumnAnalysis)
    # print(ppretty(x))
    # print(ppretty(fields(DbdColumnAnalysis)))
    # print(inspect.signature(DbdColumnAnalysis).parameters.keys())
