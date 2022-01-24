#!/usr/bin/env python3

import sys
import os
import csv
from typing import List, Dict, Optional, Union, Tuple, Any, Set
import dbdwrapper

def get_fk_cols(dbds: dbdwrapper.DbdDirectory) -> Set[str]:
    fks: Set[str] = set()

    for _, data in dbds.items():
        for _, coldata in data.columns.items():
            if coldata.fk:
                key = f"{coldata.fk.table}.{coldata.fk.column}"
                fks.add(key)

    return fks


def compare_data(table_name: str, col: str, info: Dict[str, Any],
                 tags: Set[str], coldata: dbdwrapper.DbdVersionedCol,
                 fkcols: Set[str]) -> Set[str]:

    diffs: Set[str] = set()

    if f"{table_name}.{col}" in fkcols:
        diffs.add("REFERENT")

    if coldata.definition.type == "int":
        if "INT" not in tags:
            diffs.add("TYPE_SHOULD_BE_INT")

        if "UNIQUE" in tags and "id" not in coldata.annotation and f"{table_name}.{col}" not in fkcols:
            diffs.add("POSSIBLE_KEY")

        if "UNSIGNED" in tags and not coldata.is_unsigned:
            diffs.add("POSSIBLE_UNSIGNED")

        elif "UNSIGNED" not in tags and coldata.is_unsigned:
            diffs.add("POSSIBLE_SIGNED")

    if coldata.definition.type == "float":
        if "FLOAT" not in tags:
            diffs.add("SHOULD_BE_FLOAT")

    if coldata.definition.type == "string" or coldata.definition.type == "locstring":
        if "STRING" not in tags:
            diffs.add("SHOULD_BE_STRING")

        if "UNIQUE" in tags and "id" not in coldata.annotation and f"{table_name}.{col}" not in fkcols:
            diffs.add("POSSIBLE_KEY")

    return diffs


def generate_stats(column: List[str]) -> Dict[str, Any]:
    num_null = 0
    num_int = 0
    num_float = 0
    num_string = 0

    num_dupe = 0
    num_zero = 0
    num_negative = 0

    val_min: Union[int, float, None] = None
    val_max: Union[int, float, None] = None

    len_min: Optional[int] = None
    len_max: Optional[int] = None

    seen_types = set()

    num_rows = len(column)

    without_nulls = [x for x in column if x]
    num_null = len(column) - len(without_nulls)

    without_duplicates = set(without_nulls)
    num_dupe = len(without_nulls) - len(without_duplicates)

    x: Union[int, float]
    for v in without_duplicates:
        try:
            x = int(v)
            assert type(x) == int

            if val_min is None:
                val_min = x
                val_max = x
            else:
                assert val_min is not None and val_max is not None
                val_min = min(val_min, x)
                val_max = max(val_max, x)

            if x < 0:
                num_negative += 1

            if x == 0:
                num_zero += 1

            seen_types.add("int")
            num_int += 1
            continue
        except ValueError:
            pass

        # Not an int. Float?
        try:
            x = float(v)
            assert type(x) == float

            if val_min is None:
                val_min = x
                val_max = x
            else:
                assert val_min is not None and val_max is not None
                val_min = min(val_min, x)
                val_max = max(val_max, x)

            if x < 0:
                num_negative += 1

            if x == 0:
                num_zero += 1

            seen_types.add("float")
            num_float += 1
            continue
        except ValueError:
            pass

        # not a float, must be a string
        assert type(v) == str
        if len_min is None:
            len_min = len(v)
            len_max = len(v)
        else:
            assert len_min is not None and len_max is not None
            len_min = min(len_min, len(v))
            len_max = max(len_max, len(v))

        seen_types.add("string")
        num_string += 1

    if "int" in seen_types and "float" in seen_types:
        seen_types.remove("int")

    need_bits: Optional[int] = None
    if "int" in seen_types:
        assert type(val_min) == int and type(val_max) == int
        need_bits = max(val_min.bit_length(), val_max.bit_length())

    ret = {
        "num_null": num_null,
        "num_int": num_int,
        "num_float": num_float,
        "num_string": num_string,

        "num_rows": num_rows,
        "num_dupe": num_dupe,
        "num_zero": num_zero,
        "num_negative": num_negative,

        "val_min": val_min,
        "val_max": val_max,
        "need_bits": need_bits,

        "len_min": len_min,
        "len_max": len_max,



        "seen_types": seen_types,
    }

    return ret


def infer_type(info: Dict[str, Any]) -> Set[str]:
    tags: Set[str] = set()

    if info['num_rows'] <= 2:
        tags.add("INSUFFICIENT_DATA")
        return tags

    if len(info['seen_types']) > 1:
        tags.add("MIXED_TYPES")
    elif len(info['seen_types']) == 0:
        tags.add("NO_TYPES")
    else:
        if "int" in info['seen_types']:
            tags.add("INT")
        elif "float" in info['seen_types']:
            tags.add("FLOAT")
        elif "string" in info['seen_types']:
            tags.add("STRING")
        else:
            assert False, f"unknown type: {info['seen_types']}"

    if info['num_null'] == 0:
        tags.add("NOT_NULL")

    if info['num_dupe'] == 0:
        tags.add("UNIQUE")

    if "int" in info['seen_types']:
        if info['num_negative'] == 0:
            tags.add("UNSIGNED")

        if info['num_zero'] == 0:
            tags.add("NONZERO")

    return tags


def process_one(directory: str, table_name: str, view: dbdwrapper.DbdVersionedView, fkcols: Set[str]) -> None:
    file = table_name + ".csv"
    data: Dict[str, List[str]] = {}
    with open(os.path.join(directory, file), newline="") as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            for k in row.keys():
                if k not in data:
                    data[k] = []
                data[k].append(row[k])

    for col, values in data.items():
        if col.endswith("[0]"):
            col = col.replace("[0]", "")
        elif col.endswith("]"):
            continue

        info = generate_stats(values)
        tags = infer_type(info)

        diff = set("NO_DBD_INFO")
        fkstr = ""
        if table_name in view and col in view[table_name]:
            if view[table_name][col].definition.fk:
                fk = view[table_name][col].definition.fk
                assert fk is not None
                fkstr = f"{fk.table}.{fk.column}"

            diff = compare_data(table_name, col, info, tags, view[table_name][col], fkcols)

        line = f"{table_name},{col},{info['num_rows']},{info['num_int']},{info['num_float']},{info['num_string']},{info['num_null']},{info['num_dupe']},{info['num_zero']},{info['num_negative']},{info['val_min']},{info['val_max']},{info['need_bits']},{info['len_min']},{info['len_max']},{' '.join(tags)},{fkstr},{' '.join(diff)}"
        print(line.replace("None", ""))

    # add a blank line between tables
    print()


def main():
    # table_name = sys.argv[1]
    directory = "dbd-920dbcs41257"

    dbds = dbdwrapper.load_directory_cached("../../definitions", silent=True)
    build = dbdwrapper.BuildId.from_string("9.1.5.41488")
    view = dbds.get_view(build)
    fkcols = get_fk_cols(dbds)

    print("table,column,num_rows,num_int,num_float,num_str,num_null,num_dupe,num_zero,num_neg,val_min,val_max,need_bits,len_min,len_max,tags,FK,issues")

    for file in sorted(os.listdir(directory)):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            table_name = filename.replace(".csv", "")
            process_one(directory, table_name, view, fkcols)
            continue

    # info = determine_type(values)

    # exist_count = info["num_rows"] - info["num_null"]
    # exist_pct = (exist_count / info["num_rows"]) * 100.0
    # print(f"info for {column}:")
    # print(f"  types seen: {len(info['seen_types'])} ({','.join(info['seen_types'])})")
    # print(f"  int: {info['num_int']}  float: {info['num_float']}  string: {info['num_string']}")
    # print(f"  total rows: {info['num_rows']}")
    # print(f"  rows with values: {exist_count} ({exist_pct:.2f}%)")
    # print(f"table_nacolumn_nameth duplicate values: {info['num_dupe']}")
    # print(f"  rows with zero values: {info['num_zero']}")
    # print(f"  rows with negative values: {info['num_negative']}")
    # print(f"  row min/max: {info['val_min']}/{info['val_max']}")


main()
