#!/usr/bin/env python3

import sys
import os
import csv
from typing import List, Dict, Optional, Union, Tuple, Any


def determine_type(column: List[str]) -> Dict[str, Any]:
    num_null = 0
    num_int = 0
    num_float = 0
    num_string = 0

    num_dupe = 0
    num_zero = 0
    num_negative = 0

    val_min: Union[int, float, None] = None
    val_max: Union[int, float, None] = None

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
        seen_types.add("string")
        num_string += 1

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

        "seen_types": seen_types,
    }
    # print(f"returning: {ret}")
    return ret


def main():
    table_name = sys.argv[1]

    dir = "dbd-920dbcs41257"
    file = table_name.lower() + ".csv"
    data: Dict[str, List[str]] = {}
    with open(os.path.join(dir, file), newline="") as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            for k in row.keys():
                if k not in data:
                    data[k] = []
                data[k].append(row[k])


    print("table,column,num_rows,num_int,num_float,num_string,num_null,num_dupe,num_zero,num_negative,val_min,val_max")
    for col, values in data.items():
        info = determine_type(values)

        line = f"{table_name},{col},{info['num_rows']},{info['num_int']},{info['num_float']},{info['num_string']},{info['num_null']},{info['num_dupe']},{info['num_zero']},{info['num_negative']},{info['val_min']},{info['val_max']}"
        print(line)
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
