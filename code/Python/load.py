#!/usr/bin/env python3
import csv
import os
import re
import sys
from typing import Dict, Any

from ppretty import ppretty
import argparse
import time
# sqlalchemy is mostly overkill, but it makes things easy.
import sqlalchemy
from sqlalchemy import MetaData, Table, create_engine, insert, text
from dataclasses import dataclass
from dbdwrapper import DbdColumnId
from dbdanalyze import load_analysis, AnalysisData
import dbdwrapper as dbd

# directory = "dbd-920dbcs41257"
dbstring = "mysql+pymysql://root@localhost/wowdbd"
# table = "AdventureJournal"

array_re = re.compile(r"\[\d+\]$")

def load_one(engine, directory: str, tablename: str, analysis: AnalysisData):
    print(f"Loading {tablename}...", end="", flush=True)

    file = tablename + ".csv"
    time_prep_start = time.monotonic()

    rows = []
    with open(os.path.join(directory, file), newline="") as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            prow = {}
            for colname, colval in row.items():
                prow[colname] = colval if colval != "" else None

                a_column = analysis.for_column(DbdColumnId(tablename, colname))
                if a_column is None:
                    print(f"No analysis for {tablename}.{colname}")
                elif "NEG_IS_NULL" in a_column.tags and int(colval) < 0:
                    prow[colname] = None

            rows.append(prow)

    time_prep_end = time.monotonic()
    print(f" {len(rows)} rows, prepaired in {time_prep_end - time_prep_start:.2f}s", end="", flush=True)

    time_load_start = time.monotonic()
    metadata_obj = MetaData()

    try:
        tablemeta = Table(tablename, metadata_obj, autoload_with=engine)
    except sqlalchemy.exc.NoSuchTableError:
        print(f"ERROR: Table {tablename} not found", file=sys.stderr)
        return

    with engine.begin() as conn:
        try:
            conn.execute(text("SET SESSION foreign_key_checks = 0"))
            conn.execute(text(f"TRUNCATE `{tablename}`"))
        except sqlalchemy.exc.InvalidRequestError as e:
            print(f"ERROR: Invalid sql request for {tablename}: {e}", file=sys.stderr)
            return


        result = conn.execute(
            insert(tablemeta), rows
        )
        # print(result)
        time_load_end = time.monotonic()
        print(f", loaded in {time_load_end - time_load_start:.2f}s", flush=True)

def build_string_regex(arg_value, pat=re.compile(r"^\d+\.\d+\.\d+\.\d+$")) -> str:
    if not pat.match(arg_value):
        raise argparse.ArgumentTypeError("invalid build string (try e.g. '9.1.5.41488')")

    return arg_value

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--analysis", dest="analysis_file", type=str, action='store',
        default="analysis.csv", help="extra column analysis data")
    parser.add_argument(
        "--datadir", dest="datadir", type=str, action='store', default=None,
        help="location of DBD data .csv files")
    parser.add_argument(
        "--build", dest="build", type=build_string_regex, default="9.2.0.41257",
        help="full build number to use for parsing")
    parser.add_argument(
        "--connect-string", dest="connect_string", type=str, action='store',
        default="mysql+pymysql://root@localhost/wowdbd")

    args = parser.parse_args()
    build = dbd.BuildId.from_string(args.build)

    if args.datadir is None:
        args.datadir = f"dbd-{build.major}{build.minor}{build.patch}dbcs{build.build}"

    analysis = load_analysis(args.analysis_file)
    engine = create_engine(args.connect_string, echo=False, future=True)

    for file in sorted(os.listdir(args.datadir)):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            table_name = filename.replace(".csv", "")
            load_one(engine, args.datadir, table_name, analysis)

    return 0


if __name__ == "__main__":
    sys.exit(main())
