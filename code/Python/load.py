#!/usr/bin/env python3
import csv
import os
import re
import sys
from typing import Dict, Any, Union, Set

from pathlib import Path
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
import hashlib


def get_file_hash(file: Union[str, Path]) -> str:
    file = Path(file)
    with file.open("rb") as f:
        h = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            h.update(chunk)
            chunk = f.read(8192)

    return h.hexdigest()


def load_one(engine, file: Path, tablename: str, analysis: AnalysisData, datameta: Table) -> int:
    print(f"Loading {tablename}...", end="", flush=True)

    negnullcols: Set[str] = set()

    for c in analysis.get_columns(tablename):
        cc = analysis.for_column(DbdColumnId(tablename, c))
        if cc:
            if "NEG_IS_NULL" in cc.tags:
                negnullcols.add(c)

    time_prep_start = time.monotonic()

    rows = []
    with file.open("r", newline="") as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            rows.append({k: None if (k in negnullcols and int(v) < 0)
                         else v for k, v in row.items()})

    time_prep_end = time.monotonic()
    print(f" {len(rows)} rows, prepaired in {time_prep_end - time_prep_start:.2f}s", end="", flush=True)

    time_load_start = time.monotonic()
    metadata_obj = MetaData()

    try:
        tablemeta = Table(tablename, metadata_obj, autoload_with=engine)
    except sqlalchemy.exc.NoSuchTableError:
        print(f"ERROR: Table {tablename} not found", file=sys.stderr)
        return 0

    with engine.begin() as conn:
        try:
            conn.execute(text("SET SESSION foreign_key_checks = 0"))
            conn.execute(text(f"TRUNCATE `{tablename}`"))
            # conn.execute(text(f"LOCK TABLES `{tablename}` WRITE"))
            # conn.execute(text(f"ALTER TABLE `{tablename}` DISABLE KEYS"))
        except sqlalchemy.exc.InvalidRequestError as e:
            print(f"ERROR: Invalid sql request for {tablename}: {e}", file=sys.stderr)
            return 0


        result = conn.execute(
            insert(tablemeta), rows
        )
        # print(result)

        # conn.execute(text(f"ALTER TABLE `{tablename}` ENABLE KEYS"))
        # conn.execute(text(f"UNLOCK TABLES"))

        time_load_end = time.monotonic()
        print(f", loaded in {time_load_end - time_load_start:.2f}s", flush=True)

        hash = get_file_hash(file)
        result = conn.execute(
            insert(datameta), {"table": tablename, "path": str(file), "hash": hash}
        )

        return len(rows)

def build_string_regex(arg_value, pat=re.compile(r"^\d+\.\d+\.\d+\.\d+$")) -> str:
    if not pat.match(arg_value):
        raise argparse.ArgumentTypeError("invalid build string (try e.g. '9.1.5.41488')")

    return arg_value

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--analysis", "--analysis-file", dest="analysis_file", type=str,
        action='store', default="analysis.csv", help="extra column analysis data")
    parser.add_argument(
        "--datadir", dest="datadir", type=Path, action='store', default=None,
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
        args.datadir = Path(f"dbd-{build.major}{build.minor}{build.patch}dbcs{build.build}")

    analysis = load_analysis(args.analysis_file)
    engine = create_engine(args.connect_string, echo=False, future=True)


    try:
        data_meta_obj = MetaData()
        data_meta = Table("_dbd_data_meta", data_meta_obj, autoload_with=engine)
    except sqlalchemy.exc.NoSuchTableError:
        print(f"ERROR: Table _dbd_data_meta not found", file=sys.stderr)
        return 0


    # for file in sorted(os.listdir(args.datadir)):
    #     filename = os.fsdecode(file)
    #     if filename.endswith(".csv"):
    #         table_name = filename.replace(".csv", "")
    #         load_one(engine, args.datadir, table_name, analysis)

    print(f"Loading data for build {build}")

    time_start = time.monotonic()
    rows_loaded = 0
    tables_loaded = 0

    for tablename in sorted(analysis.tablenames()):
        # for tablename in ["SpellVisualKitModelAttach"]:
        file = args.datadir / (tablename + ".csv")
        if file.exists():
            rows_loaded += load_one(engine, file, tablename, analysis, data_meta)
            tables_loaded += 1
        else:
            print(f"WARNING: No file for {tablename}", file=sys.stderr)

    time_end = time.monotonic()
    print(
        f"DONE. Loaded {rows_loaded} rows for {tables_loaded} tables in {time_end - time_start:.2f}s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
