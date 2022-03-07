#!/usr/bin/env python3
import csv
import os
import re
import sys
from typing import Dict, Any, Union, Set, List

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


def insert_rows(engine, file: Path, tablename: str, rows: List[Dict[str, Any]], datameta: Table) -> None:
    time_load_start = time.monotonic()
    metadata_obj = MetaData()

    try:
        tablemeta = Table(tablename, metadata_obj, autoload_with=engine)
    except sqlalchemy.exc.NoSuchTableError as e:
        print(f"ERROR: Table {tablename} not found in database", file=sys.stderr)
        raise e from None

    with engine.begin() as conn:
        try:
            conn.execute(text("SET SESSION foreign_key_checks = 0"))
            conn.execute(text(f"TRUNCATE `{tablename}`"))
            conn.execute(text(f"DELETE FROM _dbd_data_meta WHERE `table`='{tablename}'"))
            # conn.execute(text(f"LOCK TABLES `{tablename}` WRITE"))
            # conn.execute(text(f"ALTER TABLE `{tablename}` DISABLE KEYS"))

        except sqlalchemy.exc.InvalidRequestError as e:
            print(f"ERROR: Invalid sql setup request for {tablename}: {e}", file=sys.stderr)
            raise e from None


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


def load_listfile(engine, file: Path, datameta: Table) -> int:
    print(f"Loading listfile {file}...", end="", flush=True)
    time_prep_start = time.monotonic()

    rows = []
    with file.open("r", newline="") as listfile:
        for line in [z.rstrip() for z in listfile]:
            s = line.split(";")
            if len(s) != 2:
                print(f"ERROR: Invalid line in listfile: {line}", file=sys.stderr)
                continue

            rows.append({
                "ID": int(s[0]),
                "Filepath": os.path.dirname(s[1]),
                "Filename": os.path.basename(s[1]),
            })

    time_prep_end = time.monotonic()
    print(f" {len(rows)} rows, prepaired in {time_prep_end - time_prep_start:.2f}s", end="", flush=True)

    insert_rows(engine, file, "FileData", rows, datameta)
    return len(rows)


def load_one(engine, file: Path, tablename: str, analysis: AnalysisData, datameta: Table) -> int:
    print(f"Loading {tablename}...", end="", flush=True)

    time_prep_start = time.monotonic()

    rows = []
    negnullcols: Set[str] = set()
    zeronullcols: Set[str] = set()

    with file.open("r", newline="") as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')

        assert reader.fieldnames is not None
        for c in reader.fieldnames:
            cc = analysis.for_column(DbdColumnId(tablename, c))
            if cc and "IGNORE_FK" not in cc.tags:
                if "NEG_IS_NULL" in cc.tags:
                    negnullcols.add(c)
                if "ZERO_IS_NULL" in cc.tags:
                    zeronullcols.add(c)

        for row in reader:
            rows.append({k: None if (k in negnullcols and int(v) < 0) or (k in zeronullcols and int(v) == 0)
                         else v for k, v in row.items()})

    time_prep_end = time.monotonic()
    print(f" {len(rows)} rows, prepaired in {time_prep_end - time_prep_start:.2f}s", end="", flush=True)

    insert_rows(engine, file, tablename, rows, datameta)

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
        "--no-data", dest="no_data", action='store_true', default=False,
        help="don't load DBD data (from csv files)")
    parser.add_argument(
        "--build", dest="build", type=build_string_regex, default="9.2.0.42423",
        help="full build number to use for parsing")
    parser.add_argument(
        "--connect-string", dest="connect_string", type=str, action='store',
        default="mysql+pymysql://root@localhost/wowdbd")
    parser.add_argument(
        "--listfile", dest="listfile", type=Path, action='store', default="listfile.csv",
        help="listfile to load")
    parser.add_argument(
        "--no-filedata", "--no-fdid", dest="no_filedata", action='store_true', default=False,
        help="don't load FileData table (from listfile)")
    parser.add_argument(
        "table", nargs='*', action='store',
        help="optional list of tables to load"
    )

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
        print("ERROR: Table _dbd_data_meta not found", file=sys.stderr)
        return 0


    # for file in sorted(os.listdir(args.datadir)):
    #     filename = os.fsdecode(file)
    #     if filename.endswith(".csv"):
    #         table_name = filename.replace(".csv", "")
    #         load_one(engine, args.datadir, table_name, analysis)

    t_start = time.monotonic()
    num_rows = 0
    num_tables = 0

    if not args.no_filedata:
        if not args.listfile.is_file():
            print(f"WARNING: Listfile '{args.listfile}' not found", file=sys.stderr)
        else:
            load_listfile(engine, args.listfile, data_meta)

    if not args.no_data:
        print(f"Loading data for build {build}...")

        if args.table:
            tablelist = args.table
        else:
            tablelist = sorted(analysis.tablenames())

        for tablename in tablelist:
            # for tablename in ["SpellVisualKitModelAttach"]:
            file = args.datadir / (tablename + ".csv")
            if file.exists():
                num_rows += load_one(engine, file, tablename, analysis, data_meta)
                num_tables += 1
            else:
                print(f"WARNING: No file for {tablename}", file=sys.stderr)

        t_end = time.monotonic()
        print(
            f"DONE. Loaded {num_rows} rows for {num_tables} tables in {t_end - t_start:.2f}s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
