#!/usr/bin/env python3
import csv
import os
import re
import sys
import typing

from ppretty import ppretty

import sqlalchemy
from sqlalchemy import MetaData, Table, create_engine, insert, text

directory = "dbd-920dbcs41257"
dbstring = "mysql+pymysql://root@localhost/wowdbd"
table = "AdventureJournal"

array_re = re.compile(r"\[(.*?)\]")
def load_one(engine, directory: str, tablename: str):
    print(f"Loading {tablename}...")
    file = tablename + ".csv"

    # with engine.begin() as conn:
    #     conn.execute(
    #         text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
    #         [{"x": 11, "y": 12}, {"x": 13, "y": 14}]
    #     )
    #     # conn.execute(things)
    #     pass
    rows = []
    with open(os.path.join(directory, file), newline="") as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            # prow = {re.sub(array_re, r"__\g<1>", k): v for k, v in row.items()}
            prow = {k: v for k, v in row.items()}
            rows.append(prow)

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


def main():
    engine = create_engine(dbstring, echo=False, future=True)
    # conn = engine.connect()
    # result = conn.execute(text(f"SET foreign_key_checks = 0"))
    # result = conn.execute(text(f"TRUNCATE {table}"))
    # print(result.all())
    # metadata_obj = MetaData()
    # some_table = Table("Achievement", metadata_obj, autoload_with=engine)
    # print(ppretty(some_table))

    table = "Lock"
    load_one(engine, directory, table)
    # return 0
    # db = records.Database('mysql+pymysql://root@localhost/wowdbd')

    for file in sorted(os.listdir(directory)):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            table_name = filename.replace(".csv", "")
            load_one(engine, directory, table_name)
            # continue


main()
