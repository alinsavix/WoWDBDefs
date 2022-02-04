#!/usr/bin/env python3
import csv
import os
import re
import sys
from typing import Dict, Any

from ppretty import ppretty

# sqlalchemy is mostly overkill, but it makes things easy.
import sqlalchemy
from sqlalchemy import MetaData, Table, create_engine, insert, text
from dataclasses import dataclass
from dbdwrapper import DbdColumnId
from dbdanalyze import load_analysis, AnalysisData


directory = "dbd-920dbcs41257"
dbstring = "mysql+pymysql://root@localhost/wowdbd"
# table = "AdventureJournal"

array_re = re.compile(r"\[\d+\]$")

def load_one(engine, directory: str, tablename: str, analysis: AnalysisData):
    print(f"Loading {tablename}...")
    file = tablename + ".csv"

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

    # print(ppretty(rows, seq_length=999999))

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


def main() -> int:
    analysis = load_analysis("analysis.csv")
    engine = create_engine(dbstring, echo=False, future=True)

    for file in sorted(os.listdir(directory)):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            table_name = filename.replace(".csv", "")
            load_one(engine, directory, table_name, analysis)

    return 0


if __name__ == "__main__":
    sys.exit(main())
