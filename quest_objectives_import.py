#!/usr/bin/env python3
"""
quest_objectives_import.py

Reads QuestObjective-enUS.csv from Wago DB2 exports and generates SQL INSERT
statements for any rows missing from world.quest_objectives.

CSV columns  : ID, Description_lang, Type, Amount, ObjectID, OrderIndex,
               Flags, StorageIndex, Field_12_0_0_63534_007, QuestID
DB columns   : ID, QuestID, Type, Order, StorageIndex, ObjectID, Amount,
               ConditionalAmount, Flags, Flags2, ProgressBarWeight,
               ParentObjectiveID, Visible, Description, VerifiedBuild

Column mapping:
  CSV ID            -> DB ID
  CSV QuestID       -> DB QuestID
  CSV Type          -> DB Type
  CSV OrderIndex    -> DB Order
  CSV StorageIndex  -> DB StorageIndex
  CSV ObjectID      -> DB ObjectID
  CSV Amount        -> DB Amount
  CSV Flags         -> DB Flags
  CSV Description_lang -> DB Description
  (all others)      -> DB defaults (0 / NULL / 1 for Visible, 66192 for VerifiedBuild)

NOTE on Field_12_0_0_63534_007: This unnamed field is present in the DB2 CSV but is
always 0 across all 10,492 rows in build 66192. It has no corresponding column in
world.quest_objectives and is intentionally ignored. If Blizzard populates it in a
future build, revisit whether it maps to Flags2 or ConditionalAmount.
"""

import csv
import subprocess
import sys
import os
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CSV_PATH = r"C:/Users/atayl/source/wago/wago_csv/major_12/12.0.1.66192/enUS/QuestObjective-enUS.csv"
OUTPUT_PATH = r"C:/Users/atayl/source/wago/raidbots/sql_output/quest_objectives_import.sql"
MYSQL_BIN = r"C:/Program Files/MySQL/MySQL Server 8.0/bin/mysql.exe"
MYSQL_ARGS = [MYSQL_BIN, "-u", "root", "-padmin"]
DB_NAME = "world"
TABLE = "quest_objectives"
VERIFIED_BUILD = 66192
BATCH_SIZE = 500

# ---------------------------------------------------------------------------
# MySQL helpers
# ---------------------------------------------------------------------------

def mysql_query(sql: str) -> str:
    """Run a SQL query and return stdout as a string."""
    cmd = MYSQL_ARGS + [DB_NAME, "-e", sql, "--skip-column-names", "--batch"]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        print(f"[ERROR] MySQL error: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def fetch_existing_ids() -> set[int]:
    """Return the set of IDs already present in world.quest_objectives."""
    print("[*] Fetching existing IDs from world.quest_objectives ...", flush=True)
    raw = mysql_query(f"SELECT ID FROM {TABLE};")
    ids: set[int] = set()
    for line in raw.splitlines():
        line = line.strip()
        if line:
            try:
                ids.add(int(line))
            except ValueError:
                pass
    print(f"    -> {len(ids):,} existing rows found", flush=True)
    return ids

# ---------------------------------------------------------------------------
# String escaping
# ---------------------------------------------------------------------------

def escape_sql_string(value: str) -> str:
    """Escape a string for use inside single quotes in a MySQL INSERT."""
    # Escape backslash first, then quotes, then control chars
    value = value.replace("\\", "\\\\")
    value = value.replace("'", "\\'")
    value = value.replace('"', '\\"')
    value = value.replace("\n", "\\n")
    value = value.replace("\r", "\\r")
    value = value.replace("\x00", "\\0")
    value = value.replace("\x1a", "\\Z")
    return value


def sql_str_or_null(value: str) -> str:
    """Return SQL representation: NULL if empty, else 'escaped string'."""
    if value == "" or value is None:
        return "NULL"
    return f"'{escape_sql_string(value)}'"

# ---------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------

def read_csv(path: str) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
    return rows

# ---------------------------------------------------------------------------
# Row -> SQL value tuple
# ---------------------------------------------------------------------------

def row_to_values(row: dict) -> str:
    """
    Build the VALUES(...) string for a single DB row.

    DB column order:
      ID, QuestID, Type, Order, StorageIndex, ObjectID, Amount,
      ConditionalAmount, Flags, Flags2, ProgressBarWeight,
      ParentObjectiveID, Visible, Description, VerifiedBuild
    """
    def intval(key: str, default: int = 0) -> int:
        v = row.get(key, "").strip()
        if v == "":
            return default
        try:
            return int(v)
        except ValueError:
            return default

    obj_id       = intval("ID")
    quest_id     = intval("QuestID")
    obj_type     = intval("Type")
    order_index  = intval("OrderIndex")
    storage_idx  = intval("StorageIndex")
    object_id    = intval("ObjectID")
    amount       = intval("Amount")
    flags        = intval("Flags")
    description  = sql_str_or_null(row.get("Description_lang", "").strip())

    # Defaults for columns not present in the CSV
    conditional_amount  = 0
    flags2              = 0
    progress_bar_weight = 0.0
    parent_objective_id = 0
    visible             = 1

    return (
        f"({obj_id},{quest_id},{obj_type},{order_index},{storage_idx},"
        f"{object_id},{amount},{conditional_amount},{flags},{flags2},"
        f"{progress_bar_weight},{parent_objective_id},{visible},"
        f"{description},{VERIFIED_BUILD})"
    )

# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

INSERT_HEADER = (
    "INSERT IGNORE INTO `quest_objectives` "
    "(`ID`,`QuestID`,`Type`,`Order`,`StorageIndex`,`ObjectID`,`Amount`,"
    "`ConditionalAmount`,`Flags`,`Flags2`,`ProgressBarWeight`,"
    "`ParentObjectiveID`,`Visible`,`Description`,`VerifiedBuild`)\nVALUES\n"
)


def write_sql(new_rows: list[dict], output_path: str,
              total_csv: int, already_in_db: int) -> None:
    new_count = len(new_rows)
    distinct_quests = len({r["QuestID"] for r in new_rows})

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="\n") as fh:
        # Header comment
        fh.write("-- quest_objectives_import.sql\n")
        fh.write(f"-- Generated by quest_objectives_import.py\n")
        fh.write(f"-- Source: QuestObjective-enUS.csv (build {VERIFIED_BUILD})\n")
        fh.write(f"-- Total CSV rows     : {total_csv:,}\n")
        fh.write(f"-- Already in DB      : {already_in_db:,}\n")
        fh.write(f"-- New rows to insert : {new_count:,}\n")
        fh.write(f"-- Distinct quests    : {distinct_quests:,}\n")
        fh.write("--\n\n")

        if new_count == 0:
            fh.write("-- Nothing to insert — all CSV rows already exist in the DB.\n")
            return

        fh.write("USE `world`;\n\n")
        fh.write("SET NAMES utf8mb4;\n\n")
        fh.write("SET autocommit = 0;\nSTART TRANSACTION;\n\n")

        # Write in batches
        for batch_start in range(0, new_count, BATCH_SIZE):
            batch = new_rows[batch_start : batch_start + BATCH_SIZE]
            values_list = [row_to_values(r) for r in batch]
            fh.write(INSERT_HEADER)
            fh.write(",\n".join(values_list))
            fh.write(";\n\n")

        fh.write("COMMIT;\nSET autocommit = 1;\n")

    print(f"\n[+] Output written to: {output_path}")
    print(f"    Batches: {((new_count - 1) // BATCH_SIZE) + 1} x {BATCH_SIZE}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"[*] Reading CSV: {CSV_PATH}", flush=True)
    csv_rows = read_csv(CSV_PATH)
    total_csv = len(csv_rows)
    print(f"    -> {total_csv:,} rows in CSV", flush=True)

    existing_ids = fetch_existing_ids()
    already_in_db = 0
    new_rows = []
    skipped_bad_id = 0
    skipped_bad_type = 0
    for r in csv_rows:
        try:
            rid = int(r["ID"])
        except (ValueError, KeyError):
            skipped_bad_id += 1
            continue
        # Validate Type is in the known range (0..22 per QuestDef.h MAX_QUEST_OBJECTIVE_TYPE)
        obj_type = int(r.get("Type", "0") or "0")
        if obj_type < 0 or obj_type > 22:
            quest_id = r.get("QuestID", "?")
            print(f"  WARNING: Quest {quest_id} objective ID={rid} has invalid Type={obj_type}, skipping")
            skipped_bad_type += 1
            continue
        if rid in existing_ids:
            already_in_db += 1
        else:
            new_rows.append(r)
    if skipped_bad_id:
        print(f"    Skipped {skipped_bad_id} rows with non-integer ID", flush=True)
    if skipped_bad_type:
        print(f"    Skipped {skipped_bad_type} rows with out-of-range Type", flush=True)
    new_count = len(new_rows)
    distinct_quests = len({r["QuestID"] for r in new_rows})

    print(f"\n[*] Summary:")
    print(f"    Total CSV rows     : {total_csv:,}")
    print(f"    Already in DB      : {already_in_db:,}")
    print(f"    New rows to insert : {new_count:,}")
    print(f"    Distinct quests    : {distinct_quests:,}")

    write_sql(new_rows, OUTPUT_PATH, total_csv, already_in_db)
    print("\n[done]")


if __name__ == "__main__":
    main()
