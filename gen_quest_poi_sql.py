#!/usr/bin/env python3
"""
gen_quest_poi_sql.py

Reads Wago DB2 QuestPOIBlob and QuestPOIPoint CSVs and generates INSERT IGNORE SQL
to fill missing quest_poi and quest_poi_points rows in the world database.

NOTE: BlobIndex is hardcoded to 0 for all generated rows. TrinityCore uses
BlobIndex as an always-zero field (the per-quest sequencing is done via Idx1).
The DB2 data does not contain a BlobIndex; the value 0 matches stock TC behavior.

Mapping rules (verified against live DB + TC source ObjectMgr::LoadQuestPOI):
  quest_poi:
    QuestID         = blob.QuestID
    BlobIndex       = 0 (always for new inserts; TC uses per-quest sequential index)
    Idx1            = per-quest 0-based rank of the blob, sorted by CSV blob ID ascending
    ObjectiveIndex  = blob.ObjectiveIndex
    QuestObjectiveID= blob.ObjectiveID
    QuestObjectID   = 0
    MapID           = blob.MapID
    UiMapID         = blob.UiMapID
    Priority        = 0
    Flags           = blob.Flags
    WorldEffectID   = 0
    PlayerConditionID = blob.PlayerConditionID
    NavigationPlayerConditionID = blob.NavigationPlayerConditionID
    SpawnTrackingID = 0
    AlwaysAllowMergingBlobs = 0
    VerifiedBuild   = 66192

  quest_poi_points:
    QuestID  = quest_id of the parent blob
    Idx1     = same Idx1 as the parent blob row in quest_poi
    Idx2     = 0-based index of the point within this blob, sorted by CSV point ID
    X, Y, Z  = point.X, point.Y, point.Z
    VerifiedBuild = 66192
"""

import argparse
import csv
import os
import sys
from collections import defaultdict

# ── paths ─────────────────────────────────────────────────────────────────────
BLOB_CSV   = "C:/Users/atayl/source/wago/wago_csv/major_12/12.0.1.66192/enUS/QuestPOIBlob-enUS.csv"
POINT_CSV  = "C:/Users/atayl/source/wago/wago_csv/major_12/12.0.1.66192/enUS/QuestPOIPoint-enUS.csv"
EXISTING_POI        = "C:/Users/atayl/source/wago/raidbots/existing_quest_poi.txt"
EXISTING_POI_POINTS = "C:/Users/atayl/source/wago/raidbots/existing_quest_poi_points.txt"
EXISTING_QUESTS     = "C:/Users/atayl/source/wago/raidbots/existing_quest_template.txt"
OUT_DIR    = "C:/Users/atayl/source/wago/raidbots/sql_output"
OUT_POI    = os.path.join(OUT_DIR, "quest_poi_import.sql")
OUT_POINTS = os.path.join(OUT_DIR, "quest_poi_points_import.sql")

VERIFIED_BUILD = 66192
BATCH_SIZE     = 500

# ── load existing keys ────────────────────────────────────────────────────────
def load_existing_poi(path):
    """Returns set of (QuestID, BlobIndex) tuples that already exist in DB."""
    keys = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('mysql:') or line.startswith('WARNING'):
                continue
            parts = line.split('\t')
            if len(parts) == 2:
                try:
                    keys.add((int(parts[0]), int(parts[1])))
                except ValueError:
                    continue
    return keys

def load_existing_poi_points(path):
    """Returns set of (QuestID, Idx1, Idx2) tuples that already exist in DB."""
    keys = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('mysql:') or line.startswith('WARNING'):
                continue
            parts = line.split('\t')
            if len(parts) == 3:
                try:
                    keys.add((int(parts[0]), int(parts[1]), int(parts[2])))
                except ValueError:
                    continue
    return keys

def load_existing_quests(path):
    """Returns set of quest IDs present in quest_template."""
    ids = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('mysql:') or line.startswith('WARNING'):
                continue
            try:
                ids.add(int(line))
            except ValueError:
                continue
    return ids

# ── load CSV data ─────────────────────────────────────────────────────────────
def load_blobs(path):
    """
    Returns dict: quest_id -> list of blob dicts, sorted by blob CSV ID ascending.
    Each blob dict has keys: id, map_id, ui_map_id, flags, quest_id,
    objective_index, objective_id, player_condition_id, navigation_player_condition_id
    """
    by_quest = defaultdict(list)
    total = 0
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            total += 1
            by_quest[int(row['QuestID'])].append({
                'id':                            int(row['ID']),
                'map_id':                        int(row['MapID']),
                'ui_map_id':                     int(row['UiMapID']),
                'flags':                         int(row['Flags']),
                'quest_id':                      int(row['QuestID']),
                'objective_index':               int(row['ObjectiveIndex']),
                'objective_id':                  int(row['ObjectiveID']),
                'player_condition_id':           int(row['PlayerConditionID']),
                'navigation_player_condition_id':int(row['NavigationPlayerConditionID']),
            })
    # sort each quest's blobs by their CSV ID (ascending) to establish stable Idx1
    for qid in by_quest:
        by_quest[qid].sort(key=lambda b: b['id'])
    return by_quest, total

def load_points(path):
    """
    Returns dict: blob_id -> list of point dicts sorted by point CSV ID ascending.
    Each point: {id, x, y, z, blob_id}
    """
    by_blob = defaultdict(list)
    total = 0
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            total += 1
            by_blob[int(row['QuestPOIBlobID'])].append({
                'id':      int(row['ID']),
                'x':       int(row['X']),
                'y':       int(row['Y']),
                'z':       int(row['Z']),
                'blob_id': int(row['QuestPOIBlobID']),
            })
    # sort points within each blob by CSV ID for stable Idx2
    for bid in by_blob:
        by_blob[bid].sort(key=lambda p: p['id'])
    return by_blob, total

# ── SQL helpers ───────────────────────────────────────────────────────────────
POI_COLS    = ("`QuestID`, `BlobIndex`, `Idx1`, `ObjectiveIndex`, `QuestObjectiveID`, "
               "`QuestObjectID`, `MapID`, `UiMapID`, `Priority`, `Flags`, `WorldEffectID`, "
               "`PlayerConditionID`, `NavigationPlayerConditionID`, `SpawnTrackingID`, "
               "`AlwaysAllowMergingBlobs`, `VerifiedBuild`")

POINT_COLS  = "`QuestID`, `Idx1`, `Idx2`, `X`, `Y`, `Z`, `VerifiedBuild`"

def write_batches(f, table, cols, rows, batch_size):
    """Write INSERT IGNORE batches of batch_size rows each."""
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        f.write(f"INSERT IGNORE INTO `{table}` ({cols}) VALUES\n")
        for j, row in enumerate(batch):
            comma = "," if j < len(batch) - 1 else ";"
            f.write(f"{row}{comma}\n")
        f.write("\n")

# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Generate quest POI SQL from Wago DB2 CSVs")
    parser.add_argument("--force", action="store_true",
                        help="Skip dedup entirely — insert all CSV rows regardless of existing DB state. "
                             "Use after DELETE FROM quest_poi / quest_poi_points for a clean reimport.")
    args = parser.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)

    print("Loading existing DB keys...")
    existing_poi        = load_existing_poi(EXISTING_POI)
    existing_poi_points = load_existing_poi_points(EXISTING_POI_POINTS)
    existing_quests     = load_existing_quests(EXISTING_QUESTS)

    print(f"  existing quest_poi rows:        {len(existing_poi):>8,}")
    print(f"  existing quest_poi_points rows: {len(existing_poi_points):>8,}")
    print(f"  quests in quest_template:       {len(existing_quests):>8,}")

    print("\nLoading CSV data...")
    blobs_by_quest, total_blobs = load_blobs(BLOB_CSV)
    points_by_blob, total_points = load_points(POINT_CSV)

    print(f"  QuestPOIBlob CSV rows:  {total_blobs:>8,}")
    print(f"  QuestPOIPoint CSV rows: {total_points:>8,}")

    # ── build quest_poi inserts ──────────────────────────────────────────────
    # DEDUP LIMITATION: The existing_poi export file has (QuestID, BlobIndex) pairs
    # but NOT Idx1. The true PK is (QuestID, BlobIndex, Idx1). Because we lack
    # per-Idx1 granularity, dedup is CONSERVATIVE: any quest that already has at
    # least one BlobIndex=0 row in the DB is skipped entirely. This means partially
    # imported quests won't be completed on re-run.
    #
    # To handle partial imports, either:
    #   1. DELETE the quest's rows and re-run, or
    #   2. Use --force to skip dedup entirely (after DELETE FROM quest_poi / quest_poi_points)
    #   3. Re-export with Idx1 included: SELECT QuestID, BlobIndex, Idx1 FROM quest_poi

    if args.force:
        quests_with_existing_poi = set()
        print("  --force: skipping dedup, all CSV quests will be inserted")
    else:
        quests_with_existing_poi = set()
        for (qid, bidx) in existing_poi:
            if bidx == 0:
                quests_with_existing_poi.add(qid)

    print(f"\n  Quests with existing BlobIndex=0 poi rows: {len(quests_with_existing_poi):,}")

    # quests skipped because not in quest_template
    quests_in_csv = set(blobs_by_quest.keys())
    # Exclude QuestID=0 — it is a placeholder row in quest_template ("Quests") and
    # the 3,000+ blobs with QuestID=0 in the DB2 CSV are unlinked navigation paths.
    quests_in_csv.discard(0)
    quests_not_in_qt = quests_in_csv - existing_quests
    quests_to_process = (quests_in_csv & existing_quests) - quests_with_existing_poi

    # build the set of (blob_id -> quest_id, idx1) so we know which points to include
    # blob_id_to_idx1[blob_id] = (quest_id, idx1)
    blob_id_to_idx1 = {}
    for qid, blobs in blobs_by_quest.items():
        for idx1, blob in enumerate(blobs):
            blob_id_to_idx1[blob['id']] = (qid, idx1)

    # ── build poi rows ───────────────────────────────────────────────────────
    poi_rows = []
    blobs_skipped_existing = 0
    blobs_skipped_no_qt    = 0
    distinct_quests_poi    = set()

    # We need per-blob Idx1 tracking for all quests (even already-existing ones)
    # because points reference blob IDs
    all_new_blob_ids   = set()  # blob IDs we are inserting
    all_valid_blob_ids = set()  # blob IDs that are in DB (existing) or being inserted

    # Compute valid blob IDs for existing quests (to allow their points to be inserted)
    # For existing quests (quests_with_existing_poi) we assume ALL their blobs exist in DB
    for qid in quests_with_existing_poi:
        if qid in blobs_by_quest:
            for blob in blobs_by_quest[qid]:
                all_valid_blob_ids.add(blob['id'])

    for qid in sorted(quests_to_process):
        blobs = blobs_by_quest[qid]
        for idx1, blob in enumerate(blobs):
            poi_rows.append(
                f"({qid}, 0, {idx1}, {blob['objective_index']}, "
                f"{blob['objective_id']}, 0, "
                f"{blob['map_id']}, {blob['ui_map_id']}, 0, "
                f"{blob['flags']}, 0, "
                f"{blob['player_condition_id']}, "
                f"{blob['navigation_player_condition_id']}, "
                f"0, 0, {VERIFIED_BUILD})"
            )
            all_new_blob_ids.add(blob['id'])
            all_valid_blob_ids.add(blob['id'])
        distinct_quests_poi.add(qid)

    blobs_skipped_existing = sum(
        len(blobs_by_quest[qid]) for qid in quests_with_existing_poi
        if qid in blobs_by_quest
    )
    blobs_skipped_no_qt = sum(
        len(blobs_by_quest[qid]) for qid in quests_not_in_qt
    )

    # ── build poi_points rows ────────────────────────────────────────────────
    # Insert points whose parent blob is being inserted (all_new_blob_ids)
    # Skip points that already exist in existing_poi_points
    point_rows = []
    points_skipped_existing  = 0
    points_skipped_no_blob   = 0
    distinct_quests_points   = set()

    for blob_id, points in points_by_blob.items():
        if blob_id not in all_new_blob_ids:
            if blob_id not in all_valid_blob_ids:
                points_skipped_no_blob += len(points)
            continue
        if blob_id not in blob_id_to_idx1:
            points_skipped_no_blob += len(points)
            continue
        quest_id, idx1 = blob_id_to_idx1[blob_id]
        for idx2, pt in enumerate(points):
            key = (quest_id, idx1, idx2)
            if key in existing_poi_points:
                points_skipped_existing += 1
                continue
            point_rows.append(
                f"({quest_id}, {idx1}, {idx2}, {pt['x']}, {pt['y']}, {pt['z']}, {VERIFIED_BUILD})"
            )
            distinct_quests_points.add(quest_id)

    # ── write SQL files ──────────────────────────────────────────────────────
    with open(OUT_POI, 'w', encoding='utf-8') as f:
        f.write("-- Quest POI import generated by gen_quest_poi_sql.py\n")
        f.write(f"-- Source: QuestPOIBlob-enUS.csv (build {VERIFIED_BUILD})\n")
        f.write(f"-- Rows to insert: {len(poi_rows):,}\n")
        f.write(f"-- Distinct quests: {len(distinct_quests_poi):,}\n")
        f.write(f"-- Skipped (quest not in quest_template): {blobs_skipped_no_qt:,}\n")
        f.write(f"-- Skipped (already in DB): {blobs_skipped_existing:,}\n")
        f.write(f"-- NOTE: BlobIndex is hardcoded to 0 for all rows (matches stock TC behavior).\n")
        f.write("\nUSE `world`;\n\n")
        if poi_rows:
            f.write("SET autocommit = 0;\nSTART TRANSACTION;\n\n")
            write_batches(f, "quest_poi", POI_COLS, poi_rows, BATCH_SIZE)
            f.write("COMMIT;\nSET autocommit = 1;\n")
        else:
            f.write("-- No new rows to insert.\n")

    with open(OUT_POINTS, 'w', encoding='utf-8') as f:
        f.write("-- Quest POI Points import generated by gen_quest_poi_sql.py\n")
        f.write(f"-- Source: QuestPOIPoint-enUS.csv (build {VERIFIED_BUILD})\n")
        f.write(f"-- Rows to insert: {len(point_rows):,}\n")
        f.write(f"-- Distinct quests: {len(distinct_quests_points):,}\n")
        f.write(f"-- Skipped (already in DB): {points_skipped_existing:,}\n")
        f.write(f"-- Skipped (parent blob not being inserted): {points_skipped_no_blob:,}\n")
        f.write(f"-- NOTE: BlobIndex is hardcoded to 0 for all parent POI rows (matches stock TC behavior).\n")
        f.write("\nUSE `world`;\n\n")
        if point_rows:
            f.write("SET autocommit = 0;\nSTART TRANSACTION;\n\n")
            write_batches(f, "quest_poi_points", POINT_COLS, point_rows, BATCH_SIZE)
            f.write("COMMIT;\nSET autocommit = 1;\n")
        else:
            f.write("-- No new rows to insert.\n")

    # ── report ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("QUEST POI IMPORT REPORT")
    print("=" * 62)
    print(f"\n{'CSV INPUT':}")
    print(f"  QuestPOIBlob rows in CSV:           {total_blobs:>8,}")
    print(f"  QuestPOIPoint rows in CSV:          {total_points:>8,}")
    print(f"  Distinct quests in CSV:             {len(quests_in_csv):>8,}")
    print(f"\n{'FILTERING':}")
    print(f"  Quests filtered (not in QT):        {len(quests_not_in_qt):>8,}")
    print(f"  Blobs filtered (quest not in QT):   {blobs_skipped_no_qt:>8,}")
    print(f"  Quests skipped (already in DB):     {len(quests_with_existing_poi):>8,}")
    print(f"  Blobs skipped (already in DB):      {blobs_skipped_existing:>8,}")
    print(f"\n{'quest_poi OUTPUT':}")
    print(f"  Already in DB:                      {len(existing_poi):>8,}")
    print(f"  New rows to insert:                 {len(poi_rows):>8,}")
    print(f"  Distinct quests affected:           {len(distinct_quests_poi):>8,}")
    print(f"\n{'quest_poi_points OUTPUT':}")
    print(f"  Already in DB:                      {len(existing_poi_points):>8,}")
    print(f"  New rows to insert:                 {len(point_rows):>8,}")
    print(f"  Skipped (parent not inserted):      {points_skipped_no_blob:>8,}")
    print(f"  Distinct quests affected:           {len(distinct_quests_points):>8,}")
    print(f"\n{'OUTPUT FILES':}")
    print(f"  {OUT_POI}")
    print(f"  {OUT_POINTS}")
    print("=" * 62)

if __name__ == "__main__":
    main()
