#!/usr/bin/env python3
"""
quest_chain_gen.py
Reads QuestLineXQuest and QuestLine DB2 CSVs and generates SQL UPDATE statements
to populate PrevQuestID / NextQuestID in world.quest_template_addon.

Rules:
- Only UPDATE quests that already exist in quest_template_addon
- Only set PrevQuestID where it is currently 0 (don't overwrite existing data)
- Only set NextQuestID where it is currently 0 (don't overwrite existing data)
- First quest in a chain: PrevQuestID = 0 (no update needed unless already 0)
- Last quest in a chain:  NextQuestID = 0 (no update needed)
- Quests appearing in multiple chains with conflicting prev/next are resolved
  by keeping the lowest QuestLine ID's value (with a warning)
- Cycles in the chain graph are detected and broken
"""

import csv
import subprocess
import sys
import os
from collections import defaultdict
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
WAGO_BASE = "C:/Users/atayl/source/wago/wago_csv/major_12/12.0.1.66192/enUS"
QUEST_LINE_XQUEST_CSV  = os.path.join(WAGO_BASE, "QuestLineXQuest-enUS.csv")
QUEST_LINE_CSV         = os.path.join(WAGO_BASE, "QuestLine-enUS.csv")
OUTPUT_SQL             = "C:/Users/atayl/source/wago/raidbots/sql_output/quest_chains.sql"

MYSQL = "C:/Program Files/MySQL/MySQL Server 8.0/bin/mysql.exe"
MYSQL_ARGS = ["-u", "root", "-padmin"]

# ---------------------------------------------------------------------------
# MySQL helper
# ---------------------------------------------------------------------------
def mysql_query(sql: str) -> list[str]:
    """Run a SQL query and return lines of output (tab-separated)."""
    result = subprocess.run(
        [MYSQL] + MYSQL_ARGS + ["--skip-column-names", "--batch", "-e", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[ERROR] MySQL query failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    lines = [l for l in result.stdout.splitlines() if l.strip()]
    return lines

# ---------------------------------------------------------------------------
# Load existing quest_template_addon data
# ---------------------------------------------------------------------------
def load_addon_state() -> dict[int, tuple[int, int]]:
    """
    Returns dict: quest_id -> (current_PrevQuestID, current_NextQuestID)
    Only for rows that exist in the table.
    """
    print("[*] Loading quest_template_addon from DB...", flush=True)
    rows = mysql_query("SELECT ID, PrevQuestID, NextQuestID FROM world.quest_template_addon;")
    state: dict[int, tuple[int, int]] = {}
    for row in rows:
        parts = row.split("\t")
        if len(parts) < 3:
            continue
        qid      = int(parts[0])
        prev_val = int(parts[1])
        next_val = int(parts[2])
        state[qid] = (prev_val, next_val)
    print(f"    Loaded {len(state):,} rows.", flush=True)
    return state

# ---------------------------------------------------------------------------
# Load QuestLine names
# ---------------------------------------------------------------------------
def load_quest_lines() -> dict[int, str]:
    """Returns dict: line_id -> Name_lang"""
    print("[*] Loading QuestLine CSV...", flush=True)
    names: dict[int, str] = {}
    with open(QUEST_LINE_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                line_id = int(row["ID"])
                names[line_id] = row.get("Name_lang", f"QuestLine {line_id}").strip()
            except (ValueError, KeyError):
                continue
    print(f"    Loaded {len(names):,} quest lines.", flush=True)
    return names

# ---------------------------------------------------------------------------
# Load QuestLineXQuest, build per-line ordered quest lists
# ---------------------------------------------------------------------------
def load_quest_line_x_quest() -> dict[int, list[tuple[int, int]]]:
    """
    Returns dict: line_id -> sorted list of (order_index, quest_id)
    """
    print("[*] Loading QuestLineXQuest CSV...", flush=True)
    lines: dict[int, list[tuple[int, int]]] = defaultdict(list)
    total = 0
    with open(QUEST_LINE_XQUEST_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                line_id     = int(row["QuestLineID"])
                quest_id    = int(row["QuestID"])
                order_index = int(row["OrderIndex"])
            except (ValueError, KeyError):
                continue
            lines[line_id].append((order_index, quest_id))
            total += 1

    # Sort each line by OrderIndex
    for line_id in lines:
        lines[line_id].sort(key=lambda x: x[0])

    print(f"    Loaded {total:,} quest-line entries across {len(lines):,} chains.", flush=True)
    return dict(lines)

# ---------------------------------------------------------------------------
# Cycle detection via DFS on the directed graph
# ---------------------------------------------------------------------------
def detect_and_break_cycles(
    desired_updates: dict[int, tuple[int, int]],
) -> list[tuple[int, int, str]]:
    """
    Given desired_updates: quest_id -> (desired_prev, desired_next),
    build a directed graph from NextQuestID edges and detect cycles via DFS.
    Returns list of removed edges as (from_quest, to_quest, reason).
    Mutates desired_updates in place to remove cycle-causing edges.
    """
    # Build adjacency list from NextQuestID edges only (prev is the reverse)
    # An edge exists: quest_id -> desired_next
    graph: dict[int, int] = {}
    for qid, (d_prev, d_next) in desired_updates.items():
        if d_next != 0:
            graph[qid] = d_next

    removed_edges: list[tuple[int, int, str]] = []

    # DFS-based cycle detection (3-color: WHITE=unvisited, GRAY=in-stack, BLACK=done)
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[int, int] = defaultdict(int)  # default WHITE

    def dfs(node: int) -> bool:
        """Returns True if a cycle was found and broken starting from node."""
        color[node] = GRAY
        neighbor = graph.get(node)
        if neighbor is not None:
            if color[neighbor] == GRAY:
                # Found a cycle: edge node -> neighbor is the back edge
                # Remove this edge (set NextQuestID to 0 for node)
                reason = f"cycle: {node} -> {neighbor} (back edge)"
                removed_edges.append((node, neighbor, reason))
                # Mutate: clear the NextQuestID for the offending node
                old_prev, _ = desired_updates[node]
                desired_updates[node] = (old_prev, 0)
                del graph[node]
                # Also clear PrevQuestID on the neighbor if it pointed back to node
                if neighbor in desired_updates:
                    n_prev, n_next = desired_updates[neighbor]
                    if n_prev == node:
                        desired_updates[neighbor] = (0, n_next)
                return True
            elif color[neighbor] == WHITE:
                if dfs(neighbor):
                    return True
        color[node] = BLACK
        return False

    # Run DFS from all nodes
    all_nodes = set(graph.keys()) | set(graph.values())
    for node in all_nodes:
        if color[node] == WHITE:
            dfs(node)

    return removed_edges

# ---------------------------------------------------------------------------
# Cross-chain conflict detection
# ---------------------------------------------------------------------------
def detect_cross_chain_conflicts(
    quest_lines_data: dict[int, list[tuple[int, int]]],
    addon_state: dict[int, tuple[int, int]],
) -> tuple[dict[int, tuple[int, int, int]], list[str]]:
    """
    Compute desired (prev, next) per quest from each chain, detect conflicts
    where a quest appears in multiple chains with different desired values.

    Returns:
        resolved: dict quest_id -> (desired_prev, desired_next, winning_line_id)
        conflict_warnings: list of warning strings
    """
    # quest_id -> list of (questline_id, desired_prev, desired_next)
    quest_chain_map: dict[int, list[tuple[int, int, int]]] = defaultdict(list)

    for line_id, ordered in sorted(quest_lines_data.items()):
        # Fix 1: Deduplicate quest IDs within each chain (preserves order)
        quest_ids = list(dict.fromkeys(qid for (_idx, qid) in ordered))

        # Fix 4: Filter to quests present in addon table
        quest_ids_in_db = [qid for qid in quest_ids if qid in addon_state]

        if not quest_ids_in_db:
            continue

        for i, quest_id in enumerate(quest_ids_in_db):
            desired_prev = quest_ids_in_db[i - 1] if i > 0 else 0
            desired_next = quest_ids_in_db[i + 1] if i < len(quest_ids_in_db) - 1 else 0

            # Fix 2: Self-reference prevention
            # NOTE: Quests 36341 and 36342 have NextQuestID = their own ID in the
            # Wago QuestLineXQuest source CSV. The DFS cycle detector (Phase 2)
            # won't catch these because they're single-node self-refs set directly
            # from the CSV, not multi-node cycles built by chaining logic.
            # fix_quest_chains.sql handles them post-import — this is by design.
            if desired_prev == quest_id:
                desired_prev = 0
            if desired_next == quest_id:
                desired_next = 0

            quest_chain_map[quest_id].append((line_id, desired_prev, desired_next))

    # Resolve conflicts: lowest QuestLine ID wins
    resolved: dict[int, tuple[int, int, int]] = {}
    conflict_warnings: list[str] = []

    for quest_id, entries in sorted(quest_chain_map.items()):
        if len(entries) == 1:
            line_id, d_prev, d_next = entries[0]
            resolved[quest_id] = (d_prev, d_next, line_id)
            continue

        # Multiple chains claim this quest -- check for actual conflicts
        # Group by (desired_prev, desired_next) to see if they actually differ
        unique_values: dict[tuple[int, int], list[int]] = defaultdict(list)
        for line_id, d_prev, d_next in entries:
            unique_values[(d_prev, d_next)].append(line_id)

        if len(unique_values) == 1:
            # All chains agree on the same values -- no conflict
            (d_prev, d_next), line_ids = next(iter(unique_values.items()))
            resolved[quest_id] = (d_prev, d_next, min(line_ids))
            continue

        # Real conflict: different chains want different prev/next
        # Sort entries by line_id (already sorted since we iterate sorted keys)
        entries_sorted = sorted(entries, key=lambda e: e[0])
        winner_line, winner_prev, winner_next = entries_sorted[0]

        conflict_detail_parts = []
        for line_id, d_prev, d_next in entries_sorted:
            marker = " [WINNER]" if line_id == winner_line else ""
            conflict_detail_parts.append(
                f"    QuestLine {line_id}: prev={d_prev}, next={d_next}{marker}"
            )
        conflict_detail = "\n".join(conflict_detail_parts)
        warning = (
            f"  WARNING: Quest {quest_id} appears in {len(entries)} chains "
            f"with CONFLICTING values (keeping QuestLine {winner_line}):\n"
            f"{conflict_detail}"
        )
        conflict_warnings.append(warning)

        resolved[quest_id] = (winner_prev, winner_next, winner_line)

    return resolved, conflict_warnings

# ---------------------------------------------------------------------------
# Generate UPDATE statements
# ---------------------------------------------------------------------------
def generate_sql(
    quest_lines_data: dict[int, list[tuple[int, int]]],
    line_names: dict[int, str],
    addon_state: dict[int, tuple[int, int]],
) -> tuple[list[str], dict, list[str], list[tuple[int, int, str]]]:
    """
    Returns (sql_blocks, stats_dict, conflict_warnings, removed_edges)
    sql_blocks: ready-to-write SQL content blocks
    stats: summary counters
    conflict_warnings: cross-chain conflict warning strings
    removed_edges: cycle edges that were removed (from, to, reason)
    """
    stats = {
        "chains_total":          0,
        "chains_skipped_no_db":  0,
        "chains_with_updates":   0,
        "quests_in_db":          0,
        "quests_missing_db":     0,
        "prev_updates":          0,
        "next_updates":          0,
        "prev_skipped_existing": 0,
        "next_skipped_existing": 0,
        "cross_chain_conflicts": 0,
        "cycles_broken":         0,
        "dupes_removed":         0,
    }

    # -----------------------------------------------------------------------
    # Phase 1: Detect cross-chain conflicts and resolve desired values
    # -----------------------------------------------------------------------
    print("[*] Detecting cross-chain conflicts...", flush=True)
    resolved, conflict_warnings = detect_cross_chain_conflicts(
        quest_lines_data, addon_state
    )
    stats["cross_chain_conflicts"] = len(conflict_warnings)

    if conflict_warnings:
        print(f"    Found {len(conflict_warnings)} cross-chain conflict(s):", flush=True)
        for w in conflict_warnings:
            print(w, flush=True)
    else:
        print("    No cross-chain conflicts found.", flush=True)

    # Build desired_updates: quest_id -> (desired_prev, desired_next)
    desired_updates: dict[int, tuple[int, int]] = {
        qid: (d_prev, d_next)
        for qid, (d_prev, d_next, _line_id) in resolved.items()
    }

    # -----------------------------------------------------------------------
    # Phase 2: Cycle detection
    # -----------------------------------------------------------------------
    print("[*] Running cycle detection...", flush=True)
    removed_edges = detect_and_break_cycles(desired_updates)
    stats["cycles_broken"] = len(removed_edges)

    if removed_edges:
        print(f"    Broke {len(removed_edges)} cycle(s):", flush=True)
        for from_q, to_q, reason in removed_edges:
            print(f"    Removed edge: {from_q} -> {to_q} ({reason})", flush=True)
    else:
        print("    No cycles detected.", flush=True)

    # -----------------------------------------------------------------------
    # Phase 3: Generate SQL blocks per chain, using resolved desired values
    # -----------------------------------------------------------------------
    # Track seen UPDATE statements for deduplication
    seen_updates: set[str] = set()
    sql_blocks: list[str] = []

    for line_id, ordered in sorted(quest_lines_data.items()):
        stats["chains_total"] += 1
        line_name = line_names.get(line_id, f"QuestLine {line_id}")

        # Fix 1: Deduplicate quest IDs within each chain
        quest_ids = list(dict.fromkeys(qid for (_idx, qid) in ordered))

        # Fix 4: Filter to quests present in addon table
        quest_ids_in_db = [qid for qid in quest_ids if qid in addon_state]

        if not quest_ids_in_db:
            stats["chains_skipped_no_db"] += 1
            continue

        stats["quests_in_db"]      += len(quest_ids_in_db)
        stats["quests_missing_db"] += len(quest_ids) - len(quest_ids_in_db)

        # Build UPDATE statements for this chain
        block_lines: list[str] = []

        for i, quest_id in enumerate(quest_ids_in_db):
            current_prev, current_next = addon_state[quest_id]

            # Use the resolved desired values (conflict-resolved, cycle-clean)
            if quest_id not in desired_updates:
                continue
            desired_prev, desired_next = desired_updates[quest_id]

            # --- PrevQuestID ---
            if desired_prev != 0:
                if current_prev == 0:
                    stmt = (
                        f"UPDATE `quest_template_addon` SET `PrevQuestID` = {desired_prev} "
                        f"WHERE `ID` = {quest_id} AND `PrevQuestID` = 0;"
                    )
                    if stmt not in seen_updates:
                        seen_updates.add(stmt)
                        block_lines.append(
                            f"{stmt} "
                            f"-- chain pos {i+1}/{len(quest_ids_in_db)}"
                        )
                        stats["prev_updates"] += 1
                    else:
                        stats["dupes_removed"] += 1
                else:
                    stats["prev_skipped_existing"] += 1

            # --- NextQuestID ---
            if desired_next != 0:
                if current_next == 0:
                    stmt = (
                        f"UPDATE `quest_template_addon` SET `NextQuestID` = {desired_next} "
                        f"WHERE `ID` = {quest_id} AND `NextQuestID` = 0;"
                    )
                    if stmt not in seen_updates:
                        seen_updates.add(stmt)
                        block_lines.append(
                            f"{stmt} "
                            f"-- chain pos {i+1}/{len(quest_ids_in_db)}"
                        )
                        stats["next_updates"] += 1
                    else:
                        stats["dupes_removed"] += 1
                else:
                    stats["next_skipped_existing"] += 1

        if block_lines:
            stats["chains_with_updates"] += 1
            quest_id_list_str = ", ".join(str(q) for q in quest_ids_in_db[:8])
            if len(quest_ids_in_db) > 8:
                quest_id_list_str += f", ... ({len(quest_ids_in_db)} total)"
            block_header = [
                f"",
                f"-- ============================================================",
                f"-- QuestLine {line_id}: {line_name}",
                f"-- Quests ({len(quest_ids_in_db)}): {quest_id_list_str}",
                f"-- ============================================================",
            ]
            sql_blocks.append("\n".join(block_header + block_lines))

    return sql_blocks, stats, conflict_warnings, removed_edges

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("Quest Chain SQL Generator")
    print(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 60)

    addon_state      = load_addon_state()
    line_names       = load_quest_lines()
    quest_lines_data = load_quest_line_x_quest()

    sql_blocks, stats, conflict_warnings, removed_edges = generate_sql(
        quest_lines_data, line_names, addon_state
    )

    total_updates = stats["prev_updates"] + stats["next_updates"]

    # -----------------------------------------------------------------------
    # Write SQL file
    # -----------------------------------------------------------------------
    os.makedirs(os.path.dirname(OUTPUT_SQL), exist_ok=True)

    header_lines = [
        "--",
        "-- quest_chains.sql",
        f"-- Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"-- Source: QuestLineXQuest-enUS.csv + QuestLine-enUS.csv (build 66192)",
        "--",
        f"-- Chains processed:        {stats['chains_total']:>8,}",
        f"-- Chains with updates:     {stats['chains_with_updates']:>8,}",
        f"-- Chains skipped (no DB):  {stats['chains_skipped_no_db']:>8,}",
        f"-- Quests in DB:            {stats['quests_in_db']:>8,}",
        f"-- Quests missing from DB:  {stats['quests_missing_db']:>8,}",
        f"-- PrevQuestID UPDATEs:     {stats['prev_updates']:>8,}",
        f"-- NextQuestID UPDATEs:     {stats['next_updates']:>8,}",
        f"-- PrevQuestID skipped:     {stats['prev_skipped_existing']:>8,}  (already non-zero)",
        f"-- NextQuestID skipped:     {stats['next_skipped_existing']:>8,}  (already non-zero)",
        f"-- Total UPDATE statements: {total_updates:>8,}",
        f"-- Duplicate UPDATEs removed: {stats['dupes_removed']:>6,}",
        f"-- Cross-chain conflicts:   {stats['cross_chain_conflicts']:>8,}",
        f"-- Cycles broken:           {stats['cycles_broken']:>8,}",
        "--",
    ]

    # Add conflict details as SQL comments if any
    if conflict_warnings:
        header_lines.append("-- === CROSS-CHAIN CONFLICT DETAILS ===")
        for w in conflict_warnings:
            for wline in w.splitlines():
                header_lines.append(f"-- {wline}")
        header_lines.append("--")

    # Add cycle details as SQL comments if any
    if removed_edges:
        header_lines.append("-- === CYCLE REMOVAL DETAILS ===")
        for from_q, to_q, reason in removed_edges:
            header_lines.append(f"-- Removed: {from_q} -> {to_q} ({reason})")
        header_lines.append("--")

    header_lines += [
        "",
        "USE `world`;",
        "",
        "SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;",
        "",
        "START TRANSACTION;",
        "",
    ]

    footer_lines = [
        "",
        "-- ============================================================",
        "-- Dangling reference cleanup (dynamic)",
        "-- ============================================================",
        "UPDATE `quest_template_addon` SET `NextQuestID` = 0",
        "WHERE `NextQuestID` != 0 AND ABS(`NextQuestID`) NOT IN (SELECT `ID` FROM `quest_template`);",
        "",
        "UPDATE `quest_template_addon` SET `PrevQuestID` = 0",
        "WHERE `PrevQuestID` != 0 AND ABS(`PrevQuestID`) NOT IN (SELECT `ID` FROM `quest_template`);",
        "",
        "COMMIT;",
        "",
        "SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;",
        "",
        f"-- End of quest_chains.sql ({total_updates:,} UPDATE statements)",
    ]

    with open(OUTPUT_SQL, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(header_lines))
        for block in sql_blocks:
            f.write(block)
            f.write("\n")
        f.write("\n".join(footer_lines))
        f.write("\n")

    # -----------------------------------------------------------------------
    # Report
    # -----------------------------------------------------------------------
    print()
    print("=" * 60)
    print("REPORT")
    print("=" * 60)
    print(f"  Quest lines in CSV:          {stats['chains_total']:>8,}")
    print(f"  Chains with updates:         {stats['chains_with_updates']:>8,}")
    print(f"  Chains skipped (no DB rows): {stats['chains_skipped_no_db']:>8,}")
    print(f"  Quests found in DB:          {stats['quests_in_db']:>8,}")
    print(f"  Quests missing from DB:      {stats['quests_missing_db']:>8,}")
    print(f"  PrevQuestID UPDATEs:         {stats['prev_updates']:>8,}")
    print(f"  NextQuestID UPDATEs:         {stats['next_updates']:>8,}")
    print(f"  PrevQuestID skipped:         {stats['prev_skipped_existing']:>8,}  (non-zero already)")
    print(f"  NextQuestID skipped:         {stats['next_skipped_existing']:>8,}  (non-zero already)")
    print(f"  Total UPDATE statements:     {total_updates:>8,}")
    print(f"  Duplicate UPDATEs removed:   {stats['dupes_removed']:>8,}")
    print(f"  Cross-chain conflicts:       {stats['cross_chain_conflicts']:>8,}")
    print(f"  Cycles broken:               {stats['cycles_broken']:>8,}")
    print()
    print(f"  Output: {OUTPUT_SQL}")

    # Rough file size
    size_bytes = os.path.getsize(OUTPUT_SQL)
    if size_bytes > 1_048_576:
        print(f"  File size: {size_bytes / 1_048_576:.1f} MB")
    else:
        print(f"  File size: {size_bytes / 1024:.1f} KB")

    print("=" * 60)

if __name__ == "__main__":
    main()
