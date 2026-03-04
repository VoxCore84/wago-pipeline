#!/usr/bin/env python3
"""
run_all_imports.py -- Master execution script for Raidbots SQL imports.

Runs all 8 SQL files in the correct order with error checking.
Requires Python 3.10+ and MySQL 8.0 CLI on the system.

Usage:
    python run_all_imports.py                  # Run all steps
    python run_all_imports.py --dry-run        # Print what would be run
    python run_all_imports.py --step 4         # Resume from step 4
    python run_all_imports.py --skip-verification  # Skip final cycle check
    python run_all_imports.py --regenerate     # Re-run generators before import
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

MYSQL_EXE = r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe"
MYSQL_USER = "root"
MYSQL_PASS = "admin"

# Ordered list of (step_number, filename, description)
STEPS: list[tuple[int, str, str]] = [
    (1, "quest_chains.sql",            "Import quest chains (world)"),
    (2, "fix_quest_chains.sql",        "Fix quest chains (world)"),
    (3, "quest_objectives_import.sql", "Import quest objectives (world)"),
    (4, "quest_poi_import.sql",        "Import quest POI (world)"),
    (5, "quest_poi_points_import.sql", "Import quest POI points (world)"),
    (6, "item_sparse_locale.sql",      "Import item_sparse locale (hotfixes)"),
    (7, "item_search_name_locale.sql", "Import item_search_name locale (hotfixes)"),
    (8, "fix_locale_and_orphans.sql",  "Fix locale & orphans (multi-DB)"),
]

# Ordered list of (script_filename, description) for --regenerate
GENERATORS: list[tuple[str, str]] = [
    ("import_item_names.py",        "Regenerate item locale SQL"),
    ("quest_chain_gen.py",          "Regenerate quest chain SQL"),
    ("gen_quest_poi_sql.py",        "Regenerate quest POI SQL"),
    ("quest_objectives_import.py",  "Regenerate quest objectives SQL"),
]

VERIFICATION_SQL = """\
SET SESSION cte_max_recursion_depth = 5000;
WITH RECURSIVE verify_chain AS (
    SELECT a.ID AS start_id, a.NextQuestID AS current_id, 1 AS depth
    FROM world.quest_template_addon a
    WHERE a.NextQuestID != 0 AND a.ID != 0
    UNION ALL
    SELECT vc.start_id, a.NextQuestID, vc.depth + 1
    FROM verify_chain vc
    JOIN world.quest_template_addon a ON a.ID = vc.current_id
    WHERE vc.depth < 100 AND a.NextQuestID != 0 AND a.NextQuestID != vc.start_id
)
SELECT start_id, current_id AS cycle_at, depth AS cycle_length
FROM verify_chain vc WHERE vc.current_id = vc.start_id;
"""


def build_mysql_cmd() -> list[str]:
    """Build the base mysql CLI invocation (no database specified)."""
    return [MYSQL_EXE, f"-u{MYSQL_USER}", f"-p{MYSQL_PASS}"]


def run_sql_file(sql_path: Path) -> subprocess.CompletedProcess[str]:
    """Execute a SQL file through the mysql CLI."""
    cmd = build_mysql_cmd()
    with open(sql_path, "r", encoding="utf-8") as f:
        return subprocess.run(
            cmd,
            stdin=f,
            capture_output=True,
            text=True,
        )


def run_inline_sql(sql: str) -> subprocess.CompletedProcess[str]:
    """Execute an inline SQL string through the mysql CLI."""
    cmd = build_mysql_cmd() + ["-e", sql]
    return subprocess.run(cmd, capture_output=True, text=True)


def format_elapsed(seconds: float) -> str:
    """Format elapsed seconds into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.1f}s"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run all Raidbots SQL imports in order."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be run without executing.",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=1,
        metavar="N",
        help="Start from step N (1-8). Default: 1.",
    )
    parser.add_argument(
        "--skip-verification",
        action="store_true",
        help="Skip the final quest-chain cycle verification query.",
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Re-run generator scripts before executing SQL imports.",
    )
    args = parser.parse_args()

    if args.step < 1 or args.step > len(STEPS):
        print(f"ERROR: --step must be between 1 and {len(STEPS)}, got {args.step}")
        return 1

    # Resolve sql_output/ relative to this script's location
    script_dir = Path(__file__).resolve().parent
    sql_dir = script_dir / "sql_output"

    if not sql_dir.is_dir():
        print(f"ERROR: SQL directory not found: {sql_dir}")
        return 1

    # Pre-flight: verify all needed SQL files exist
    missing = []
    for step_num, filename, _ in STEPS:
        if step_num < args.step:
            continue
        sql_path = sql_dir / filename
        if not sql_path.is_file():
            missing.append(str(sql_path))
    if missing:
        print("ERROR: Missing SQL files:")
        for m in missing:
            print(f"  {m}")
        return 1

    # Pre-flight: verify mysql CLI exists
    if not args.dry_run and not Path(MYSQL_EXE).is_file():
        print(f"ERROR: MySQL CLI not found at: {MYSQL_EXE}")
        return 1

    # Header
    total_steps = len([s for s in STEPS if s[0] >= args.step])
    print("=" * 70)
    print("  Raidbots SQL Import Pipeline")
    print(f"  Steps to run: {total_steps} (starting from step {args.step})")
    if args.dry_run:
        print("  MODE: DRY RUN (no SQL will be executed)")
    if args.regenerate:
        print(f"  Regenerate: YES ({len(GENERATORS)} generators)")
    if args.skip_verification:
        print("  Verification: SKIPPED")
    print("=" * 70)
    print()

    overall_start = time.monotonic()

    # Generator phase (only when --regenerate is passed)
    if args.regenerate:
        total_gens = len(GENERATORS)
        for gen_idx, (gen_script, gen_desc) in enumerate(GENERATORS, start=1):
            gen_path = script_dir / gen_script
            print(f"[Gen {gen_idx}/{total_gens}] {gen_script}")
            print(f"         {gen_desc}")

            if args.dry_run:
                print(f"         Would run: python {gen_script}")
                print(f"         SKIPPED (dry run)")
                print()
                continue

            gen_start = time.monotonic()
            result = subprocess.run(
                [sys.executable, str(gen_path)],
                capture_output=True,
                text=True,
                cwd=str(script_dir),
            )
            elapsed = time.monotonic() - gen_start

            if result.returncode != 0:
                print(f"         FAILED (exit code {result.returncode}, {format_elapsed(elapsed)})")
                stderr = result.stderr.strip()
                if stderr:
                    for line in stderr.splitlines():
                        print(f"         | {line}")
                print()
                print(f"ABORTED at generator {gen_idx}. Fix {gen_script} and re-run with --regenerate.")
                return 1

            print(f"         OK ({format_elapsed(elapsed)})")
            print()

        if not args.dry_run:
            print(f"  All {total_gens} generators completed. Starting SQL import...\n")

    completed = 0
    failed = False

    for step_num, filename, description in STEPS:
        if step_num < args.step:
            continue

        sql_path = sql_dir / filename
        print(f"[Step {step_num}/8] {filename}")
        print(f"         {description}")

        if args.dry_run:
            cmd_str = " ".join(build_mysql_cmd()) + f' < "{sql_path}"'
            print(f"         Would run: {cmd_str}")
            print(f"         SKIPPED (dry run)")
            print()
            completed += 1
            continue

        step_start = time.monotonic()
        result = run_sql_file(sql_path)
        elapsed = time.monotonic() - step_start

        if result.returncode != 0:
            print(f"         FAILED (exit code {result.returncode}, {format_elapsed(elapsed)})")
            stderr = result.stderr.strip()
            if stderr:
                # Indent each line of stderr for readability
                for line in stderr.splitlines():
                    print(f"         | {line}")
            print()
            print(f"ABORTED at step {step_num}. Use --step {step_num} to resume after fixing.")
            failed = True
            break

        print(f"         OK ({format_elapsed(elapsed)})")
        # Print any warnings from stderr (mysql sends password warning there)
        stderr = result.stderr.strip()
        if stderr:
            for line in stderr.splitlines():
                # Skip the standard password-on-command-line warning
                if "Using a password on the command line" in line:
                    continue
                print(f"         | {line}")
        print()
        completed += 1

    # Verification step
    verification_passed = True
    if not failed and not args.skip_verification and not args.dry_run:
        print(f"[Verify] Quest chain cycle detection")
        verify_start = time.monotonic()
        result = run_inline_sql(VERIFICATION_SQL)
        elapsed = time.monotonic() - verify_start

        if result.returncode != 0:
            print(f"         FAILED (exit code {result.returncode}, {format_elapsed(elapsed)})")
            stderr = result.stderr.strip()
            if stderr:
                for line in stderr.splitlines():
                    if "Using a password on the command line" in line:
                        continue
                    print(f"         | {line}")
            verification_passed = False
        else:
            stdout = result.stdout.strip()
            if stdout:
                # If there's output, cycles were detected
                print(f"         WARNING: Cycles detected! ({format_elapsed(elapsed)})")
                for line in stdout.splitlines():
                    print(f"         | {line}")
                verification_passed = False
            else:
                print(f"         PASSED - no cycles found ({format_elapsed(elapsed)})")
        print()

    # Final summary
    overall_elapsed = time.monotonic() - overall_start
    print("=" * 70)
    print("  SUMMARY")
    print(f"  Steps completed: {completed}/{total_steps}")
    print(f"  Total time:      {format_elapsed(overall_elapsed)}")

    if args.dry_run:
        print(f"  Result:          DRY RUN (nothing executed)")
    elif failed:
        print(f"  Result:          FAILED")
    elif not verification_passed:
        print(f"  Result:          COMPLETED WITH WARNINGS (cycle check failed)")
    else:
        print(f"  Result:          PASSED")
    print("=" * 70)

    if failed:
        return 1
    if not verification_passed and not args.skip_verification:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
