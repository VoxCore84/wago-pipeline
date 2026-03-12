# wago-pipeline

![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue) ![License: MIT](https://img.shields.io/github/license/VoxCore84/wago-pipeline) ![GitHub release](https://img.shields.io/github/v/release/VoxCore84/wago-pipeline)

Data enrichment pipeline for [RoleplayCore](https://github.com/VoxCore84/RoleplayCore) (TrinityCore Midnight 12.x). Imports localized item names, quest chains, quest POI, quest objectives, and Wowhead-scraped data into the world/hotfixes databases.

## Data Sources

- **Raidbots** — `item-names.json` (171K items, 10 locales). Download from `https://www.raidbots.com/static/data/live/`
- **Wago DB2** — QuestLineXQuest, QuestObjective, QuestPOIBlob, QuestPOIPoint CSVs. Download from `https://wago.tools/db2/`
- **Wowhead** — NPC data via `wowhead_scraper.py`

## Scripts

| Script | Purpose |
|--------|---------|
| `import_item_names.py` | Raidbots item-names.json to locale SQL (item_sparse_locale, item_search_name_locale) |
| `quest_chain_gen.py` | Wago QuestLineXQuest CSV to PrevQuestID/NextQuestID chain UPDATEs |
| `gen_quest_poi_sql.py` | Wago QuestPOIBlob/Point CSVs to quest_poi/quest_poi_points INSERTs |
| `quest_objectives_import.py` | Wago QuestObjective CSV to quest_objectives INSERTs |
| `wowhead_scraper.py` | Multi-threaded Wowhead scraper (NPCs, vendors, quests) |
| `run_all_imports.py` | Master runner — orchestrates all 8 SQL steps |

## Usage

```bash
# Generate SQL from source data, then import
python run_all_imports.py --regenerate

# Just import (SQL files already exist)
python run_all_imports.py

# Preview without executing
python run_all_imports.py --dry-run

# Resume from a specific step
python run_all_imports.py --step 4
```

## Execution Order

1. `quest_chains.sql` — Import quest chain data
2. `fix_quest_chains.sql` — Fix self-refs, cycles, dangling refs (depth-only recursive CTE)
3. `quest_objectives_import.sql` — Import quest objectives
4. `quest_poi_import.sql` — Import quest POI
5. `quest_poi_points_import.sql` — Import quest POI points
6. `item_sparse_locale.sql` — Import item locale data (hotfixes)
7. `item_search_name_locale.sql` — Import item search name locale data (hotfixes)
8. `fix_locale_and_orphans.sql` — Cleanup duplicate locale rows and orphan quest_objectives

One-off (not in pipeline): `fix_orphan_quest_refs.sql` — orphan quest starter/ender cleanup.

## Requirements

- Python 3.10+
- MySQL 8.0 CLI (`mysql.exe`)
- TrinityCore world + hotfixes databases populated
