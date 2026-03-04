-- ============================================================================
-- fix_locale_and_orphans.sql
-- Generated: 2026-03-03
--
-- Fixes:
--   1a. Remove duplicate rows from hotfixes.item_sparse_locale where
--       VerifiedBuild = 61609 and a newer build (66192) exists for the same
--       (ID, locale) pair.
--   1b. Remove rows from hotfixes.item_sparse_locale with NULL Display_lang
--       (ID 110230 and 110231, locale deDE, VerifiedBuild 0)
--   1c. Remove duplicate rows from hotfixes.item_search_name_locale where
--       VerifiedBuild = 61609 and a newer build (66192) exists for the same
--       (ID, locale) pair.
--   2.  Remove orphaned rows from world.quest_objectives
--       (QuestID references quest_template rows that don't exist)
--
-- Expected rows deleted (based on initial analysis):
--   ~348 + ~2 + ~174 + ~890 = ~1,414
-- Actual counts may vary on re-runs if the data has changed.
-- ============================================================================

-- ============================================================================
-- HOTFIXES CLEANUP
-- ============================================================================
USE hotfixes;
START TRANSACTION;

-- ============================================================================
-- TASK 1a: item_sparse_locale -- delete VerifiedBuild 61609 duplicates
-- ============================================================================
-- PK is (ID, locale, VerifiedBuild). For many (ID, locale) pairs, two rows
-- exist with VerifiedBuild 66192 and 61609. We keep 66192 (newer) and delete
-- only the specific old build 61609 where a higher-build row exists.

DELETE a
FROM hotfixes.item_sparse_locale a
INNER JOIN hotfixes.item_sparse_locale b
    ON  b.ID     = a.ID
    AND b.locale = a.locale
    AND b.VerifiedBuild > a.VerifiedBuild
WHERE a.VerifiedBuild = 61609;
-- Expected: ~348 rows deleted (based on initial analysis)

-- ============================================================================
-- TASK 1b: item_sparse_locale -- delete rows with NULL/empty Display_lang
-- ============================================================================
-- ID 110230 (deDE, VerifiedBuild 0) and ID 110231 (deDE, VerifiedBuild 0)
-- have NULL Display_lang -- these are junk rows with no useful locale data.

DELETE FROM hotfixes.item_sparse_locale
WHERE Display_lang IS NULL
  AND VerifiedBuild = 0;
-- Expected: ~2 rows deleted (based on initial analysis)

COMMIT;

-- ============================================================================
-- TASK 1c: item_search_name_locale -- delete VerifiedBuild 61609 duplicates
-- ============================================================================
-- Same pattern as item_sparse_locale: many (ID, locale) pairs have rows with
-- VerifiedBuild 66192 and 61609. Keep 66192, delete only 61609 where a
-- higher-build row exists.
START TRANSACTION;

DELETE a
FROM hotfixes.item_search_name_locale a
INNER JOIN hotfixes.item_search_name_locale b
    ON  b.ID     = a.ID
    AND b.locale = a.locale
    AND b.VerifiedBuild > a.VerifiedBuild
WHERE a.VerifiedBuild = 61609;
-- Expected: ~174 rows deleted (based on initial analysis)

COMMIT;

-- ============================================================================
-- WORLD CLEANUP
-- ============================================================================
START TRANSACTION;

-- ============================================================================
-- TASK 2: quest_objectives -- delete orphaned rows
-- ============================================================================
-- Rows in world.quest_objectives that reference QuestIDs with no
-- corresponding row in world.quest_template. These are leftover from
-- removed or never-imported quests.
-- Uses NOT EXISTS instead of NOT IN to avoid the NULL trap (if quest_template.ID
-- ever contains a NULL, NOT IN would silently return zero rows).

DELETE qo
FROM world.quest_objectives qo
WHERE NOT EXISTS (
    SELECT 1 FROM world.quest_template qt WHERE qt.ID = qo.QuestID
);
-- Expected: ~890 rows deleted (based on initial analysis)

COMMIT;
