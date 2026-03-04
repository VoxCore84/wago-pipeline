-- ============================================================================
-- fix_orphan_quest_refs.sql
-- Removes orphan quest starter/ender rows that reference non-existent
-- creatures, gameobjects, or quests.
--
-- Verified orphan counts (2026-03-03):
--   creature_queststarter  -> missing creature_template:  112 rows
--   creature_questender    -> missing creature_template:   14 rows
--   creature_queststarter  -> missing quest_template:     353 rows
--   creature_questender    -> missing quest_template:     361 rows
--   gameobject_queststarter -> missing gameobject_template: 55 rows
--   gameobject_questender   -> missing gameobject_template: 66 rows
--                                                  Total: 961 rows
--
-- Uses NOT EXISTS pattern (not NOT IN) to avoid NULL-value trap.
-- ============================================================================

USE world;

START TRANSACTION;

-- ---------------------------------------------------------------------------
-- 1. creature_queststarter referencing non-existent creature_template (112)
-- ---------------------------------------------------------------------------
DELETE cqs FROM creature_queststarter cqs
WHERE NOT EXISTS (
    SELECT 1 FROM creature_template ct WHERE ct.entry = cqs.id
);

-- ---------------------------------------------------------------------------
-- 2. creature_questender referencing non-existent creature_template (14)
-- ---------------------------------------------------------------------------
DELETE cqe FROM creature_questender cqe
WHERE NOT EXISTS (
    SELECT 1 FROM creature_template ct WHERE ct.entry = cqe.id
);

-- ---------------------------------------------------------------------------
-- 3. creature_queststarter referencing non-existent quest_template (353)
-- ---------------------------------------------------------------------------
DELETE cqs FROM creature_queststarter cqs
WHERE NOT EXISTS (
    SELECT 1 FROM quest_template qt WHERE qt.ID = cqs.quest
);

-- ---------------------------------------------------------------------------
-- 4. creature_questender referencing non-existent quest_template (361)
-- ---------------------------------------------------------------------------
DELETE cqe FROM creature_questender cqe
WHERE NOT EXISTS (
    SELECT 1 FROM quest_template qt WHERE qt.ID = cqe.quest
);

-- ---------------------------------------------------------------------------
-- 5. gameobject_queststarter referencing non-existent gameobject_template (55)
-- ---------------------------------------------------------------------------
DELETE gqs FROM gameobject_queststarter gqs
WHERE NOT EXISTS (
    SELECT 1 FROM gameobject_template gt WHERE gt.entry = gqs.id
);

-- ---------------------------------------------------------------------------
-- 6. gameobject_questender referencing non-existent gameobject_template (66)
-- ---------------------------------------------------------------------------
DELETE gqe FROM gameobject_questender gqe
WHERE NOT EXISTS (
    SELECT 1 FROM gameobject_template gt WHERE gt.entry = gqe.id
);

COMMIT;

-- ---------------------------------------------------------------------------
-- Verification: all counts should be 0
-- ---------------------------------------------------------------------------
SELECT 'creature_queststarter -> missing creature' AS `check`,
       COUNT(*) AS orphan_count
FROM creature_queststarter cqs
WHERE NOT EXISTS (SELECT 1 FROM creature_template ct WHERE ct.entry = cqs.id)

UNION ALL

SELECT 'creature_questender -> missing creature',
       COUNT(*)
FROM creature_questender cqe
WHERE NOT EXISTS (SELECT 1 FROM creature_template ct WHERE ct.entry = cqe.id)

UNION ALL

SELECT 'creature_queststarter -> missing quest',
       COUNT(*)
FROM creature_queststarter cqs
WHERE NOT EXISTS (SELECT 1 FROM quest_template qt WHERE qt.ID = cqs.quest)

UNION ALL

SELECT 'creature_questender -> missing quest',
       COUNT(*)
FROM creature_questender cqe
WHERE NOT EXISTS (SELECT 1 FROM quest_template qt WHERE qt.ID = cqe.quest)

UNION ALL

SELECT 'gameobject_queststarter -> missing gameobject',
       COUNT(*)
FROM gameobject_queststarter gqs
WHERE NOT EXISTS (SELECT 1 FROM gameobject_template gt WHERE gt.entry = gqs.id)

UNION ALL

SELECT 'gameobject_questender -> missing gameobject',
       COUNT(*)
FROM gameobject_questender gqe
WHERE NOT EXISTS (SELECT 1 FROM gameobject_template gt WHERE gt.entry = gqe.id);
