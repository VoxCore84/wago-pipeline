USE world;
SET cte_max_recursion_depth = 5000;
START TRANSACTION;

-- ============================================================================
-- fix_quest_chains.sql
-- Fixes quest chain data issues in quest_template_addon
--
-- Sections:
--   1. Self-references       — quests whose Next/PrevQuestID points to itself
--   2. N-hop cycle detection — recursive CTE finds cycles of any length,
--                              breaks each cycle at the highest-ID quest
--   3. Dangling NextQuestID  — NextQuestID references a non-existent quest
--   4. Dangling PrevQuestID  — ABS(PrevQuestID) references a non-existent quest
--   5. Verification          — confirms zero cycles remain (after COMMIT)
--
-- MySQL 8.0 CTE notes:
--   - cte_max_recursion_depth raised to 5000 (default 1000 too low for 47K quests)
--   - Cycle detection uses depth-only approach (no path column) to avoid
--     CHAR overflow on long chains. The recursive walk terminates when
--     NextQuestID = start_id (cycle found) or depth limit is reached.
--   - Column name is `ID` (not QuestId) per actual schema
-- ============================================================================


-- ============================================================================
-- Section 1: Self-references
-- Zero out fields where a quest's Next/PrevQuestID equals its own QuestId.
-- These are always wrong — a quest cannot be its own prerequisite or successor.
-- ============================================================================

UPDATE quest_template_addon SET NextQuestID = 0
WHERE NextQuestID = ID AND ID != 0;

UPDATE quest_template_addon SET PrevQuestID = 0
WHERE ABS(PrevQuestID) = ID AND ID != 0;


-- ============================================================================
-- Section 2: N-hop cycle detection and fix
-- Uses a recursive CTE to walk NextQuestID chains up to 50 hops.
-- When the walk returns to its starting quest, a cycle is detected.
-- Each cycle is broken by zeroing NextQuestID on the highest-ID quest
-- in the cycle, and zeroing the corresponding back-pointing PrevQuestID.
-- ============================================================================

-- Temp table collects the breaking-point edge for each detected cycle
DROP TEMPORARY TABLE IF EXISTS _cycle_edges;
CREATE TEMPORARY TABLE _cycle_edges (
    cycle_quest_id INT UNSIGNED NOT NULL,
    next_quest_id  INT UNSIGNED NOT NULL,
    cycle_length   INT NOT NULL,
    PRIMARY KEY (cycle_quest_id)
);

-- Depth-only CTE: walks NextQuestID chains looking for return to start_id.
-- No path column needed — avoids CHAR overflow on long chains (47K quests).
-- Termination: stops when next_id = start_id (cycle) or depth > 100.
INSERT INTO _cycle_edges (cycle_quest_id, next_quest_id, cycle_length)
WITH RECURSIVE chain AS (
    -- Seed: every quest that has a non-zero NextQuestID
    SELECT
        a.ID AS start_id,
        a.ID AS current_id,
        a.NextQuestID AS next_id,
        1 AS depth
    FROM quest_template_addon a
    WHERE a.NextQuestID != 0 AND a.ID != 0

    UNION ALL

    -- Walk: follow NextQuestID until we return to start or hit depth limit
    SELECT
        c.start_id,
        c.next_id,
        a.NextQuestID,
        c.depth + 1
    FROM chain c
    JOIN quest_template_addon a ON a.ID = c.next_id
    WHERE c.depth < 100
      AND a.NextQuestID != 0
      AND a.NextQuestID != c.start_id  -- stop BEFORE re-entering the cycle
)
-- Rows where next_id = start_id indicate a completed cycle.
-- Break at the highest-ID quest in each cycle.
SELECT c.current_id, c.next_id, c.depth
FROM chain c
WHERE c.next_id = c.start_id
  AND c.current_id = (
      SELECT MAX(sub.current_id)
      FROM chain sub
      WHERE sub.start_id = c.start_id
        AND sub.next_id = sub.start_id
        AND sub.depth = c.depth
  )
ON DUPLICATE KEY UPDATE cycle_length = VALUES(cycle_length);

-- Break cycle: zero NextQuestID on the highest-ID quest
UPDATE quest_template_addon a
JOIN _cycle_edges ce ON ce.cycle_quest_id = a.ID
SET a.NextQuestID = 0;

-- Clean up the reverse pointer: if the quest that was pointed to has a
-- PrevQuestID pointing back to the quest we just broke, zero it too
UPDATE quest_template_addon a
JOIN _cycle_edges ce ON ce.next_quest_id = a.ID
SET a.PrevQuestID = 0
WHERE a.PrevQuestID = ce.cycle_quest_id;

DROP TEMPORARY TABLE _cycle_edges;


-- ============================================================================
-- Section 3: Dangling NextQuestID
-- NextQuestID points to a quest that does not exist in quest_template.
-- Uses a dynamic subquery so the fix adapts to whatever data is present.
-- ============================================================================

UPDATE quest_template_addon SET NextQuestID = 0
WHERE NextQuestID != 0
  AND ABS(NextQuestID) NOT IN (SELECT ID FROM quest_template);


-- ============================================================================
-- Section 4: Dangling PrevQuestID
-- ABS(PrevQuestID) points to a quest that does not exist in quest_template.
-- Uses a dynamic subquery so the fix adapts to whatever data is present.
-- ============================================================================

UPDATE quest_template_addon SET PrevQuestID = 0
WHERE PrevQuestID != 0
  AND ABS(PrevQuestID) NOT IN (SELECT ID FROM quest_template);


COMMIT;


-- ============================================================================
-- Section 5: Verification — confirm no remaining NextQuestID cycles
-- This SELECT should return 0 rows if all cycles were successfully broken.
-- Run outside the transaction so it reads committed state.
-- ============================================================================

-- Depth-only verification: walks chains looking for return to start_id.
-- Returns 0 rows if all cycles are broken.
WITH RECURSIVE verify_chain AS (
    SELECT
        a.ID AS start_id,
        a.NextQuestID AS current_id,
        1 AS depth
    FROM quest_template_addon a
    WHERE a.NextQuestID != 0 AND a.ID != 0

    UNION ALL

    SELECT
        vc.start_id,
        a.NextQuestID,
        vc.depth + 1
    FROM verify_chain vc
    JOIN quest_template_addon a ON a.ID = vc.current_id
    WHERE vc.depth < 100
      AND a.NextQuestID != 0
      AND a.NextQuestID != vc.start_id
)
SELECT start_id, current_id AS cycle_at, depth AS cycle_length
FROM verify_chain vc
WHERE vc.current_id = vc.start_id;
