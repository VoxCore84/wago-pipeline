#!/usr/bin/env python3
"""
Wowhead Scraper — extract NPC, item, spell, quest, vendor, talent, and effect data
from Wowhead's tooltip API and main pages.

Usage:
    python wowhead_scraper.py <command> [options]

Commands: npc, item, spell, quest, vendor, talent, effect

Examples:
    python wowhead_scraper.py item --ids 19019 --verbose
    python wowhead_scraper.py spell --range 1 100 --format csv
    python wowhead_scraper.py npc --ids-file npc_ids.txt --threads 4
"""

import argparse
import concurrent.futures as cf
import csv
import json
import re
import sys
import threading
import time
from html import unescape as html_unescape
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TOOLTIP_URL = "https://nether.wowhead.com/tooltip/{type}/{id}"
PAGE_URL = "https://www.wowhead.com/{type}={id}"
USER_AGENT = "wowhead-scraper/1.0 (private-server-tooling; personal use)"
DEFAULT_OUT = Path(__file__).parent / "wowhead_data"
ENTITY_TYPES = ("npc", "item", "spell", "quest", "vendor", "talent", "effect")

# Quality names for items
QUALITY_NAMES = {
    -1: "Unknown", 0: "Poor", 1: "Common", 2: "Uncommon",
    3: "Rare", 4: "Epic", 5: "Legendary", 6: "Artifact", 7: "Heirloom",
}

# NPC classification from tooltip text
NPC_CLASSIFICATIONS = {"Elite", "Rare", "Rare Elite", "Boss", "World Boss"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def die(msg: str, code: int = 1) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    raise SystemExit(code)


def strip_html(html: str) -> str:
    """Strip HTML tags and decode entities, collapse whitespace."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_tooltip_lines(html: str) -> list[str]:
    """Parse tooltip HTML into individual text lines."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    lines: list[str] = []
    for tr in soup.find_all("tr"):
        text = tr.get_text(separator="\n").strip()
        if text:
            for line in text.split("\n"):
                line = line.strip()
                if line:
                    lines.append(line)
    return lines


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------


class RateLimiter:
    """Token-bucket style rate limiter with adaptive backoff."""

    def __init__(self, base_delay: float = 0.5, max_delay: float = 30.0) -> None:
        self.base_delay = base_delay
        self.current_delay = base_delay
        self.max_delay = max_delay
        self._last_time = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_time
            if elapsed < self.current_delay:
                time.sleep(self.current_delay - elapsed)
            self._last_time = time.monotonic()

    def backoff(self) -> None:
        with self._lock:
            self.current_delay = min(self.current_delay * 2, self.max_delay)

    def ease(self) -> None:
        with self._lock:
            if self.current_delay > self.base_delay:
                self.current_delay = max(self.current_delay * 0.8, self.base_delay)


# ---------------------------------------------------------------------------
# Disk Cache
# ---------------------------------------------------------------------------


class DiskCache:
    """File-based JSON cache with checkpoint support for resume."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.raw_dir = base_dir / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def has(self, entity_id: int, suffix: str = "") -> bool:
        return self._path(entity_id, suffix).exists()

    def get(self, entity_id: int, suffix: str = "") -> dict | None:
        p = self._path(entity_id, suffix)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

    def put(self, entity_id: int, data: dict, suffix: str = "") -> None:
        p = self._path(entity_id, suffix)
        tmp = p.with_suffix(p.suffix + ".part")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(p)

    def save_checkpoint(self, checkpoint_id: int) -> None:
        """Save checkpoint — the minimum uncompleted ID (high-water mark).

        All IDs below this value are guaranteed complete.
        """
        cp = self.base_dir / "_checkpoint.txt"
        cp.write_text(str(checkpoint_id), encoding="utf-8")

    def load_checkpoint(self) -> int | None:
        cp = self.base_dir / "_checkpoint.txt"
        if cp.exists():
            try:
                return int(cp.read_text(encoding="utf-8").strip())
            except ValueError:
                return None
        return None

    def _path(self, entity_id: int, suffix: str = "") -> Path:
        name = f"{entity_id}{suffix}.json"
        return self.raw_dir / name


# ---------------------------------------------------------------------------
# Tooltip Parser
# ---------------------------------------------------------------------------


class TooltipParser:
    """Parse Wowhead tooltip JSON into structured dicts per entity type."""

    @staticmethod
    def parse_item(data: dict) -> dict:
        tooltip_html = data.get("tooltip", "")
        lines = extract_tooltip_lines(tooltip_html)
        result: dict[str, Any] = {
            "id": data.get("id"),
            "name": data.get("name", ""),
            "quality": data.get("quality", -1),
            "quality_name": QUALITY_NAMES.get(data.get("quality", -1), "Unknown"),
            "icon": data.get("icon", ""),
        }

        soup = BeautifulSoup(tooltip_html, "html.parser") if tooltip_html else None

        # Item level
        for line in lines:
            m = re.match(r"Item Level (\d+)", line)
            if m:
                result["item_level"] = int(m.group(1))
                break

        # Required level
        for line in lines:
            m = re.match(r"Requires Level (\d+)", line)
            if m:
                result["required_level"] = int(m.group(1))
                break

        # Slot and type (e.g., "Main Hand  Sword")
        slot_types = {
            "Head", "Neck", "Shoulder", "Back", "Chest", "Shirt", "Tabard",
            "Wrist", "Hands", "Waist", "Legs", "Feet", "Finger", "Trinket",
            "Main Hand", "Off Hand", "One-Hand", "Two-Hand", "Ranged", "Held In Off-hand",
        }
        for line in lines:
            for slot in slot_types:
                if line.startswith(slot):
                    result["slot"] = slot
                    rest = line[len(slot):].strip()
                    if rest:
                        result["weapon_type"] = rest
                    break

        # DPS line
        for line in lines:
            m = re.search(r"\(([\d.]+) damage per second\)", line)
            if m:
                result["dps"] = float(m.group(1))
                break

        # Damage range
        for line in lines:
            m = re.match(r"(\d+)\s*-\s*(\d+) Damage", line)
            if m:
                result["dmg_min"] = int(m.group(1))
                result["dmg_max"] = int(m.group(2))
                break

        # Speed
        for line in lines:
            m = re.search(r"Speed ([\d.]+)", line)
            if m:
                result["speed"] = float(m.group(1))
                break

        # Armor
        for line in lines:
            m = re.match(r"(\d+) Armor", line)
            if m:
                result["armor"] = int(m.group(1))
                break

        # Stats (e.g., "+5 Stamina", "+10 Intellect")
        stats: dict[str, int] = {}
        for line in lines:
            m = re.match(r"[+\-](\d+) (\w[\w ]*)", line)
            if m:
                stat_name = m.group(2).strip().lower()
                stat_val = int(m.group(1))
                if line.startswith("-"):
                    stat_val = -stat_val
                if stat_name in ("stamina", "intellect", "strength", "agility",
                                 "spirit", "armor", "critical strike", "haste",
                                 "mastery", "versatility"):
                    stats[stat_name] = stat_val
        if stats:
            result["stats"] = stats

        # Durability
        for line in lines:
            m = re.match(r"Durability (\d+) / (\d+)", line)
            if m:
                result["durability"] = int(m.group(2))
                break

        # Sell price — look for span with class "moneygold" etc.
        if soup:
            sell_span = soup.find(string=re.compile(r"Sell Price:"))
            if sell_span:
                parent = sell_span.parent if sell_span.parent else soup
                gold = parent.find("span", class_="moneygold")
                silver = parent.find("span", class_="moneysilver")
                copper = parent.find("span", class_="moneycopper")
                price_copper = 0
                if gold:
                    price_copper += int(gold.get_text()) * 10000
                if silver:
                    price_copper += int(silver.get_text()) * 100
                if copper:
                    price_copper += int(copper.get_text())
                result["sell_price"] = price_copper

        return result

    @staticmethod
    def parse_spell(data: dict) -> dict:
        tooltip_html = data.get("tooltip", "")
        buff_html = data.get("buff", "")
        lines = extract_tooltip_lines(tooltip_html)
        result: dict[str, Any] = {
            "id": data.get("id"),
            "name": data.get("name", ""),
            "icon": data.get("icon", ""),
        }

        # Cast time, range, cooldown from tooltip lines
        for line in lines:
            if "sec cast" in line.lower() or "cast" in line.lower():
                m = re.search(r"([\d.]+)\s*sec cast", line, re.IGNORECASE)
                if m:
                    result["cast_time"] = float(m.group(1))
                elif "instant" in line.lower():
                    result["cast_time"] = 0.0
            if "yd range" in line.lower():
                m = re.search(r"([\d,]+)\s*yd range", line, re.IGNORECASE)
                if m:
                    result["range"] = int(m.group(1).replace(",", ""))
            m = re.search(r"(\d+)\s*(min|sec|hr)\s*cooldown", line, re.IGNORECASE)
            if m:
                val = int(m.group(1))
                unit = m.group(2).lower()
                if unit == "min":
                    val *= 60
                elif unit == "hr":
                    val *= 3600
                result["cooldown"] = val

        # Resource cost (mana, rage, energy, etc.)
        for line in lines:
            m = re.match(r"([\d,]+)\s+(Mana|Rage|Energy|Focus|Runic Power|Insanity|Fury|Pain|Maelstrom|Astral Power|Holy Power|Soul Shard|Essence|Chi)", line, re.IGNORECASE)
            if m:
                result["resource_cost"] = int(m.group(1).replace(",", ""))
                result["resource_type"] = m.group(2)
                break

        # Description — the main body text (last line(s) that aren't meta)
        desc_lines = []
        meta_patterns = (
            r"^\d+ (Mana|Rage|Energy|Focus|Runic Power)",
            r"sec cast", r"yd range", r"cooldown", r"Instant",
            r"Requires", r"Reagents:", r"Tools:",
        )
        for line in lines:
            is_meta = any(re.search(p, line, re.IGNORECASE) for p in meta_patterns)
            if not is_meta and len(line) > 10:
                desc_lines.append(line)
        if desc_lines:
            result["description"] = " ".join(desc_lines)

        # Buff text
        if buff_html:
            result["buff_text"] = strip_html(buff_html)

        return result

    @staticmethod
    def parse_npc(data: dict) -> dict:
        tooltip_html = data.get("tooltip", "")
        lines = extract_tooltip_lines(tooltip_html)
        result: dict[str, Any] = {
            "id": data.get("id"),
            "name": data.get("name", ""),
        }

        # Level, type, classification from "Level ?? Undead (Elite)" line
        for line in lines:
            m = re.match(r"Level\s+(\d+|\?\?)\s*(.*)", line, re.IGNORECASE)
            if m:
                level_str = m.group(1)
                result["level"] = -1 if level_str == "??" else int(level_str)
                rest = m.group(2).strip()
                # Parse "Undead (Elite)" or "Beast" or "Humanoid (Rare Elite)"
                cm = re.match(r"([\w\s]+?)(?:\s*\((\w[\w ]*)\))?\s*$", rest)
                if cm:
                    result["type"] = cm.group(1).strip()
                    if cm.group(2):
                        result["classification"] = cm.group(2).strip()
                break

        # Tag — usually in a <div class="q"> or separate line like "<Stormwind Guard>"
        for line in lines:
            if line.startswith("<") and line.endswith(">"):
                result["tag"] = line[1:-1]
                break

        # Map data
        map_data = data.get("map")
        if map_data:
            result["zone_id"] = map_data.get("zone")
            coords = map_data.get("coords", {})
            if coords:
                # Take first coord set
                for floor_id, coord_list in coords.items():
                    if coord_list and len(coord_list) > 0:
                        result["coords"] = coord_list[0]
                        break

        return result

    @staticmethod
    def parse_quest(data: dict) -> dict:
        tooltip_html = data.get("tooltip", "")
        lines = extract_tooltip_lines(tooltip_html)
        result: dict[str, Any] = {
            "id": data.get("id"),
            "name": data.get("name", ""),
        }

        # Objectives — lines after the quest name, before "Requirements:"
        objectives: list[str] = []
        requirements: list[str] = []
        in_req = False
        for line in lines:
            if line == result["name"]:
                continue
            if "Requirements:" in line or "Requires" in line:
                in_req = True
                continue
            if in_req:
                if line.startswith("- "):
                    requirements.append(line[2:].strip())
                else:
                    requirements.append(line)
            else:
                objectives.append(line)

        if objectives:
            result["objectives"] = " ".join(objectives)
        if requirements:
            result["requirements"] = requirements

        return result

    @staticmethod
    def parse_talent(data: dict) -> dict:
        """Parse talent tooltip — same structure as spell."""
        result = TooltipParser.parse_spell(data)
        result["type"] = "talent"
        return result

    @staticmethod
    def parse_effect(data: dict) -> dict:
        """Parse effect/spell visual tooltip — same structure as spell."""
        result = TooltipParser.parse_spell(data)
        result["type"] = "effect"
        return result


# ---------------------------------------------------------------------------
# Page Parsers — extract JS data from main Wowhead pages
# ---------------------------------------------------------------------------


def _find_matching_bracket(
    html: str,
    start_pos: int,
    open_char: str = "{",
    close_char: str = "}",
    max_chars: int = 200_000,
) -> int | None:
    """Find the position after the matching close bracket starting from start_pos.

    Handles: nested brackets, single/double/backtick strings with escape sequences,
    single-line (//) and multi-line (/* */) comments.

    Returns end position (exclusive) or None if not found within max_chars.
    """
    length = len(html)
    limit = min(start_pos + max_chars, length)
    depth = 0
    in_string = False
    string_char: str | None = None
    escape = False
    in_line_comment = False
    in_block_comment = False
    i = start_pos

    while i < limit:
        c = html[i]

        # --- inside a line comment: skip until newline ---
        if in_line_comment:
            if c == "\n":
                in_line_comment = False
            i += 1
            continue

        # --- inside a block comment: skip until */ ---
        if in_block_comment:
            if c == "*" and i + 1 < limit and html[i + 1] == "/":
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        # --- inside a string literal ---
        if in_string:
            if escape:
                escape = False
                i += 1
                continue
            if c == "\\":
                escape = True
                i += 1
                continue
            if c == string_char:
                in_string = False
                string_char = None
            i += 1
            continue

        # --- normal code context ---
        # Check for comment starts
        if c == "/" and i + 1 < limit:
            next_c = html[i + 1]
            if next_c == "/":
                in_line_comment = True
                i += 2
                continue
            if next_c == "*":
                in_block_comment = True
                i += 2
                continue

        # Check for string starts
        if c in ('"', "'", "`"):
            in_string = True
            string_char = c
            i += 1
            continue

        # Track bracket depth
        if c == open_char:
            depth += 1
        elif c == close_char:
            depth -= 1
            if depth == 0:
                return i + 1

        i += 1

    # Exceeded max_chars without finding match
    print(
        f"[WARNING] _find_matching_bracket: exceeded {max_chars} chars from pos {start_pos} "
        f"(depth={depth}), returning None",
        file=sys.stderr,
    )
    return None


def extract_js_object(html: str, pattern: str) -> dict | None:
    """Extract a JS object from page HTML using bracket-depth parsing.
    The pattern should match everything up to the opening { of the object."""
    m = re.search(pattern, html)
    if not m:
        return None

    # Find the opening { after the pattern match
    rest = html[m.end():]
    brace_pos = rest.find("{")
    if brace_pos == -1:
        return None

    obj_start = m.end() + brace_pos
    end = _find_matching_bracket(html, obj_start, "{", "}")
    if end is None:
        return None

    js_text = html[obj_start:end]
    try:
        return json.loads(js_text)
    except json.JSONDecodeError:
        cleaned = re.sub(r",\s*([\]}])", r"\1", js_text)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return None


def extract_gatherer_data(html: str, data_type: int) -> list[dict]:
    """Extract WH.Gatherer.addData(type, subType, {...}) entries.

    Uses bracket-depth parsing to handle nested objects correctly.
    """
    results: list[dict] = []
    pattern = rf"WH\.Gatherer\.addData\(\s*{data_type}\s*,\s*\d+\s*,\s*"
    for m in re.finditer(pattern, html):
        start = html.find("{", m.end())
        if start == -1:
            continue
        end = _find_matching_bracket(html, start, "{", "}")
        if end is None:
            continue
        js_text = html[start:end]
        try:
            results.append(json.loads(js_text))
        except json.JSONDecodeError:
            cleaned = re.sub(r",\s*([\]}])", r"\1", js_text)
            try:
                results.append(json.loads(cleaned))
            except json.JSONDecodeError:
                continue
    return results


def extract_listview_data(html: str, tab_id: str) -> list[dict]:
    """Extract Listview data array for a specific tab using bracket-depth parsing."""
    # Find the Listview block with matching id, then locate "data:[" after it
    id_pattern = rf"id:\s*'{tab_id}'"
    m = re.search(id_pattern, html)
    if not m:
        m = re.search(rf'id:\s*"{tab_id}"', html)
    if not m:
        return []

    # Search for "data:[" within 2000 chars after the id match
    search_start = m.end()
    search_region = html[search_start:search_start + 2000]
    dm = re.search(r"data:\s*\[", search_region)
    if not dm:
        return []

    # Bracket-depth parse to find the matching ]
    array_start = search_start + dm.end() - 1  # index of opening [
    end = _find_matching_bracket(html, array_start, "[", "]")
    if end is None:
        return []

    js_text = html[array_start:end]
    try:
        return json.loads(js_text)
    except json.JSONDecodeError:
        cleaned = re.sub(r",\s*([\]}])", r"\1", js_text)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return []


def parse_npc_page(html: str, npc_id: int) -> dict:
    """Extract g_npcs[id] data from NPC page."""
    result: dict[str, Any] = {}

    # $.extend(g_npcs[ID], {...})
    pattern = rf"\$\.extend\(g_npcs\[{npc_id}\]\s*,\s*"
    data = extract_js_object(html, pattern)
    if data:
        result["react"] = data.get("react", [])
        result["classification"] = data.get("classification", 0)
        result["type"] = data.get("type")
        result["boss"] = data.get("boss", 0)
        result["hasQuests"] = data.get("hasQuests", False)

    return result


def parse_item_page(html: str, item_id: int) -> dict:
    """Extract jsonequip and g_items data from item page."""
    result: dict[str, Any] = {}

    # Look for jsonequip in g_items or $.extend
    pattern = rf"\$\.extend\(g_items\[{item_id}\]\s*,\s*"
    data = extract_js_object(html, pattern)
    if data:
        if "jsonequip" in data:
            equip = data["jsonequip"]
            result["source"] = equip.get("source")
            result["sourcemore"] = equip.get("sourcemore")
            result["appearances"] = equip.get("appearances")
            result["nsockets"] = equip.get("nsockets", 0)
            result["socket1"] = equip.get("socket1")
            result["socket2"] = equip.get("socket2")
            result["socket3"] = equip.get("socket3")

    return result


def parse_quest_page(html: str, quest_id: int) -> dict:
    """Extract comprehensive quest data from Wowhead quest page.

    Sources:
      - $.extend(g_quests[id], {...})  — core metadata + rewards
      - WH.Gatherer.addData(5,...)     — prerequisite quest IDs
      - Infobox markup                 — starter/ender NPC IDs, sharable, difficulty, patch
      - Mapper objectives              — starter NPC coords + zone
      - Quick-facts-storyline HTML     — storyline chain
      - Heading sections               — description / progress / completion text
    """
    result: dict[str, Any] = {}

    # --- 1. $.extend(g_quests[id], {...}) ---
    pattern = rf"\$\.extend\(g_quests\[{quest_id}\]\s*,\s*"
    data = extract_js_object(html, pattern)
    if data:
        result["level"] = data.get("level")
        result["reqlevel"] = data.get("reqlevel")
        result["category"] = data.get("category")
        result["category2"] = data.get("category2")
        result["side"] = data.get("side")
        result["money"] = data.get("money")
        result["xp"] = data.get("xp")
        result["quest_type"] = data.get("type")
        result["wflags"] = data.get("wflags")
        result["reqclass"] = data.get("reqclass")
        result["reqrace"] = data.get("reqrace")
        # Rewards
        if data.get("reprewards"):
            result["reprewards"] = data["reprewards"]        # [[factionId, amount], ...]
        if data.get("itemrewards"):
            result["itemrewards"] = data["itemrewards"]      # [[itemId, count], ...]
        if data.get("itemchoices"):
            result["itemchoices"] = data["itemchoices"]      # [[itemId, count], ...]
        if data.get("currencyrewards"):
            result["currencyrewards"] = data["currencyrewards"]  # [[currencyId, amount], ...]

    # --- 2. Prerequisite quest IDs from WH.Gatherer.addData(5, ...) ---
    prereq_ids = []
    for obj in extract_gatherer_data(html, 5):
        try:
            prereq_ids.extend(int(k) for k in obj.keys())
        except (ValueError, TypeError):
            pass
    if prereq_ids:
        result["prereq_quests"] = sorted(set(prereq_ids))

    # --- 3. Infobox markup — starter/ender NPC, sharable, difficulty, patch ---
    infobox_match = re.search(r'var defined_infobox\s*=\s*["\'](.+?)["\']', html, re.DOTALL)
    if not infobox_match:
        infobox_match = re.search(r"Markup\.printHtml\(['\"](.+?)['\"]", html, re.DOTALL)
    if infobox_match:
        markup = infobox_match.group(1)
        # Starter NPC
        m = re.search(r'Start:\s*.*?/npc=(\d+)', markup)
        if m:
            result["starter_npc"] = int(m.group(1))
        # Ender NPC
        m = re.search(r'End:\s*.*?/npc=(\d+)', markup)
        if m:
            result["ender_npc"] = int(m.group(1))
        # Sharable
        if "Not sharable" in markup:
            result["sharable"] = False
        elif "Sharable" in markup:
            result["sharable"] = True
        # Patch added
        m = re.search(r'Added in patch\s+([\d.]+)', markup)
        if m:
            result["patch_added"] = m.group(1)
        # Difficulty range
        m = re.search(r'Difficulty:\s*([\d\s.]+)', markup)
        if m:
            result["difficulty_range"] = m.group(1).strip()

    # --- 4. Mapper objectives — starter NPC coords + zone ---
    # Use bracket-depth extraction instead of lazy regex (which truncates at first })
    mapper_data = extract_js_object(html, r'new Mapper\(')
    if mapper_data:
        try:
            objectives = mapper_data.get("objectives", {})
            for zone_id_str, zone_data in objectives.items():
                zone_name = zone_data.get("zone", "")
                levels = zone_data.get("levels", [])
                for level_list in levels:
                    for point in level_list:
                        if isinstance(point, dict) and point.get("point") == "start":
                            result["starter_zone_id"] = int(zone_id_str)
                            result["starter_zone_name"] = zone_name
                            coord = point.get("coord", [])
                            if len(coord) >= 2:
                                result["starter_x"] = coord[0]
                                result["starter_y"] = coord[1]
                            if not result.get("starter_npc") and point.get("id"):
                                result["starter_npc"] = point["id"]
                            break
        except (ValueError, TypeError):
            pass

    # --- 5. Storyline chain from quick-facts-storyline HTML ---
    storyline_match = re.search(
        r'<div class="quick-facts-storyline">\s*<a href="[^"]*?/storyline/[^"]*?-(\d+)"[^>]*>([^<]+)</a>',
        html
    )
    if storyline_match:
        result["storyline_id"] = int(storyline_match.group(1))
        result["storyline_name"] = html_unescape(storyline_match.group(2).strip())
        # Parse position in chain
        chain_quests = []
        chain_pos = 0
        for i, qm in enumerate(re.finditer(
            r'<li(?:\s+class="current")?\s*>.*?(?:href="/quest=(\d+)|<span>)',
            html[storyline_match.start():storyline_match.start() + 50000]
        ), 1):
            if qm.group(1):
                chain_quests.append(int(qm.group(1)))
            else:
                chain_quests.append(quest_id)
                chain_pos = i
        if chain_quests:
            result["storyline_quests"] = chain_quests
            result["storyline_pos"] = chain_pos

    # --- 6. Quest text sections (description / progress / completion) ---
    soup = None
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        pass

    if soup:
        for heading in soup.find_all("h2", class_="heading-size-3"):
            heading_text = heading.get_text(strip=True).lower()
            if heading_text == "description":
                desc_el = heading.find_next_sibling()
                if desc_el and desc_el.name != "h2":
                    text = desc_el.get_text(separator=" ", strip=True)
                    if text:
                        result["description_text"] = text
            elif heading_text == "progress":
                prog_el = heading.find_next_sibling()
                if prog_el and prog_el.name != "h2":
                    text = prog_el.get_text(separator=" ", strip=True)
                    if text:
                        result["progress_text"] = text
            elif heading_text == "completion":
                comp_el = heading.find_next_sibling()
                if comp_el and comp_el.name != "h2":
                    text = comp_el.get_text(separator=" ", strip=True)
                    if text:
                        result["completion_text"] = text

    return result


def parse_vendor_page(html: str) -> list[dict]:
    """Extract sells tab Listview data from NPC page."""
    items = extract_listview_data(html, "sells")
    results: list[dict] = []
    for item in items:
        entry: dict[str, Any] = {
            "item_id": item.get("id"),
            "name": item.get("name", ""),
            "quality": item.get("quality", 0),
            "quality_name": QUALITY_NAMES.get(item.get("quality", 0), "Unknown"),
        }
        # Price: cost field is [[copper, [currency_list], [extra]]]
        cost = item.get("cost")
        if cost and isinstance(cost, list) and len(cost) > 0:
            cost_inner = cost[0]
            if isinstance(cost_inner, list) and len(cost_inner) > 0:
                entry["buy_price"] = cost_inner[0] if isinstance(cost_inner[0], int) else 0
                # Currency costs (e.g., honor, badges)
                if len(cost_inner) > 1 and cost_inner[1]:
                    entry["currency_cost"] = cost_inner[1]
            elif isinstance(cost_inner, int):
                entry["buy_price"] = cost_inner

        # Extra useful fields
        if item.get("classs") is not None:
            entry["item_class"] = item["classs"]
        if item.get("subclass") is not None:
            entry["item_subclass"] = item["subclass"]
        if item.get("level") is not None:
            entry["item_level"] = item["level"]
        if item.get("slot") is not None:
            entry["slot"] = item["slot"]
        if item.get("standing") is not None:
            entry["rep_standing"] = item["standing"]
        if item.get("avail") is not None:
            entry["availability"] = item["avail"]
        results.append(entry)
    return results


# ---------------------------------------------------------------------------
# Base Scraper
# ---------------------------------------------------------------------------


class BaseScraper:
    """Shared fetch/retry/cache/threading logic for all entity scrapers."""

    entity_type: str = ""  # Override in subclass
    tooltip_type: str = ""  # Type for tooltip API (item, spell, npc, quest)
    page_type: str = ""    # Type for page URL (item, npc, quest, spell)

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.out_dir = Path(args.out) / self.entity_type if args.out else DEFAULT_OUT / self.entity_type
        self.cache = DiskCache(self.out_dir)
        self.limiter = RateLimiter(base_delay=args.delay, max_delay=30.0)
        self.verbose = args.verbose
        self.force = args.force
        self.tooltip_only = args.tooltip_only
        self.max_retries = args.max_retries
        self.timeout = args.timeout

        # Counters
        self.ok = 0
        self.skip = 0
        self.miss = 0
        self.fail = 0

        import requests as _req
        self.session = _req.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def resolve_ids(self) -> list[int]:
        """Build the list of IDs to scrape from args."""
        args = self.args
        ids: list[int] = []

        if args.ids:
            for part in args.ids.split(","):
                part = part.strip()
                if part.isdigit():
                    ids.append(int(part))

        if args.ids_file:
            p = Path(args.ids_file)
            if not p.exists():
                die(f"IDs file not found: {p}")
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    parts = line.split()
                    if parts[0].isdigit():
                        ids.append(int(parts[0]))

        if args.range_start is not None and args.range_end is not None:
            ids.extend(range(args.range_start, args.range_end + 1))

        if not ids:
            die("No IDs specified. Use --ids, --ids-file, or --range.")

        # Resume support
        if args.resume:
            cp = self.cache.load_checkpoint()
            if cp is not None:
                before = len(ids)
                ids = [i for i in ids if i > cp]
                if self.verbose:
                    print(f"[INFO] Resuming from checkpoint {cp}, skipped {before - len(ids)} IDs")

        return ids

    def fetch_tooltip(self, entity_id: int) -> dict | None:
        """Fetch tooltip JSON from Wowhead API."""
        url = TOOLTIP_URL.format(type=self.tooltip_type, id=entity_id)
        return self._fetch_json(url, entity_id, "tooltip")

    def fetch_page(self, entity_id: int) -> str | None:
        """Fetch full HTML page from Wowhead."""
        url = PAGE_URL.format(type=self.page_type, id=entity_id)
        return self._fetch_text(url, entity_id)

    def _fetch_json(self, url: str, entity_id: int, label: str) -> dict | None:
        for attempt in range(1, self.max_retries + 1):
            self.limiter.wait()
            try:
                r = self.session.get(url, timeout=self.timeout)
                if r.status_code == 404:
                    return None
                if r.status_code == 429:
                    self.limiter.backoff()
                    if self.verbose:
                        print(f"  [WARN] 429 on {label} {entity_id}, backing off to {self.limiter.current_delay:.1f}s")
                    continue
                if r.status_code >= 500:
                    self.limiter.backoff()
                    if self.verbose:
                        print(f"  [WARN] {r.status_code} on {label} {entity_id}, retry {attempt}/{self.max_retries}")
                    continue
                r.raise_for_status()
                self.limiter.ease()
                data = r.json()
                data["id"] = entity_id
                return data
            except Exception as e:
                if attempt == self.max_retries:
                    if self.verbose:
                        print(f"  [WARN] Failed {label} {entity_id}: {e}")
                    return None
                time.sleep(1)
        return None

    def _fetch_text(self, url: str, entity_id: int) -> str | None:
        for attempt in range(1, self.max_retries + 1):
            self.limiter.wait()
            try:
                r = self.session.get(url, timeout=self.timeout)
                if r.status_code == 404:
                    return None
                if r.status_code == 429:
                    self.limiter.backoff()
                    continue
                if r.status_code >= 500:
                    self.limiter.backoff()
                    continue
                r.raise_for_status()
                self.limiter.ease()
                return r.text
            except Exception:
                if attempt == self.max_retries:
                    return None
                time.sleep(1)
        return None

    def scrape_one(self, entity_id: int) -> str:
        """Scrape a single entity. Returns status string."""
        # Check cache
        if not self.force and self.cache.has(entity_id):
            return f"SKIP  {self.entity_type}/{entity_id}"

        # Fetch tooltip
        tooltip_data = self.fetch_tooltip(entity_id)
        if tooltip_data is None:
            return f"MISS  {self.entity_type}/{entity_id} (404)"

        # Cache raw tooltip
        self.cache.put(entity_id, tooltip_data)

        # Fetch page data if not tooltip-only
        page_data: dict = {}
        if not self.tooltip_only and self.page_type:
            html = self.fetch_page(entity_id)
            if html:
                page_data = self.parse_page(html, entity_id)
                if page_data:
                    self.cache.put(entity_id, page_data, suffix="_page")

        # Parse into structured result
        parsed = self.parse_tooltip(tooltip_data)
        if page_data:
            parsed.update(page_data)

        # Save combined result
        self.cache.put(entity_id, parsed, suffix="_parsed")

        return f"OK    {self.entity_type}/{entity_id} — {parsed.get('name', '?')}"

    def parse_tooltip(self, data: dict) -> dict:
        """Override in subclass to parse tooltip data."""
        return data

    def parse_page(self, html: str, entity_id: int) -> dict:
        """Override in subclass to parse page HTML."""
        return {}

    def run(self) -> None:
        """Main entry: resolve IDs, scrape with threading, export."""
        ids = self.resolve_ids()
        total = len(ids)
        threads = max(1, self.args.threads)

        print(f"[INFO] Scraping {total:,} {self.entity_type}(s) with {threads} thread(s)")
        print(f"[INFO] Output: {self.out_dir}")
        if self.args.resume:
            print(f"[INFO] Resume mode enabled")

        start = time.monotonic()

        # Track completed IDs for high-water-mark checkpoint
        id_set = set(ids)
        completed_ids: set[int] = set()

        if threads == 1:
            for i, eid in enumerate(ids, 1):
                msg = self.scrape_one(eid)
                self._count(msg)
                completed_ids.add(eid)
                self.cache.save_checkpoint(eid)
                if self.verbose or msg.startswith("OK"):
                    print(f"  [{i}/{total}] {msg}")
        else:
            with cf.ThreadPoolExecutor(max_workers=threads) as ex:
                futures = {ex.submit(self.scrape_one, eid): eid for eid in ids}
                done = 0
                for fut in cf.as_completed(futures):
                    done += 1
                    eid = futures[fut]
                    try:
                        msg = fut.result()
                        self._count(msg)
                    except Exception as e:
                        self.fail += 1
                        msg = f"FAIL  {self.entity_type}/{eid}: {e}"
                    completed_ids.add(eid)
                    # High-water mark: find the minimum ID not yet completed
                    uncompleted = id_set - completed_ids
                    if uncompleted:
                        hwm = min(uncompleted)
                    else:
                        hwm = max(ids)
                    self.cache.save_checkpoint(hwm)
                    if self.verbose or msg.startswith("OK"):
                        print(f"  [{done}/{total}] {msg}")

        elapsed = time.monotonic() - start
        print(f"[DONE] OK={self.ok} SKIP={self.skip} MISS={self.miss} FAIL={self.fail}  ({elapsed:.1f}s)")

        # Save manifest
        manifest = {
            "entity_type": self.entity_type,
            "total_requested": total,
            "ok": self.ok,
            "skip": self.skip,
            "miss": self.miss,
            "fail": self.fail,
            "elapsed_seconds": round(elapsed, 1),
            "tooltip_only": self.tooltip_only,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        (self.out_dir / "_manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )

        # Export
        fmt = self.args.format
        if fmt in ("json", "all"):
            self.export_json(ids)
        if fmt in ("csv", "all"):
            self.export_csv(ids)
        if fmt in ("sql", "all"):
            self.export_sql(ids)

    def _count(self, msg: str) -> None:
        if msg.startswith("OK"):
            self.ok += 1
        elif msg.startswith("SKIP"):
            self.skip += 1
        elif msg.startswith("MISS"):
            self.miss += 1
        else:
            self.fail += 1

    # -------------------------------------------------------------------
    # Export methods
    # -------------------------------------------------------------------

    def _load_all_parsed(self, ids: list[int]) -> list[dict]:
        results: list[dict] = []
        for eid in ids:
            data = self.cache.get(eid, suffix="_parsed")
            if data:
                results.append(data)
        return results

    def export_json(self, ids: list[int]) -> None:
        results = self._load_all_parsed(ids)
        if not results:
            return
        out = self.out_dir / f"{self.entity_type}_export.json"
        tmp = out.with_suffix(out.suffix + ".part")
        tmp.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(out)
        print(f"[INFO] Exported {len(results):,} entries to {out}")

    def export_csv(self, ids: list[int]) -> None:
        results = self._load_all_parsed(ids)
        if not results:
            return
        # Collect all keys across all results for CSV header
        all_keys: list[str] = []
        seen: set[str] = set()
        for r in results:
            for k in r:
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)

        out = self.out_dir / f"{self.entity_type}_export.csv"
        tmp = out.with_suffix(out.suffix + ".part")
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            for r in results:
                # Flatten nested dicts/lists for CSV
                flat = {}
                for k, v in r.items():
                    if isinstance(v, (dict, list)):
                        flat[k] = json.dumps(v, ensure_ascii=False)
                    else:
                        flat[k] = v
                writer.writerow(flat)
        tmp.replace(out)
        print(f"[INFO] Exported {len(results):,} entries to {out}")

    def export_sql(self, ids: list[int]) -> None:
        results = self._load_all_parsed(ids)
        if not results:
            return
        out = self.out_dir / f"{self.entity_type}_export.sql"
        tmp = out.with_suffix(out.suffix + ".part")

        # Collect all keys
        all_keys: list[str] = []
        seen: set[str] = set()
        for r in results:
            for k in r:
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)

        with open(tmp, "w", encoding="utf-8") as f:
            f.write(f"-- Wowhead {self.entity_type} data export\n")
            f.write(f"-- Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- Entries: {len(results)}\n\n")

            table_name = f"wowhead_{self.entity_type}"
            cols = ", ".join(f"`{k}`" for k in all_keys)

            for r in results:
                vals: list[str] = []
                for k in all_keys:
                    v = r.get(k)
                    if v is None:
                        vals.append("NULL")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    elif isinstance(v, (dict, list)):
                        s = json.dumps(v, ensure_ascii=False).replace("'", "''")
                        vals.append(f"'{s}'")
                    else:
                        s = str(v).replace("'", "''")
                        vals.append(f"'{s}'")
                val_str = ", ".join(vals)
                f.write(f"INSERT IGNORE INTO `{table_name}` ({cols}) VALUES ({val_str});\n")

        tmp.replace(out)
        print(f"[INFO] Exported {len(results):,} entries to {out}")


# ---------------------------------------------------------------------------
# Entity Scrapers
# ---------------------------------------------------------------------------


class NpcScraper(BaseScraper):
    entity_type = "npc"
    tooltip_type = "npc"
    page_type = "npc"

    def parse_tooltip(self, data: dict) -> dict:
        return TooltipParser.parse_npc(data)

    def parse_page(self, html: str, entity_id: int) -> dict:
        return parse_npc_page(html, entity_id)


class ItemScraper(BaseScraper):
    entity_type = "item"
    tooltip_type = "item"
    page_type = "item"

    def parse_tooltip(self, data: dict) -> dict:
        return TooltipParser.parse_item(data)

    def parse_page(self, html: str, entity_id: int) -> dict:
        return parse_item_page(html, entity_id)


class SpellScraper(BaseScraper):
    entity_type = "spell"
    tooltip_type = "spell"
    page_type = "spell"

    def parse_tooltip(self, data: dict) -> dict:
        return TooltipParser.parse_spell(data)


class QuestScraper(BaseScraper):
    entity_type = "quest"
    tooltip_type = "quest"
    page_type = "quest"

    def parse_tooltip(self, data: dict) -> dict:
        return TooltipParser.parse_quest(data)

    def parse_page(self, html: str, entity_id: int) -> dict:
        return parse_quest_page(html, entity_id)


class VendorScraper(BaseScraper):
    entity_type = "vendor"
    tooltip_type = "npc"
    page_type = "npc"

    def parse_tooltip(self, data: dict) -> dict:
        result = TooltipParser.parse_npc(data)
        result["type"] = "vendor"
        return result

    def parse_page(self, html: str, entity_id: int) -> dict:
        items = parse_vendor_page(html)
        result = parse_npc_page(html, entity_id)
        if items:
            result["sells"] = items
            result["sell_count"] = len(items)
        return result

    def scrape_one(self, entity_id: int) -> str:
        """Vendors always need the page (for sells tab).

        Instead of mutating self.tooltip_only (unsafe across threads),
        inline the fetch logic with tooltip_only forced to False.
        """
        # Check cache
        if not self.force and self.cache.has(entity_id):
            return f"SKIP  {self.entity_type}/{entity_id}"

        # Fetch tooltip
        tooltip_data = self.fetch_tooltip(entity_id)
        if tooltip_data is None:
            return f"MISS  {self.entity_type}/{entity_id} (404)"

        # Cache raw tooltip
        self.cache.put(entity_id, tooltip_data)

        # Always fetch the page for vendors (regardless of tooltip_only setting)
        page_data: dict = {}
        if self.page_type:
            html = self.fetch_page(entity_id)
            if html:
                page_data = self.parse_page(html, entity_id)
                if page_data:
                    self.cache.put(entity_id, page_data, suffix="_page")

        # Parse into structured result
        parsed = self.parse_tooltip(tooltip_data)
        if page_data:
            parsed.update(page_data)

        # Save combined result
        self.cache.put(entity_id, parsed, suffix="_parsed")

        return f"OK    {self.entity_type}/{entity_id} — {parsed.get('name', '?')}"


class TalentScraper(BaseScraper):
    entity_type = "talent"
    tooltip_type = "spell"
    page_type = "spell"

    def parse_tooltip(self, data: dict) -> dict:
        return TooltipParser.parse_talent(data)


class EffectScraper(BaseScraper):
    entity_type = "effect"
    tooltip_type = "spell"
    page_type = "spell"

    def parse_tooltip(self, data: dict) -> dict:
        return TooltipParser.parse_effect(data)


# ---------------------------------------------------------------------------
# Scraper Registry
# ---------------------------------------------------------------------------

SCRAPERS: dict[str, type[BaseScraper]] = {
    "npc": NpcScraper,
    "item": ItemScraper,
    "spell": SpellScraper,
    "quest": QuestScraper,
    "vendor": VendorScraper,
    "talent": TalentScraper,
    "effect": EffectScraper,
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Wowhead Scraper — extract NPC, item, spell, quest, vendor, talent, and effect data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python wowhead_scraper.py item --ids 19019 --verbose
  python wowhead_scraper.py spell --range 1 100 --format csv
  python wowhead_scraper.py npc --ids-file npc_ids.txt --threads 4 --format all
  python wowhead_scraper.py vendor --ids 54811 --verbose""",
    )

    parser.add_argument("command", choices=ENTITY_TYPES,
                        help="Entity type to scrape")

    # ID selection
    id_group = parser.add_argument_group("ID selection (pick one)")
    id_group.add_argument("--ids", type=str, default="",
                          help="Comma-separated IDs (e.g., 332,4730,44247)")
    id_group.add_argument("--ids-file", type=str, default="",
                          help="File with one ID per line (# comments OK)")
    id_group.add_argument("--range", type=int, nargs=2, metavar=("START", "END"),
                          dest="id_range", default=None,
                          help="Start/end range for bulk crawling")

    # Output
    out_group = parser.add_argument_group("Output")
    out_group.add_argument("--out", type=str, default="",
                           help=f"Output directory (default: {DEFAULT_OUT}/<type>/)")
    out_group.add_argument("--format", choices=("json", "csv", "sql", "all"),
                           default="json", help="Export format (default: json)")

    # Scraping options
    scrape_group = parser.add_argument_group("Scraping")
    scrape_group.add_argument("--threads", type=int, default=4,
                              help="Concurrent workers (default: 4)")
    scrape_group.add_argument("--delay", type=float, default=0.5,
                              help="Seconds between requests per thread (default: 0.5)")
    scrape_group.add_argument("--timeout", type=int, default=30,
                              help="HTTP timeout in seconds (default: 30)")
    scrape_group.add_argument("--max-retries", type=int, default=3,
                              help="Retries per failed request (default: 3)")
    scrape_group.add_argument("--force", action="store_true",
                              help="Re-scrape even if cached")
    scrape_group.add_argument("--resume", action="store_true",
                              help="Resume from last checkpoint")
    scrape_group.add_argument("--tooltip-only", action="store_true",
                              help="Tooltip API only (faster, less data)")
    scrape_group.add_argument("--verbose", action="store_true",
                              help="Detailed progress output")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Normalize range args
    if args.id_range:
        args.range_start = args.id_range[0]
        args.range_end = args.id_range[1]
    else:
        args.range_start = None
        args.range_end = None

    # Validate at least one ID source
    if not args.ids and not args.ids_file and args.range_start is None:
        die("No IDs specified. Use --ids, --ids-file, or --range.")

    scraper_cls = SCRAPERS[args.command]
    scraper = scraper_cls(args)
    scraper.run()


if __name__ == "__main__":
    main()
