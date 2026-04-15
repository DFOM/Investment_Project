from __future__ import annotations

import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.setup_env import DATA_DIR, LEDGER_PATH

TEAM_CONFIG_PATH = DATA_DIR / "team_config.json"
_DEFAULT_MEMBERS = ["Hasan", "Syed", "Luq", "Richard"]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_name(name: str) -> str:
    normalized = name.strip()
    if not normalized:
        raise ValueError("Name cannot be empty.")
    return normalized


def _casefold(value: str) -> str:
    return value.strip().casefold()


def _read_ledger_trader_names() -> list[str]:
    if not LEDGER_PATH.exists() or LEDGER_PATH.stat().st_size == 0:
        return []

    names: list[str] = []
    with LEDGER_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trader = str(row.get("Trader_Name", "")).strip()
            if trader and trader.casefold() != "system":
                names.append(trader)

    unique = sorted(set(names), key=str.casefold)
    return unique


def _default_members_payload() -> list[dict[str, Any]]:
    seed_names = sorted(set(_DEFAULT_MEMBERS + _read_ledger_trader_names()), key=str.casefold)
    now = _utc_now()
    return [
        {
            "id": str(uuid.uuid4()),
            "name": name,
            "aliases": [name],
            "active": True,
            "created_at": now,
            "updated_at": now,
        }
        for name in seed_names
    ]


def _empty_config() -> dict[str, Any]:
    return {
        "version": 1,
        "updated_at": _utc_now(),
        "members": _default_members_payload(),
    }


def _load_config(path: Path = TEAM_CONFIG_PATH) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        config = _empty_config()
        _save_config(config, path)
        return config

    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, dict):
        config = _empty_config()
        _save_config(config, path)
        return config

    members = raw.get("members")
    if not isinstance(members, list):
        raw["members"] = _default_members_payload()

    if "version" not in raw:
        raw["version"] = 1
    if "updated_at" not in raw:
        raw["updated_at"] = _utc_now()

    _save_config(raw, path)
    return raw


def _save_config(config: dict[str, Any], path: Path = TEAM_CONFIG_PATH) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    config["updated_at"] = _utc_now()
    with path.open("w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)


def _find_member_index(config: dict[str, Any], identifier: str) -> int:
    key = _casefold(identifier)
    members = config.get("members", [])

    for idx, member in enumerate(members):
        if str(member.get("id", "")).casefold() == key:
            return idx

    for idx, member in enumerate(members):
        if _casefold(str(member.get("name", ""))) == key:
            return idx

    raise ValueError(f"Member '{identifier}' not found.")


def ensure_team_config(path: Path = TEAM_CONFIG_PATH) -> dict[str, Any]:
    return _load_config(path)


def list_members(include_inactive: bool = True) -> list[dict[str, Any]]:
    config = _load_config()
    members = config.get("members", [])
    if include_inactive:
        return sorted(members, key=lambda m: str(m.get("name", "")).casefold())
    return sorted(
        [m for m in members if bool(m.get("active", True))],
        key=lambda m: str(m.get("name", "")).casefold(),
    )


def get_active_member_names() -> list[str]:
    return [str(member.get("name", "")) for member in list_members(include_inactive=False)]


def get_member_aliases(identifier: str) -> list[str]:
    config = _load_config()
    idx = _find_member_index(config, identifier)
    member = config["members"][idx]

    aliases = member.get("aliases", [])
    if not isinstance(aliases, list):
        aliases = []

    normalized_aliases = [str(a).strip() for a in aliases if str(a).strip()]
    if str(member.get("name", "")).strip() and str(member.get("name", "")).strip() not in normalized_aliases:
        normalized_aliases.append(str(member.get("name", "")).strip())

    return sorted(set(normalized_aliases), key=str.casefold)


def add_member(name: str) -> dict[str, Any]:
    normalized_name = _normalize_name(name)
    config = _load_config()

    now = _utc_now()
    for member in config.get("members", []):
        if _casefold(str(member.get("name", ""))) == _casefold(normalized_name):
            member["active"] = True
            aliases = member.get("aliases", [])
            if normalized_name not in aliases:
                aliases.append(normalized_name)
            member["aliases"] = sorted(set(str(a) for a in aliases), key=str.casefold)
            member["updated_at"] = now
            _save_config(config)
            return member

    member = {
        "id": str(uuid.uuid4()),
        "name": normalized_name,
        "aliases": [normalized_name],
        "active": True,
        "created_at": now,
        "updated_at": now,
    }
    config.setdefault("members", []).append(member)
    _save_config(config)
    return member


def rename_member(identifier: str, new_name: str, cascade_ledger: bool = False) -> dict[str, Any]:
    normalized_new_name = _normalize_name(new_name)
    config = _load_config()

    idx = _find_member_index(config, identifier)
    member = config["members"][idx]

    for i, existing in enumerate(config.get("members", [])):
        if i == idx:
            continue
        if _casefold(str(existing.get("name", ""))) == _casefold(normalized_new_name):
            raise ValueError(f"Member '{normalized_new_name}' already exists.")

    old_name = str(member.get("name", "")).strip()
    aliases = member.get("aliases", [])
    if not isinstance(aliases, list):
        aliases = []

    if old_name:
        aliases.append(old_name)
    aliases.append(normalized_new_name)

    member["name"] = normalized_new_name
    member["aliases"] = sorted(set(str(a).strip() for a in aliases if str(a).strip()), key=str.casefold)
    member["updated_at"] = _utc_now()

    if cascade_ledger:
        # Ledger is append-only; alias tracking preserves historical matching without mutating past rows.
        member["ledger_cascade_mode"] = "alias_mapping"

    _save_config(config)
    return member


def remove_member(identifier: str, hard_delete: bool = False) -> dict[str, Any]:
    config = _load_config()
    idx = _find_member_index(config, identifier)

    if hard_delete:
        removed = config.get("members", []).pop(idx)
        _save_config(config)
        return removed

    member = config["members"][idx]
    member["active"] = False
    member["updated_at"] = _utc_now()
    _save_config(config)
    return member
