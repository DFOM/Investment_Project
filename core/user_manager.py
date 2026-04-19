from __future__ import annotations

import string
import secrets
from datetime import datetime, timezone
from typing import Any

from core.database import get_database, get_cached_team_auth_df, clear_data_cache


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_name(name: str) -> str:
    normalized = name.strip()
    if not normalized:
        raise ValueError("Name cannot be empty.")
    return normalized


def generate_auth_code() -> str:
    """Generate a random 6-character alphanumeric code."""
    alphabet = string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(6))


def list_members(include_inactive: bool = True) -> list[dict[str, Any]]:
    df = get_cached_team_auth_df()
    if df.empty:
        return []
    
    members = []
    for _, row in df.iterrows():
        active = str(row.get("Active", "True")).lower() == "true"
        if not active and not include_inactive:
            continue
        members.append({
            "name": str(row.get("Trader_Name", "")),
            "auth_code": str(row.get("Auth_Code", "")),
            "active": active,
            "created_at": str(row.get("Created_At", ""))
        })
    return sorted(members, key=lambda m: m["name"].casefold())


def get_active_member_names() -> list[str]:
    return [m["name"] for m in list_members(include_inactive=False)]


def get_member_aliases(identifier: str) -> list[str]:
    # Simplify aliases to just the name for sheet-based auth
    return [identifier]


def add_member(name: str) -> dict[str, Any]:
    normalized_name = _normalize_name(name)
    df = get_cached_team_auth_df()
    
    for _, row in df.iterrows():
        if str(row.get("Trader_Name", "")).strip().casefold() == normalized_name.casefold():
            code = str(row.get("Auth_Code", "")).strip()
            if not code:
                code = generate_auth_code()
            get_database().upsert_team_auth(normalized_name, code, True)
            clear_data_cache()
            return {"name": normalized_name, "auth_code": code, "active": True}
            
    new_code = generate_auth_code()
    get_database().upsert_team_auth(normalized_name, new_code, True)
    clear_data_cache()
    return {"name": normalized_name, "auth_code": new_code, "active": True}


def rename_member(identifier: str, new_name: str, cascade_ledger: bool = False) -> dict[str, Any]:
    normalized_new_name = _normalize_name(new_name)
    success = get_database().rename_team_auth(identifier, normalized_new_name)
    if not success:
        raise ValueError(f"Member '{identifier}' not found.")
    clear_data_cache()
    df = get_cached_team_auth_df()
    for _, row in df.iterrows():
        if str(row.get("Trader_Name", "")).strip().casefold() == normalized_new_name.casefold():
            return {
                "name": normalized_new_name, 
                "auth_code": str(row.get("Auth_Code", "")), 
                "active": str(row.get("Active", "True")).lower() == "true"
            }
    return {"name": normalized_new_name}


def remove_member(identifier: str, hard_delete: bool = False) -> dict[str, Any]:
    df = get_cached_team_auth_df()
    for _, row in df.iterrows():
        if str(row.get("Trader_Name", "")).strip().casefold() == identifier.strip().casefold():
            name = str(row.get("Trader_Name", ""))
            code = str(row.get("Auth_Code", ""))
            get_database().upsert_team_auth(name, code, False)
            clear_data_cache()
            return {"name": name, "auth_code": code, "active": False}
    raise ValueError(f"Member '{identifier}' not found.")


def authenticate_user(name: str, code: str) -> bool:
    """Check if the provided code matches the active user's code in the database."""
    if not name or not code:
        return False
    df = get_cached_team_auth_df()
    for _, row in df.iterrows():
        if str(row.get("Trader_Name", "")).strip().casefold() == name.strip().casefold():
            expected = str(row.get("Auth_Code", "")).strip()
            active = str(row.get("Active", "True")).lower() == "true"
            if active and expected and expected == code.strip():
                return True
    return False


def ensure_team_config() -> dict[str, Any]:
    """Ensure the Team Auth sheet exists by fetching it."""
    get_cached_team_auth_df()
    return {}
