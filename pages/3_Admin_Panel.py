from __future__ import annotations

from typing import Any, cast

import pandas as pd
import streamlit as _st

st = cast(Any, _st)

from core.database import get_connection_status, initialize_database_schema, start_new_simulation
from core.setup_env import setup_environment
from core.user_manager import add_member, ensure_team_config, list_members, remove_member, rename_member


def _members_table() -> pd.DataFrame:
    members = list_members(include_inactive=True)
    if not members:
        return pd.DataFrame(columns=["Name", "Active", "Created At"])

    rows = []
    for member in members:
        rows.append(
            {
                "Name": str(member.get("name", "")),
                "Active": bool(member.get("active", True)),
                "Created At": str(member.get("created_at", "")),
            }
        )

    return pd.DataFrame(rows).sort_values(["Active", "Name"], ascending=[False, True])


def _member_selector_options() -> tuple[list[str], dict[str, str]]:
    members = list_members(include_inactive=True)
    labels: list[str] = []
    mapping: dict[str, str] = {}

    for member in members:
        name = str(member.get("name", ""))
        active = bool(member.get("active", True))
        status = "active" if active else "inactive"
        label = f"{name} ({status})"
        labels.append(label)
        mapping[label] = name

    return labels, mapping


def main() -> None:
    st.set_page_config(page_title="Admin Panel", layout="wide")
    setup_environment()
    ensure_team_config()

    st.title("Admin Panel")
    st.caption("Manage portfolio manager roster without editing source code.")

    st.subheader("🗄️ PostgreSQL Backend")
    connection = get_connection_status()

    if connection.get("connected"):
        st.success(str(connection.get("message", "PostgreSQL connected.")))
        c1, c2, c3 = st.columns(3)
        c1.metric("Database", str(connection.get("database_name", "Unknown")))
        c2.metric("Host", str(connection.get("host", "Unknown")))
        c3.metric("Server", str(connection.get("version", "Unknown")))
    else:
        st.error(str(connection.get("message") or connection.get("error") or "PostgreSQL disconnected."))

    if st.button("Initialize PostgreSQL Schema", type="primary"):
        try:
            result = initialize_database_schema()
            if result.get("success"):
                st.success(str(result.get("message", "PostgreSQL schema initialized.")))
                st.caption(f"Database: {result.get('database', 'Unknown')}")
            else:
                st.error(str(result.get("error", "Schema initialization failed.")))
        except Exception as exc:
            st.error(f"Schema initialization failed: {exc}")

    with st.form("add_member_form", clear_on_submit=True):
        new_name = st.text_input("Add Team Member", placeholder="Enter full name")
        add_clicked = st.form_submit_button("Add Member")

    if add_clicked:
        try:
            member = add_member(new_name)
            st.success(f"Added/activated member: {member['name']}")
            st.rerun()
        except Exception as exc:
            st.error(f"Failed to add member: {exc}")

    st.subheader("Current Team Roster")
    st.dataframe(_members_table(), use_container_width=True)

    labels, mapping = _member_selector_options()
    if not labels:
        st.info("No members configured yet.")
    else:
        selected_label = st.selectbox("Select Member", labels, index=0)
        selected_id = mapping[selected_label]

        with st.form("edit_member_form"):
            renamed_to = st.text_input("Rename Selected Member", placeholder="New display name")
            rename_clicked = st.form_submit_button("Save Rename")

        if rename_clicked:
            try:
                renamed = rename_member(selected_id, renamed_to, cascade_ledger=False)
                st.success(f"Renamed member to: {renamed['name']}")
                st.rerun()
            except Exception as exc:
                st.error(f"Rename failed: {exc}")

        c1, c2 = st.columns(2)
        if c1.button("Deactivate Member", type="secondary"):
            try:
                removed = remove_member(selected_id, hard_delete=False)
                st.success(f"Deactivated: {removed.get('name', 'member')}")
                st.rerun()
            except Exception as exc:
                st.error(f"Deactivate failed: {exc}")

        if c2.button("Delete Member Permanently", type="secondary"):
            try:
                removed = remove_member(selected_id, hard_delete=True)
                st.success(f"Deleted: {removed.get('name', 'member')}")
                st.rerun()
            except Exception as exc:
                st.error(f"Delete failed: {exc}")

    st.divider()
    st.subheader("Danger Zone: Reset Simulation")
    st.warning(
        "This appends a fresh initial-funding ledger row for the PostgreSQL-backed "
        "simulation. Export or back up the database first if you need a clean archive."
    )

    new_starting_capital = st.number_input(
        "New Starting Capital (JPY)",
        min_value=1,
        value=100_000_000,
        step=1_000_000,
        format="%d",
    )
    confirm_reset = st.checkbox("Confirm Reset")

    if st.button("Start New Simulation", type="primary"):
        if not confirm_reset:
            st.warning("Please check 'Confirm Reset' before starting a new simulation.")
        else:
            try:
                result = start_new_simulation(float(new_starting_capital))
                st.session_state.clear()
                if result.get("success"):
                    st.success(
                        "New simulation funding row recorded successfully. "
                        f"Starting capital: ¥{result.get('starting_capital', 0):,.0f}."
                    )
                else:
                    st.error(str(result.get("error", "Failed to start simulation.")))
                st.info("Proceed to Trading Desk to begin trading in the new empty simulation.")
            except Exception as exc:
                st.error(f"Failed to start new simulation: {exc}")


main()
