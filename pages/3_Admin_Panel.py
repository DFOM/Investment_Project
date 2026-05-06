from __future__ import annotations

from typing import Any, cast

import pandas as pd
import streamlit as _st

st = cast(Any, _st)

from core.database import (
    get_connection_status,
    get_simulation_settings,
    initialize_database_schema,
    set_member_initial_allocation,
    start_new_simulation,
    update_simulation_settings,
)
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
                "Initial Allocation (JPY)": float(member.get("initial_allocation_jpy", 0.0)),
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

    st.divider()
    st.subheader("Class Simulation Settings")
    settings = get_simulation_settings()
    with st.form("simulation_settings_form"):
        total_capital = st.number_input(
            "Total Starting Capital (JPY)",
            min_value=1.0,
            value=float(settings["total_starting_capital_jpy"]),
            step=1_000_000.0,
            format="%.0f",
        )
        borrowing_limit_pct = st.number_input(
            "Borrowing Limit (% of portfolio value)",
            min_value=0.0,
            max_value=100.0,
            value=float(settings["borrowing_limit_pct"] * 100),
            step=1.0,
        )
        margin_call_pct = st.number_input(
            "Margin Call Warning Threshold (%)",
            min_value=0.0,
            max_value=100.0,
            value=float(settings["margin_call_pct"] * 100),
            step=1.0,
        )
        forced_liquidation_pct = st.number_input(
            "Forced Liquidation Warning Threshold (%)",
            min_value=0.0,
            max_value=100.0,
            value=float(settings["forced_liquidation_pct"] * 100),
            step=1.0,
        )
        local_borrow_rate_pct = st.number_input(
            "Local/Japan Borrow Annual Rate (%)",
            min_value=0.0,
            max_value=100.0,
            value=float(settings["local_borrow_rate_pct"] * 100),
            step=0.01,
        )
        global_borrow_rate_pct = st.number_input(
            "Global/US Borrow Annual Rate (%)",
            min_value=0.0,
            max_value=100.0,
            value=float(settings["global_borrow_rate_pct"] * 100),
            step=0.01,
        )
        preferential_borrow_rate_pct = st.number_input(
            "Preferential Borrow Annual Rate (%)",
            min_value=0.0,
            max_value=100.0,
            value=float(settings["preferential_borrow_rate_pct"] * 100),
            step=0.01,
        )
        save_settings = st.form_submit_button("Save Simulation Settings")

    if save_settings:
        result = update_simulation_settings(
            total_starting_capital_jpy=total_capital,
            borrowing_limit_pct=borrowing_limit_pct / 100,
            margin_call_pct=margin_call_pct / 100,
            forced_liquidation_pct=forced_liquidation_pct / 100,
            local_borrow_rate_pct=local_borrow_rate_pct / 100,
            global_borrow_rate_pct=global_borrow_rate_pct / 100,
            preferential_borrow_rate_pct=preferential_borrow_rate_pct / 100,
        )
        if result.get("success"):
            st.success("Simulation settings saved.")
            st.rerun()
        else:
            st.error(str(result.get("error", "Could not save settings.")))

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

    st.subheader("Starting Capital Allocation")
    active_members = [m for m in list_members(include_inactive=False)]
    if active_members:
        st.caption("Set each member's starting JPY allocation. Use 0 for members who should share any unallocated remainder equally when a new simulation is started.")
        with st.form("member_allocations_form"):
            allocation_inputs: dict[str, float] = {}
            for member in active_members:
                name = str(member.get("name", ""))
                allocation_inputs[name] = st.number_input(
                    f"{name} allocation (JPY)",
                    min_value=0.0,
                    value=float(member.get("initial_allocation_jpy", 0.0)),
                    step=1_000_000.0,
                    format="%.0f",
                    key=f"allocation_{name}",
                )
            save_allocations = st.form_submit_button("Save Member Allocations")
        if save_allocations:
            failures = []
            for name, allocation in allocation_inputs.items():
                result = set_member_initial_allocation(name, allocation)
                if not result.get("success"):
                    failures.append(f"{name}: {result.get('error')}")
            if failures:
                st.error("; ".join(failures))
            else:
                st.success("Member allocations saved.")
                st.rerun()
    else:
        st.info("Add active team members before configuring allocations.")

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
        value=int(get_simulation_settings()["total_starting_capital_jpy"]),
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
