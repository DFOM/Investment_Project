from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

import pandas as pd
import streamlit as _st

st = cast(Any, _st)

from core.database import get_database, get_google_sheets_connection_status, initialize_database_schema, start_new_simulation
from core.setup_env import setup_environment
from core.user_manager import add_member, ensure_team_config, list_members, remove_member, rename_member

_SECRETS_PATH = Path(__file__).resolve().parents[1] / ".streamlit" / "secrets.toml"


def _persist_sheet_id(sheet_id: str) -> None:
    """Write the Sheet ID into .streamlit/secrets.toml so it survives restarts."""
    text = _SECRETS_PATH.read_text(encoding="utf-8")
    # Replace an existing GOOGLE_SHEET_ID line (with or without a value)
    new_line = f'GOOGLE_SHEET_ID = "{sheet_id}"'
    if re.search(r'^GOOGLE_SHEET_ID\s*=', text, re.MULTILINE):
        text = re.sub(r'^GOOGLE_SHEET_ID\s*=.*$', new_line, text, flags=re.MULTILINE)
    else:
        text = text.rstrip() + f"\n{new_line}\n"
    _SECRETS_PATH.write_text(text, encoding="utf-8")


def _members_table() -> pd.DataFrame:
    members = list_members(include_inactive=True)
    if not members:
        return pd.DataFrame(columns=["Name", "Active", "Aliases", "Member ID"])

    rows = []
    for member in members:
        aliases = member.get("aliases", [])
        rows.append(
            {
                "Name": str(member.get("name", "")),
                "Active": bool(member.get("active", True)),
                "Aliases": ", ".join(str(a) for a in aliases),
                "Member ID": str(member.get("id", ""))[:8],
            }
        )

    return pd.DataFrame(rows).sort_values(["Active", "Name"], ascending=[False, True])


def _member_selector_options() -> tuple[list[str], dict[str, str]]:
    members = list_members(include_inactive=True)
    labels: list[str] = []
    mapping: dict[str, str] = {}

    for member in members:
        mid = str(member.get("id", ""))
        name = str(member.get("name", ""))
        active = bool(member.get("active", True))
        status = "active" if active else "inactive"
        label = f"{name} ({status})"
        labels.append(label)
        mapping[label] = mid

    return labels, mapping


def main() -> None:
    st.set_page_config(page_title="Admin Panel", layout="wide")
    setup_environment()
    ensure_team_config()

    st.title("Admin Panel")
    st.caption("Manage portfolio manager roster without editing source code.")

    st.subheader("☁️ Google Sheets Integration")
    connection = get_google_sheets_connection_status()

    # ── Connect a new or replacement Google Sheet ──────────────────────────
    with st.expander("🔗 Connect a Google Sheet", expanded=not connection.get("connected")):
        st.caption(
            "Paste a Spreadsheet ID (found in the sheet URL after `/d/`) and click **Connect**. "
            "The system will create the Ledger and Performance worksheets automatically — "
            "no manual setup required."
        )
        with st.form("connect_sheet_form", clear_on_submit=False):
            sheet_id_input = st.text_input(
                "Google Sheet ID",
                placeholder="e.g. 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms",
                help="Open the spreadsheet in your browser. Copy the long ID string between /d/ and /edit in the URL.",
            )
            connect_clicked = st.form_submit_button("Connect & Initialize Schema", type="primary")

        if connect_clicked:
            if not sheet_id_input.strip():
                st.warning("Please enter a Sheet ID before connecting.")
            else:
                try:
                    with st.spinner("Authenticating and building schema on Google Sheets…"):
                        result = initialize_database_schema(sheet_id_input.strip())
                    _persist_sheet_id(sheet_id_input.strip())
                    st.success(f"✅ Connected to **{result['spreadsheet_title']}**")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Ledger Tab", "Created ✓" if result["ledger_created"] else "Already Exists")
                    c2.metric("Genesis Block", "Written ✓" if result["genesis_written"] else "Already Present")
                    c3.metric("Performance Tab", "Created ✓" if result["performance_created"] else "Already Exists")
                    c4.metric("Sheet1 Cleaned", "Deleted ✓" if result["sheet1_deleted"] else "Not Found")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Connection failed: {exc}")

    # ── Current connection status ──────────────────────────────────────────
    configured_sheet_id = connection.get("configured_sheet_id") or "Not configured"
    st.caption(f"GOOGLE_SHEET_ID: {configured_sheet_id}")

    if connection.get("connected"):
        st.success(str(connection.get("message", "Connected.")))
        st.caption(
            f"Linked spreadsheet: {connection.get('spreadsheet_title', 'Unknown')} ({connection.get('spreadsheet_id', 'N/A')})"
        )
    else:
        st.error(str(connection.get("message", "Disconnected.")))

    if st.button("Initialize & Format Worksheets", type="primary"):
        try:
            result = get_database().initialize_and_format_worksheets()
            st.success("Worksheets initialized and formatted successfully.")

            c1, c2, c3 = st.columns(3)
            c1.metric("Ledger Columns", int(result.get("ledger_header_columns", 0)))
            c2.metric("Performance Columns", int(result.get("performance_header_columns", 0)))
            c3.metric("Genesis Row Injected", "Yes" if result.get("genesis_row_written") else "Already Present")
        except Exception as exc:
            st.error(f"Initialization/formatting failed: {exc}")

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
        "This will create a brand-new simulation session and switch active worksheets. "
        "Historical worksheets remain preserved for audit and review."
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
                st.success(
                    "New simulation started successfully. "
                    f"Active worksheets: {result.get('active_ledger_worksheet')} / "
                    f"{result.get('active_performance_worksheet')}."
                )
                st.info("Proceed to Trading Desk to begin trading in the new empty simulation.")
            except Exception as exc:
                st.error(f"Failed to start new simulation: {exc}")


main()
