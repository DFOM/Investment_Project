from __future__ import annotations

import streamlit as st

from core.database import get_google_sheets_connection_status


def _init_session_state() -> None:
    """Set default session_state values to prevent KeyError on first render."""
    defaults: dict = {
        "active_sheet_id": "",
        "user_name": "",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def main() -> None:
    st.set_page_config(page_title="Stock Portfolio Simulator", layout="wide")

    _init_session_state()

    # ── Google Sheets connectivity check ──────────────────────────────────────
    try:
        status = get_google_sheets_connection_status()
    except Exception:
        st.error("⚠️ Connection Lost: Please check your Google Sheet ID in the Admin Panel.")
        st.stop()
        return

    if not status["connected"]:
        st.error("⚠️ Connection Lost: Please check your Google Sheet ID in the Admin Panel.")
        st.sidebar.error("⚠️ Google Sheets: Disconnected")
        st.stop()
        return

    # ── Main landing page ──────────────────────────────────────────────────────
    st.title("Stock Portfolio Simulator")
    st.caption(
        "Use the pages menu in the sidebar to open Dashboard, Trading Desk, "
        "Admin Panel, or Portfolio Deep Dive."
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("System Status")
    st.sidebar.success(
        f"✅ Connected: {status.get('spreadsheet_title', 'Unknown Sheet')}"
    )

    st.info(
        "Open pages from the Streamlit sidebar: **1_Dashboard**, **2_Trading_Desk**, "
        "**3_Admin_Panel**, or **4_Portfolio_Deep_Dive**."
    )


if __name__ == "__main__":
    main()
