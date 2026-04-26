"""Lightweight auth helpers for Streamlit session state."""
from __future__ import annotations

import streamlit as st

import db


def is_admin() -> bool:
    return st.session_state.get("is_admin", False)


def current_user() -> str:
    return st.session_state.get("admin_username", "Viewer")


def login(username: str, password: str) -> bool:
    if db.verify_admin(username.strip(), password):
        st.session_state["is_admin"] = True
        st.session_state["admin_username"] = username.strip()
        return True
    return False


def logout() -> None:
    for k in ("is_admin", "admin_username"):
        if k in st.session_state:
            del st.session_state[k]
