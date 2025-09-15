
import os, time, random
import streamlit as st
import requests

TRACKER_URL = os.getenv("TRACKER_URL", "http://localhost:8787")

qp = st.experimental_get_query_params()
token = (qp.get("token") or ["demo-token"])[0]

st.set_page_config(page_title="JVL Motor — AI Diagnostics Demo", layout="centered")
st.title("JVL Motor — AI Diagnostics Demo")
st.caption("Live telemetry (simulated) + explainable risk")

rpm = st.slider("RPM", min_value=0, max_value=3000, value=1800, step=50)
temp = st.slider("Temperature (°C)", min_value=20, max_value=100, value=55, step=1)
current = st.slider("Current (A)", min_value=0, max_value=20, value=6, step=1)

import random
trend = random.uniform(0.8, 1.2)
risk = min(100, max(0, int(0.3*rpm/30 + 0.6*(temp-20) + 2*max(0, current-8) * trend)))
reason = []
if temp > 70: reason.append("High temperature")
if current > 12: reason.append("Overcurrent spikes")
if rpm > 2500 and temp > 60: reason.append("High RPM at elevated temp")

st.metric("Health (lower is better)", f"{100 - risk}%")
st.progress(1 - risk/100)
st.write("**Explainable reasons**: ", ", ".join(reason) or "Nominal")

col1, col2 = st.columns(2)
if col1.button("Watch EtherCAT 60s clip"):
    try:
        requests.post(f"{TRACKER_URL}/event", json={"token": token, "name": "play_clip", "meta": {"clip":"ethercat"}}, timeout=3)
    except Exception:
        pass
    st.info("Pretend: playing EtherCAT clip…")
if col2.button("View protocol swap"):
    try:
        requests.post(f"{TRACKER_URL}/event", json={"token": token, "name": "view_swap", "meta": {}}, timeout=3)
    except Exception:
        pass
    st.info("Pretend: showing ECAT/PN/EIP swap…")

st.caption(f"Token: {token}")
