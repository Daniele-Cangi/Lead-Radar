from __future__ import annotations
import os, re, uuid, time, random, logging, hashlib, json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Literal, Any, TypedDict, Tuple
from urllib.parse import urlparse, urljoin

# ============================== logging ==============================
LOG_LEVEL = os.getenv("LEADR_LOG", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="[%(levelname)s] %(message)s")
log = logging.getLogger("LeadRadar")

# ============================== config ===============================
EU_COUNTRIES = ["AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT","LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE"]
EEA_EXTRA   = ["NO","IS","LI"]
UK_CH       = ["UK","GB","CH"]
DACH        = ["DE","AT","CH"]
NORDICS     = ["DK","SE","NO","FI","IS"]
BENELUX     = ["BE","NL","LU"]
IBERIA      = ["ES","PT"]
CEE         = ["PL","CZ","SK","HU","RO","BG","SI","HR","LT","LV","EE"]

REGION_EXPANSIONS: Dict[str, List[str]] = {
    "EU": EU_COUNTRIES,
    "EU_EEA_PLUS": EU_COUNTRIES + EEA_EXTRA + UK_CH,
    "EEA": EU_COUNTRIES + EEA_EXTRA,
    "DACH": DACH,
    "NORDICS": NORDICS,
    "BENELUX": BENELUX,
    "IBERIA": IBERIA,
    "CEE": CEE,
}

WEIGHTS = {
    "signal": 40,
    "stacks": {
        "EtherCAT": 25,
        "PROFINET": 20,
        "EtherNet/IP": 18,
        "ROS2": 12,
        "UR": 10,
        "TwinCAT": 8,
        "TIA": 8,
        "Studio5000": 8,
    },
}

REGEX = {
    "ethercat": re.compile(r"\bEther\s*CAT\b|\bEtherCAT\b", re.I),
    "profinet": re.compile(r"\bPROFINET\b", re.I),
    "ethernetip": re.compile(r"\bEtherNet\s*/?IP\b|\bENIP\b", re.I),
    "ros2": re.compile(r"\bROS\s*2\b|\bROS2\b", re.I),
    "twincat": re.compile(r"\bTwinCAT\b|\bTwinCAT\s*3\b", re.I),
    "tia": re.compile(r"\bTIA\s*Portal\b", re.I),
    "studio5000": re.compile(r"\bStudio\s*5000\b", re.I),
    "email": re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I),
    "phones": re.compile(r"(\+\d{1,3}[\s.-]?)?(\(?\d{1,4}\)?[\s.-]?)?[\d\s.-]{6,}", re.I),
    "email_obf": re.compile(r"([A-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\sat\s|@)\s*([A-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\sdot\s|\.|\s)\s*([A-Z]{2,})", re.I),
    "fieldbus_generic": re.compile(r"\bindustrial\s+(ethernet|networks?)\b|\breal[-\s]?time\s+ethernet\b|\bfield\s*bus|fieldbus\b", re.I),
    "motion_plc": re.compile(r"\bmotion\s+control\b|\bPLC\b|\bIEC\s*61131\b|\bCodesys|CODESYS\b|\bOPC\s*UA\b|\bTSN\b", re.I),
    "contact_link": re.compile(r"(contact|kontakt|contacts|impressum|contatti|contato|about|company|team|management|leadership|case|project|reference|news|press|product|solution|technology|industr)", re.I),
}
LINK_HINTS = {
    "contact": 0.95, "kontakt": 0.95, "contacts": 0.95, "impressum": 0.95, "contatti": 0.95, "contato": 0.95,
    "about": 0.9, "company": 0.85, "chi siamo": 0.85, "über uns": 0.85, "acerca": 0.85,
    "team": 0.8, "management": 0.8, "leadership": 0.8,
    "case": 0.8, "project": 0.8, "referenc": 0.8, "success": 0.75, "customers": 0.7,
    "news": 0.65, "press": 0.65, "events": 0.5,
    "product": 0.6, "solution": 0.6, "technology": 0.7, "industr": 0.6,
    "partners": 0.7, "ecosystem": 0.6,
}
VENDOR_PARTNERS = ["Siemens","Beckhoff","ABB","FANUC","KUKA","Yaskawa","Mitsubishi","Schneider","Rexroth","B&R","Omron","Rockwell","Universal Robots","UR","ODVA","PI","EtherCAT"]
LANG_HINTS = {"de":"DE","en":"EN","it":"IT","fr":"FR","es":"ES","pt":"PT","da":"DA","no":"NO","sv":"SV","fi":"FI","nl":"NL","pl":"PL","cs":"CS","hu":"HU","ro":"RO","bg":"BG","el":"EL","lt":"LT","lv":"LV","et":"ET"}

DEFAULT_TIMEOUT = 12
MAX_RETRIES = 5
BACKOFF_BASE = 1.7
MAX_WORKERS = int(os.getenv("LEADR_MAX_WORKERS", "12"))
PER_HOST_RPS = float(os.getenv("LEADR_PER_HOST_RPS", "0.5"))
RESPECT_ROBOTS = True

UA_POOL = [
    "LeadRadar/1.2 (+local; Python requests)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) LeadRadar/1.2",
    "Mozilla/5.0 (X11; Linux x86_64) LeadRadar/1.2",
]
