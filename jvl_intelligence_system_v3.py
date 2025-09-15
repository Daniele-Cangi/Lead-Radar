#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JVL Intelligence System — v3 (single-file, copy-paste)
------------------------------------------------------
- Tech tagging (EtherCAT / PROFINET / EtherNet/IP / ROS2 / UR/AMR / Modbus ...)
- ML-assisted relevance (tiny Naive-Bayes-like + heuristics)
- AI relevance scoring via HTTP (local Reson STRICT endpoint) with robust JSON parse
- Sales priority score (HOT/WARM/COLD)
- English pitch + unique tracking links (compatible with your FastAPI tracker)
- DEMO mode (no network) and CSV input mode

USAGE (demo, no network):
    python jvl_intelligence_system_v3.py --demo --top 20

USAGE (CSV you provide):
    python jvl_intelligence_system_v3.py --input my_profiles.csv --top 20

(ONLINE scraping skeleton is left as hook; replace `from_online()` with your Playwright module.)

Env vars (optional):
    RED_API=http://127.0.0.1:8089/classify
    BASE_URL=http://localhost:8787
    DEMO_URL=http://localhost:8866
"""

from __future__ import annotations

import os
import sys
import csv
import json
import math
import time
import uuid
import re
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse, unquote

# -------- CONFIG --------
RESON_API = os.getenv("RESON_API", "http://127.0.0.1:8089/classify")
BASE_URL  = os.getenv("BASE_URL",  "http://localhost:8787")   # tracker base
DEMO_URL  = os.getenv("DEMO_URL",  "http://localhost:8866")   # demo app base

# Tech tagging dictionary
TECH_KEYWORDS: Dict[str, List[str]] = {
    "EtherCAT":    ["ethercat", "beckhoff", "twincat"],
    "PROFINET":    ["profinet", "gsdml", "siemens tia", "plc siemens", "tia portal", "tiaportal"],
    "EtherNet_IP": ["ethernet/ip", "ethernet ip", "rockwell", "allen bradley", "studio 5000", "aoi"],
    "ROS2":        ["ros2", "gazebo", "rclcpp", "ros industrial", "ros-industrial"],
    "UR_Cobot":    ["universal robots", " ur ", "cobot", "urcap", "amr", "mobile robot"],
    "Modbus":      ["modbus", "modbus tcp", "modbus rtu"],
    "Mechatronics":["mechatronics", "controls engineer", "automation engineer", "motion control"],
    "ServoStepper":["servo motor", "stepper", "servo drive", "integrated motor", "servo integrated"],
}

# Seed samples for tiny NB-like model
POSITIVE_SAMPLES = [
    "Automation Engineer EtherCAT Beckhoff TwinCAT motion control",
    "Controls Engineer Siemens TIA PROFINET GSDML packaging line",
    "Rockwell Studio 5000 AOI Developer EtherNet/IP",
    "Robotics Engineer Universal Robots UR ROS2 AMR",
    "Mechatronics servo motor integrated motor Modbus TCP",
    "ROS2 rclcpp Gazebo URCap EtherCAT bridge",
]
NEGATIVE_SAMPLES = [
    "Electrical engineer low voltage panels lighting",
    "Sales representative CRM pipeline prospecting",
    "Marketing specialist content strategy social media",
    "Student seeking internship no experience",
    "HVAC technician maintenance air conditioning",
]

# -------- Data Model --------
@dataclass
class Profile:
    name: str = ""
    title: str = ""
    company: str = ""
    linkedin_url: str = ""
    headline: str = ""
    about: str = ""
    activity: str = ""
    connection_level: str = "3rd"
    mutual_connections: int = 0
    open_profile: bool = False
    recent_activity_days: int = 90
    hq_country: str = "DK"

    # derived
    tech_tags: List[str] | None = None
    relevance_score: int = 0
    reason: str = ""
    priority_score: float = 0.0
    priority_class: str = "COLD"
    pitch_text: str = ""
    tracking_link: str = ""
    token: str = ""

    def as_row(self) -> Dict[str, Any]:
        d = asdict(self)
        d["tech_tags"] = ", ".join(self.tech_tags or [])
        d["priority_score"] = round(self.priority_score, 1)
        return d

# -------- Utils --------
def _token() -> str:
    return str(uuid.uuid4())

def extract_company_from_url(url: str) -> str:
    try:
        parsed = urlparse(url or "")
        domain = parsed.netloc.lower()
        if "linkedin.com" in domain and "/company/" in (url or ""):
            slug = url.split("/company/")[1].split("/")[0]
            return unquote(slug).replace("-", " ").title()
        base = domain.replace("www.", "").split(".")[0]
        return base.title() if base else "Unknown Company"
    except Exception:
        return "Unknown Company"

# -------- Tiny NB-like model --------
class MiniNB:
    """Very small NB-like text scorer using token log-odds with Laplace smoothing."""
    def __init__(self):
        self.vocab: Dict[str, Tuple[int,int]] = {}  # token -> (pos, neg)
        self.pos_total = 0
        self.neg_total = 0

    def _tok(self, text: str) -> List[str]:
        text = (text or "").lower()
        return re.findall(r"[a-zA-Z0-9\-/\+_.#]+", text)

    def fit(self, positives: List[str], negatives: List[str]):
        for s in positives:
            for w in self._tok(s):
                p, n = self.vocab.get(w, (0, 0))
                self.vocab[w] = (p+1, n)
                self.pos_total += 1
        for s in negatives:
            for w in self._tok(s):
                p, n = self.vocab.get(w, (0, 0))
                self.vocab[w] = (p, n+1)
                self.neg_total += 1

    def score(self, text: str) -> float:
        eps = 1.0
        s = 0.0
        V = max(1, len(self.vocab))
        for w in self._tok(text):
            p, n = self.vocab.get(w, (0, 0))
            p = (p + eps) / (self.pos_total + eps * V + 1e-9)
            n = (n + eps) / (self.neg_total + eps * V + 1e-9)
            s += math.log((p + 1e-9) / (n + 1e-9))
        return s

    def prob(self, text: str) -> float:
        s = self.score(text)
        return 1.0 / (1.0 + math.exp(-s))

NB = MiniNB()
NB.fit(POSITIVE_SAMPLES, NEGATIVE_SAMPLES)

# -------- Tagging --------
def extract_tech_tags(text: str) -> List[str]:
    t = (text or "").lower()
    out: List[str] = []
    for tag, kws in TECH_KEYWORDS.items():
        for k in kws:
            if k.lower() in t:
                out.append(tag); break
    # dedupe
    seen = set(); tags: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x); tags.append(x)
    return tags

# -------- AI scoring (Reson STRICT over HTTP) --------
def parse_json_score_reason(s: str) -> Optional[Dict[str, Any]]:
    # strict pattern {"score": <int>, "reason": "<...>"}
    m = re.search(r'\{\s*"score"\s*:\s*(\d+)\s*,\s*"reason"\s*:\s*"([^"]*)"\s*\}', s, flags=re.DOTALL)
    if not m:
        return None
    try:
        obj = {"score": int(m.group(1)), "reason": m.group(2).strip()}
        return obj
    except Exception:
        return None

def classify_with_reson(text: str) -> Dict[str, Any]:
    # Try HTTP call to Reson; fallback to NB + tags
    try:
        import requests  # lazy
        r = requests.post(RESON_API, json={"text": text}, timeout=8)
        if r.ok:
            j = r.json()
            # Accept either parsed dict or attempt to parse string
            if isinstance(j, dict) and "score" in j and "reason" in j:
                score = max(0, min(100, int(j["score"])))
                reason = str(j["reason"]).strip() or "No reason"
                return {"score": score, "reason": reason}
            if isinstance(j, str):
                obj = parse_json_score_reason(j)
                if obj:
                    obj["score"] = max(0, min(100, int(obj["score"])))
                    if not obj.get("reason"): obj["reason"] = "No reason"
                    return obj
    except Exception:
        pass

    # Fallback: NB + tags
    prob = NB.prob(text)
    base = int(round(prob * 100))
    tags = extract_tech_tags(text)
    reason = "Detected tags: " + (", ".join(tags) if tags else "generic")
    return {"score": max(35, base), "reason": reason}

# -------- Priority scoring for Sales --------
def _activity_score(days: Optional[int]) -> float:
    if days is None: return 0.0
    days = max(0, int(days))
    # 0d -> 100; 90d -> 0
    return max(0.0, 100.0 - (days / 90.0) * 100.0)

def _contactability_score(open_profile: bool, connection_level: str, mutual: int) -> float:
    s  = 40.0 if open_profile else 0.0
    s += 30.0 if str(connection_level).startswith("2") else 0.0
    s += min(30.0, 10.0 + 5.0 * max(0, int(mutual)))
    return min(100.0, s)

def _geo_fit(country: str) -> float:
    return 100.0 if (country or "").upper() in {"DK","SE","DE","NL"} else 30.0

def _committee_role(title: str) -> float:
    r = (title or "").lower()
    if any(k in r for k in ["head","lead","manager","director"]): return 100.0
    if any(k in r for k in ["architect","senior","consultant"]):  return 70.0
    if any(k in r for k in ["engineer","technician"]):            return 60.0
    return 40.0

def compute_priority(p: Profile) -> float:
    return (
        0.35 * float(p.relevance_score) +
        0.20 * _activity_score(p.recent_activity_days) +
        0.15 * _contactability_score(p.open_profile, p.connection_level, p.mutual_connections) +
        0.15 * _geo_fit(p.hq_country) +
        0.15 * _committee_role(p.title)
    )

# -------- Pitch + link --------
def make_tracking_link() -> Tuple[str, str]:
    tok = _token()
    return tok, f"{BASE_URL}/t/{tok}"

def make_pitch_en(name: str, company: str, tech_tags: List[str], tracking_link: str) -> str:
    tech = (tech_tags[0] if tech_tags else "EtherCAT")
    who  = name or "there"
    comp = company or "your line"
    return (
        f"Hi {who} — we built a 60-sec demo: JVL MAC + {tech} + predictive health "
        f"(reasons, not just error codes). No vendor lock-in (ECAT/PN/EIP swap).\n"
        f"1-min link: {tracking_link}\n"
        f"If relevant, I’ll tailor it to {comp}."
    )

# -------- DEMO data --------
def demo_profiles() -> List[Profile]:
    return [
        Profile(name="Lars M.", title="Automation Engineer", company="Odense Robotics",
                linkedin_url="https://linkedin.com/in/lars",
                headline="Automation Engineer | EtherCAT | Beckhoff TwinCAT",
                about="Motion control projects", activity="Posts on EtherCAT sync; TwinCAT scope usage.",
                connection_level="2nd", mutual_connections=5, open_profile=True, recent_activity_days=6, hq_country="DK"),
        Profile(name="Sofia K.", title="Controls Engineer", company="Siemens Partner",
                linkedin_url="https://linkedin.com/in/sofia",
                headline="Controls Engineer (Siemens TIA/PROFINET)",
                about="Packaging lines", activity="Commented on Profinet diagnostics",
                connection_level="3rd", mutual_connections=1, open_profile=False, recent_activity_days=45, hq_country="DE"),
        Profile(name="Pedro R.", title="Automation Engineer", company="Rockwell Integrator",
                linkedin_url="https://linkedin.com/in/pedro",
                headline="Rockwell Studio 5000 AOI Developer",
                about="AOI motion libraries", activity="Shared AOI code snippets",
                connection_level="2nd", mutual_connections=3, open_profile=True, recent_activity_days=12, hq_country="NL"),
        Profile(name="Maja N.", title="Robotics Engineer", company="Cobot Labs",
                linkedin_url="https://linkedin.com/in/maja",
                headline="Robotics Engineer (UR/ROS2/AMR)", about="UR5e + Gazebo sim",
                activity="rclcpp node; ROS-Industrial",
                connection_level="2nd", mutual_connections=4, open_profile=True, recent_activity_days=3, hq_country="DK"),
        Profile(name="Kenji T.", title="Mechatronics Engineer", company="ServoTech",
                linkedin_url="https://linkedin.com/in/kenji",
                headline="Mechatronics / Motion Control | Servo motors | Modbus TCP",
                about="Stepper retrofits", activity="Thread on Modbus RTU vs TCP",
                connection_level="3rd", mutual_connections=0, open_profile=False, recent_activity_days=120, hq_country="SE"),
        Profile(name="Anna P.", title="Electrical Engineer", company="PanelWorks",
                linkedin_url="https://linkedin.com/in/anna",
                headline="Electrical Engineer",
                about="LV panels", activity="Posts about safety relays",
                connection_level="3rd", mutual_connections=1, open_profile=False, recent_activity_days=200, hq_country="DK"),
    ]

# -------- CSV input loader --------
def load_profiles_from_csv(path: str) -> List[Profile]:
    rows: List[Profile] = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for d in r:
            rows.append(Profile(
                name=d.get("name","").strip(),
                title=d.get("title","").strip(),
                company=d.get("company","").strip(),
                linkedin_url=d.get("linkedin_url","").strip(),
                headline=d.get("headline","").strip(),
                about=d.get("about","").strip(),
                activity=d.get("activity","").strip(),
                connection_level=(d.get("connection_level","") or "3rd").strip(),
                mutual_connections=int(d.get("mutual_connections","0") or 0),
                open_profile=str(d.get("open_profile","")).strip().lower() in {"1","true","yes"},
                recent_activity_days=int(d.get("recent_activity_days","90") or 90),
                hq_country=(d.get("hq_country","DK") or "DK").strip().upper(),
            ))
    return rows

# -------- Analyze pipeline --------
def analyze_profiles(profiles: List[Profile]) -> List[Profile]:
    out: List[Profile] = []
    for p in profiles:
        blob = " | ".join([p.headline, p.about, p.activity]).strip()
        p.tech_tags = extract_tech_tags(blob)
        ai = classify_with_reson(blob)  # tries HTTP; fallback to NB+tags
        p.relevance_score = int(max(0, min(100, ai["score"])))
        p.reason = ai["reason"]
        p.priority_score = compute_priority(p)
        p.priority_class = "HOT" if p.priority_score >= 80 else ("WARM" if p.priority_score >= 60 else "COLD")
        p.token, p.tracking_link = make_tracking_link()
        p.pitch_text = make_pitch_en(p.name, p.company, p.tech_tags, p.tracking_link)
        out.append(p)
    return out

# -------- Export --------
def export_reports(profiles: List[Profile], top: int = 20, outdir: str = ".") -> Tuple[str, str]:
    os.makedirs(outdir, exist_ok=True)
    headers = [
        "name","title","company","linkedin_url",
        "connection_level","mutual_connections","open_profile","recent_activity_days","hq_country",
        "tech_tags","relevance_score","reason","priority_score","priority_class",
        "pitch_text","tracking_link","token"
    ]
    # sort: HOT first, then by score
    profiles_sorted = sorted(
        profiles,
        key=lambda p: (p.priority_class in ("HOT","WARM"), p.priority_score),
        reverse=True
    )[:top]

    csv_path = os.path.join(outdir, "lead_report_en.csv")
    md_path  = os.path.join(outdir, "lead_report.md")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers); w.writeheader()
        for p in profiles_sorted:
            row = p.as_row()
            row = {k: row.get(k, "") for k in headers}
            w.writerow(row)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# JVL Lead Report — Top {top}\n\n")
        for i, p in enumerate(profiles_sorted, start=1):
            f.write(f"**{i}. {p.name or p.title} — {p.company}**  \n")
            f.write(f"Score: {p.relevance_score} | Priority: {p.priority_class} ({p.priority_score:.1f})  \n")
            f.write(f"Tags: {', '.join(p.tech_tags or [])}  \n")
            f.write(f"Reason: {p.reason}\n\n")
            f.write(f"Pitch:\n\n> {p.pitch_text}\n\n")
            f.write(f"[Profile]({p.linkedin_url}) | [Demo link]({p.tracking_link})  \n\n---\n\n")

    return csv_path, md_path

# -------- Main CLI --------
def main():
    import argparse
    ap = argparse.ArgumentParser(description="JVL Intelligence System v3 (single-file)")
    ap.add_argument("--demo", action="store_true", help="Use built-in DEMO profiles (no network)")
    ap.add_argument("--input", type=str, default="", help="CSV with profiles to process")
    ap.add_argument("--top", type=int, default=20, help="How many leads to export")
    ap.add_argument("--outdir", type=str, default=".", help="Output directory")
    args = ap.parse_args()

    if args.demo:
        profiles = demo_profiles()
    elif args.input:
        profiles = load_profiles_from_csv(args.input)
    else:
        print("No input provided. Use --demo OR --input <csv>.", file=sys.stderr)
        sys.exit(1)

    analyzed = analyze_profiles(profiles)
    csv_path, md_path = export_reports(analyzed, top=args.top, outdir=args.outdir)
    print(f"✅ Exported: {csv_path}")
    print(f"✅ Exported: {md_path}")
    print("Tip: run your tracker (FastAPI) and demo (Streamlit) so links are live.")

if __name__ == "__main__":
    main()
