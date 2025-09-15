#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reson_live_cli.py
Live, one-profile-at-a-time classifier that prints results directly to the console.
- Talks to your Reson HTTP endpoint (default: http://127.0.0.1:8089/classify)
- Shows SCORE and REASON on screen
- (Optional) appends results to a CSV if --csv is provided

Usage:
  python reson_live_cli.py
  python reson_live_cli.py --csv lead_report_en.csv

Set endpoint via env:
  RESON_API=http://127.0.0.1:8089/classify
"""

import os, sys, csv, re, json, time
from typing import Dict, Any, List

RESON_API = os.getenv("RESON_API", "http://127.0.0.1:8089/classify")

TECH_KEYWORDS = {
    "EtherCAT":    ["ethercat","beckhoff","twincat"],
    "PROFINET":    ["profinet","gsdml","siemens tia","tia portal","plc siemens"],
    "EtherNet_IP": ["ethernet/ip","ethernet ip","rockwell","allen bradley","studio 5000","aoi"],
    "ROS2":        ["ros2","gazebo","rclcpp","ros-industrial","ros industrial"],
    "UR_Cobot":    ["universal robots"," ur ","cobot","urcap","amr","mobile robot"],
    "Modbus":      ["modbus","modbus tcp","modbus rtu"],
    "Motion":      ["motion control","mechatronics","servo","integrated motor","stepper"],
}

def tag_text(text: str) -> List[str]:
    t = (text or "").lower()
    tags = []
    for tag, kws in TECH_KEYWORDS.items():
        if any(k in t for k in kws):
            tags.append(tag)
    # dedupe keep order
    seen = set(); out = []
    for tg in tags:
        if tg not in seen:
            seen.add(tg); out.append(tg)
    return out

def call_reson(text: str) -> Dict[str, Any]:
    try:
        import requests
    except ImportError:
        print("ERROR: requests not installed. Run: pip install requests", file=sys.stderr)
        return {"score": 50, "reason": "requests not installed"}

    try:
        r = requests.post(RESON_API, json={"text": text}, timeout=30)
    except Exception as e:
        return {"score": 50, "reason": f"Cannot reach Reson: {e}"}

    # Accept dict JSON or string with JSON
    try:
        body = r.json()
    except Exception:
        return {"score": 50, "reason": "Non-JSON response from Reson"}

    if isinstance(body, dict) and "score" in body and "reason" in body:
        try:
            score = int(body["score"])
        except Exception:
            score = 50
        reason = str(body.get("reason", "")).strip() or "No reason provided"
        return {"score": max(0, min(100, score)), "reason": reason}

    if isinstance(body, str):
        m = re.search(r'\{\s*"score"\s*:\s*(\d+)\s*,\s*"reason"\s*:\s*"([^"]*)"\s*\}', body, flags=re.DOTALL)
        if m:
            score = max(0, min(100, int(m.group(1))))
            reason = m.group(2).strip() or "No reason provided"
            return {"score": score, "reason": reason}

    return {"score": 50, "reason": "Unable to parse Reson output"}

def append_csv(path: str, row: Dict[str, Any], header: List[str]):
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in header})

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Live Reson classifier (one profile at a time)")
    ap.add_argument("--csv", type=str, default="", help="Optional CSV path to append results")
    args = ap.parse_args()

    csv_path = args.csv
    header = ["name","title","company","linkedin_url","headline","about","activity","tags","score","reason"]

    print("Reson Live CLI — English only")
    print(f"Endpoint: {RESON_API}")
    if csv_path:
        print(f"Append to CSV: {csv_path}")
    print("\nPaste one profile at a time. Leave a field empty if unknown.\n")

    while True:
        try:
            name     = input("Name (or blank): ").strip()
            title    = input("Title (or blank): ").strip()
            company  = input("Company (or blank): ").strip()
            li_url   = input("LinkedIn URL (or blank): ").strip()

            print("\nPaste HEADLINE (single line). Press Enter when done:")
            headline = input("> ").strip()

            print("\nPaste ABOUT (single/multi line). Finish with a single line containing only 'END':")
            about_lines = []
            while True:
                line = input()
                if line.strip() == "END":
                    break
                about_lines.append(line)
            about = "\n".join(about_lines).strip()

            print("\nPaste ACTIVITY / recent posts (single/multi line). Finish with 'END':")
            act_lines = []
            while True:
                line = input()
                if line.strip() == "END":
                    break
                act_lines.append(line)
            activity = "\n".join(act_lines).strip()

            blob = " | ".join([headline, about, activity]).strip()
            print("\nCalling Reson…")
            res = call_reson(blob)

            tags = tag_text(blob)
            score  = int(res.get("score", 50))
            reason = str(res.get("reason", "No reason provided"))

            # Pretty print
            print("\n================ RESULT ================")
            print(f"Name:   {name or '-'}")
            print(f"Title:  {title or '-'}")
            print(f"Company:{company or '-'}")
            print(f"URL:    {li_url or '-'}")
            print("---------------------------------------")
            print(f"SCORE:  {score}")
            print(f"REASON: {reason}")
            print(f"TAGS:   {', '.join(tags) if tags else '-'}")
            print("=======================================\n")

            if csv_path:
                append_csv(csv_path, {
                    "name": name, "title": title, "company": company, "linkedin_url": li_url,
                    "headline": headline, "about": about, "activity": activity,
                    "tags": ", ".join(tags), "score": score, "reason": reason
                }, header)
                print(f"Appended to {csv_path}\n")

            cont = input("Press Enter for next profile, or type 'q' to quit: ").strip().lower()
            if cont == "q":
                print("Bye.")
                break

        except KeyboardInterrupt:
            print("\nInterrupted. Bye.")
            break
        except EOFError:
            print("\nEOF. Bye.")
            break

if __name__ == "__main__":
    main()
