#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reson_csv_reader_plus.py
Reads ANY CSV, builds the best-possible text blob per row, calls Reson one-by-one,
prints SCORE/JUDGMENT/REASON, and can write an enriched CSV.
"""

import os, sys, csv, re, json, time, requests

RESON_API = os.getenv("RESON_API", "http://127.0.0.1:8089/classify")

# columns we may find in various CSVs
PREF_COLS = {
    "headline": ["headline"],
    "about": ["about","summary","bio"],
    "activity": ["activity","posts","recent_activity"],
    # fallbacks to salvage info from lead_report
    "tags": ["tech_tags","tags"],
    "title": ["title","role"],
    "reason_prev": ["reason","reason_prev"],
    "pitch": ["pitch_text","pitch"],
}

def pick(d, keys):
    for k in keys:
        if k in d and d[k]:
            return d[k]
    return ""

def build_blob(row):
    # primary fields
    h = pick(row, PREF_COLS["headline"])
    a = pick(row, PREF_COLS["about"])
    act = pick(row, PREF_COLS["activity"])
    if h or a or act:
        return " | ".join([h,a,act]).strip(" |")

    # fallback: try to compose from what we have (report-style CSV)
    title = pick(row, PREF_COLS["title"])
    tags = pick(row, PREF_COLS["tags"])
    reason_prev = pick(row, PREF_COLS["reason_prev"])
    pitch = pick(row, PREF_COLS["pitch"])

    parts = []
    if title: parts.append(title)
    if tags: parts.append(f"Tags: {tags}")
    if reason_prev: parts.append(f"Prev reason: {reason_prev}")
    if pitch: parts.append(pitch[:280])
    return " | ".join(parts)

def call_reson(text: str):
    try:
        r = requests.post(RESON_API, json={"text": text}, timeout=30)
        j = r.json()
        if isinstance(j, dict) and "score" in j and "reason" in j:
            return int(j["score"]), str(j["reason"]).strip()
        if isinstance(j, str):
            m = re.search(r'\{\s*"score"\s*:\s*(\d+)\s*,\s*"reason"\s*:\s*"([^"]*)"\s*\}', j, flags=re.DOTALL)
            if m:
                return int(m.group(1)), m.group(2).strip()
    except Exception as e:
        return 50, f"Error contacting Reson: {e}"
    return 50, "Fallback: no valid JSON"

def judge(score: int) -> str:
    if score >= 80: return "HOT lead 🔥"
    if score >= 60: return "WARM lead"
    if score >= 40: return "COLD lead"
    return "Not relevant"

def main():
    if len(sys.argv) < 2:
        print("Usage: python reson_csv_reader_plus.py input.csv [output_enriched.csv]")
        sys.exit(1)

    inp = sys.argv[1]
    out_enriched = sys.argv[2] if len(sys.argv) > 2 else ""

    with open(inp, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    writer = None
    if out_enriched:
        headers = list(rows[0].keys()) if rows else []
        for extra in ["score_v2","judgment_v2","reason_v2"]:
            if extra not in headers: headers.append(extra)
        g = open(out_enriched, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(g, fieldnames=headers)
        writer.writeheader()

    for i, row in enumerate(rows, start=1):
        name = row.get("name","") or row.get("title","") or f"Row {i}"
        blob = build_blob(row)

        if not blob.strip():
            print(f"\n=== Profile {i}: {name} ===")
            print("SCORE   : 45")
            print("JUDGMENT: COLD lead")
            print("REASON  : No text available for evaluation.")
            if writer:
                row.update({"score_v2":45, "judgment_v2":"COLD lead", "reason_v2":"No text available for evaluation."})
                writer.writerow(row)
            continue

        print(f"\n=== Profile {i}: {name} ===")
        score, reason = call_reson(blob)
        judgment = judge(score)
        print(f"SCORE   : {score}")
        print(f"JUDGMENT: {judgment}")
        print(f"REASON  : {reason}")

        if writer:
            row.update({"score_v2":score, "judgment_v2":judgment, "reason_v2":reason})
            writer.writerow(row)

        time.sleep(0.2)

    if writer:
        g.close()
        print(f"\nEnriched file written to: {out_enriched}")

if __name__ == "__main__":
    main()
