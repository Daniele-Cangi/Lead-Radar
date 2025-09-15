import random

def make_pitches(profiles, template="pitch_ethercat.txt"):
    """
    Genera messaggi brevi (max 4 righe) usando i template.
    """
    with open(f"config/templates/{template}") as f:
        pitch_template = f.read()
    for p in profiles:
        tech = ",".join(p.get("tech_tags", []))
        p["pitch_text"] = pitch_template.format(
            name=p["name"], company=p["company_std"], tech=tech
        )
    return profiles
