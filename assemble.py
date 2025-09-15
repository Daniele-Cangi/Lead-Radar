import csv

def assemble_report(profiles, top_n=20):
    """
    Ordina per relevance_score e dwell_time, salva CSV/MD finale.
    """
    ranked = sorted(profiles, key=lambda x: (x["relevance_score"], x.get("dwell_time",0)), reverse=True)[:top_n]
    with open("data/final/leads_final.csv","w",newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ranked[0].keys())
        writer.writeheader()
        writer.writerows(ranked)
    # anche report markdown leggibile
    with open("data/final/lead_report.md","w") as f:
        for r in ranked:
            f.write(f"- {r['name']} ({r['company_std']}) | Score {r['relevance_score']} | Reason: {r['reason']}\n")
