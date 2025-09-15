def score_profiles(profiles):
    """
    Usa la tua AI (Reson/LLaMA tunato) per classificare.
    Input: list[dict] con name, headline, about, activity, tech_tags
    Output: list[dict] con relevance_score, reason
    """
    results = []
    for p in profiles:
        # Qui agganci la tua AI con prompt tipo:
        # "Classifica profilo per JVL: score 0-100 e spiega in una frase"
        relevance, reason = call_your_ai(p)
        p["relevance_score"] = relevance
        p["reason"] = reason
        results.append(p)
    return results
