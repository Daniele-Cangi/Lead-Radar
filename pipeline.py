from collector.search import run_search
from collector.fetch import fetch_profiles
from cleaner.normalize import clean_profiles
from profiler.classify import score_profiles
from pitcher.generate import make_pitches
from connector.linkgen import generate_links
from reporter.assemble import assemble_report

def main():
    # Step 1: search + fetch
    raw = run_search("EtherCAT AND Automation Engineer")
    profiles = fetch_profiles(raw)

    # Step 2: clean
    clean = clean_profiles(profiles)

    # Step 3: score with AI
    scored = score_profiles(clean)

    # Step 4: generate personalized pitches
    pitched = make_pitches(scored)

    # Step 5: generate links for demo
    linked = generate_links(pitched)

    # Step 6: final report
    assemble_report(linked, top_n=20)

if __name__ == "__main__":
    main()
