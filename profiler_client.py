
"""
profiler_client.py — minimal client to call the local Reson classifier API.
Usage:
  python profiler_client.py "headline | about | activity"
"""
import sys, json, requests
API = "http://127.0.0.1:8089/classify"

def main():
    if len(sys.argv) < 2:
        print("Usage: python profiler_client.py \"text to classify\"")
        sys.exit(1)
    text = sys.argv[1]
    r = requests.post(API, json={"text": text}, timeout=30)
    print(json.dumps(r.json(), ensure_ascii=False))

if __name__ == "__main__":
    main()
