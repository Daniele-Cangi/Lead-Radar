
import os, sqlite3, time
from fastAPI import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from pydantic import BaseModel
from urllib.parse import urlencode

BASE_URL = os.getenv("BASE_URL", "http://localhost:8787")
DEMO_URL = os.getenv("DEMO_URL", "http://localhost:8866")
DB_PATH = os.getenv("ANALYTICS_DB", "./data/analytics.sqlite")

app = FastAPI(title="Profile Hunter — Tracker")

def ensure_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS opens(
        token TEXT,
        ts REAL,
        ip TEXT,
        ua TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS events(
        token TEXT,
        ts REAL,
        name TEXT,
        meta TEXT
    )""")
    con.commit()
    con.close()

ensure_db()

@app.get("/t/{token}")
async def track_and_redirect(token: str, request: Request):
    ip = request.client.host if request.client else ""
    ua = request.headers.get("user-agent","")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("INSERT INTO opens(token, ts, ip, ua) VALUES(?,?,?,?)", (token, time.time(), ip, ua))
    con.commit(); con.close()

    qs = urlencode({"token": token})
    return RedirectResponse(f"{DEMO_URL}/?{qs}")

class Event(BaseModel):
    token: str
    name: str
    meta: dict | None = None

@app.post("/event")
async def track_event(e: Event):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("INSERT INTO events(token, ts, name, meta) VALUES(?,?,?,?)",
                (e.token, time.time(), e.name, (e.meta and str(e.meta)) or ""))
    con.commit(); con.close()
    return JSONResponse({"ok": True})
