"""
EU Lead Radar — Mini-API Skeleton (v1)
FastAPI app + in-memory job queue + ETG adapter (stub) according to the blueprint.

How to run (local):
  pip install fastapi uvicorn pydantic[dotenv]
  uvicorn app:api --reload --port 8080

Notes:
- This is a single-file skeleton to ease review. In real usage, split into
  /api, /core, /adapters, /storage, etc.
- ETGAdapter.scan is stubbed: it returns mocked rows shaped like real output.
  Replace TODO sections with real scraping/HTTP logic (requests/BS4 or Playwright).
"""
from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, TypedDict

from fastapi import FastAPI, HTTPException, Query
# new deps used by the upgraded ETG adapter: requests, beautifulsoup4
from pydantic import BaseModel, Field, HttpUrl, validator

# --------------------------------------------------------------------------------------
# Config (weights, regex, mapping) — minimal defaults matching the blueprint
# --------------------------------------------------------------------------------------

WEIGHTS = {
    "signals": {
        "ETG_PRODUCT": 40,
        "SIEMENS_PARTNER": 40,
        "ROK_PARTNER": 40,
        "UR_ECO": 35,
        "TRADE_FAIR": 25,
        "CLUSTER": 20,
        "ODVA_DOC": 45,
    },
    "stacks": {
        "EtherCAT": 18,
        "PROFINET": 16,
        "EtherNet/IP": 16,
        "ROS2": 12,
        "UR": 10,
        "Safety": 6,
    },
    "combos": {
        "ECAT_TWINCAT": 15,
        "PN_TIA": 15,
        "EIP_STUDIO5000": 15,
        "UR_ROS2": 10,
        "UR_FIELDBUS": 8,
        "FIELDBUS_VISION": 5,
    },
    "context": {"country_core": 10, "recency18m": 5, "segment_SI": 8, "segment_Dist": 6},
    "penalties": {"stale": -10, "vague": -8, "no_contact": -6},
}

REGEX = {
    "ethercat": r"(?i)\bEtherCAT\b",
    "twincat": r"(?i)\bTwin\s*-?\s*CAT\b",
    "profinet": r"(?i)\bPROFINET\b",
    "tia": r"(?i)\bTIA\s*-?\s*Portal\b",
    "gsdml": r"(?i)\bGSDML\b",
    "ethernetip": r"(?i)\bEther\s*Net\s*/?\s*IP\b",
    "studio5000": r"(?i)\bStudio\s*5000\b",
    "ros2": r"(?i)\bROS\s*2\b",
    "rclcpp": r"(?i)\brclcpp\b",
    "urcap": r"(?i)\bURCap\b",
    "cobot": r"(?i)\bcobot(s)?\b",
    "amr_agv": r"(?i)\bAMR\b|\bAGV\b",
    "safety": r"(?i)\bSIL\b|\bPL\s*[abcde]\b|\bFunctional\s*Safety\b|\bPilz\b",
    "vision": r"(?i)\bCognex\b|\bKeyence\b|\bHalcon\b|\bOpenCV\b",
    "email": r"[\w\.-]+@[\w\.-]+\.[a-z]{2,}",
    "contact_pages": r"(?i)contact|kontakt|contatti|contacto|impressum|om\s*os|om\s*oss",
}

SEGMENT_PRIORITY = ["SI", "Distributor", "OEM", "Tool", "Service"]
CORE_COUNTRIES = {"DK", "DE", "NL", "SE", "IT", "FR", "ES", "PL", "CZ", "AT"}

# --------------------------------------------------------------------------------------
# Data models (Pydantic) — API schemas
# --------------------------------------------------------------------------------------

SourceName = Literal[
    "ETG",
    "UR",
    "SIEMENS",
    "ROCKWELL",
    "FAIRS",
    "CLUSTERS",
    "ODVA",
    "OEM_ABB",
    "OEM_KUKA",
    "OEM_FANUC",
    "OEM_BR",
    "OEM_REXROTH",
    "OEM_SCHNEIDER",
    "OEM_MITSUBISHI",
    "OEM_YASKAWA",
]

Segment = Literal["OEM", "SI", "Distributor", "Tool", "Service"]
Priority = Literal["HOT", "WARM", "COLD"]
StackTag = Literal[
    "EtherCAT",
    "TwinCAT",
    "PROFINET",
    "TIA",
    "EtherNet/IP",
    "Studio5000",
    "ROS2",
    "UR",
    "AMR",
    "Safety",
    "Vision",
]

# Pydantic v2‑safe alias for job status
StatusType = Literal[
    "queued", "running", "scanned",
    "enriching", "enriched",
    "scoring", "scored",
    "exporting", "exported",
    "failed",
]


class ScanRequest(BaseModel):
    countries: List[str] = Field(..., description="ISO2 country codes")
    sources: List[SourceName]
    categories: List[str] = ["robotics", "motion", "fieldbus"]
    since_months: int = 18
    max_per_source: int = 2000
    respect_robots: bool = True


class EnrichRequest(BaseModel):
    job_id: str
    max_sites: int = 2000
    pdf_timeout_ms: int = 3000
    concurrency_per_domain: int = 1


class ScoreRequest(BaseModel):
    job_id: str
    stack_focus: List[StackTag] = ["EtherCAT", "PROFINET", "EtherNet/IP", "ROS2", "UR"]
    country_bonus: List[str] = list(CORE_COUNTRIES)


class ExportRequest(BaseModel):
    format: List[Literal["csv", "md"]] = ["csv", "md"]
    filters: Dict[str, Any] = {}


class SourceHit(BaseModel):
    name: SourceName
    strength: float = Field(ge=0, le=1)
    source_url: Optional[str] = None


class Lead(BaseModel):
    company_id: str
    company_name: str
    country: str
    website: Optional[HttpUrl] = None
    segment: Optional[Segment] = None
    stack_tags: List[StackTag] = Field(default_factory=list)
    sources: List[SourceHit] = Field(default_factory=list)
    score: int = Field(0, ge=0, le=100)
    priority_class: Optional[Priority] = None
    contact_email: Optional[str] = None
    contact_url: Optional[str] = None
    phone: Optional[str] = None
    reason: Optional[str] = None
    pitch: Optional[str] = None
    last_seen: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class LeadPage(BaseModel):
    items: List[Lead]
    page: int = 1
    size: int = 50
    total: int


class JobAccepted(BaseModel):
    job_id: str
    status: StatusType


class JobStatus(BaseModel):
    job_id: str
    status: StatusType
    progress: Dict[str, float] = Field(default_factory=dict)
    found: int = 0
    errors: int = 0
    started_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    message: Optional[str] = None


class ErrorModel(BaseModel):
    code: str
    message: str
    details: Optional[Dict[str, Any]] = None


# --------------------------------------------------------------------------------------
# In-memory storage (for skeleton). Replace with Postgres/Redis in real app.
# --------------------------------------------------------------------------------------

JOBS: Dict[str, JobStatus] = {}
LEADS: Dict[str, Lead] = {}
CONFIG = {"weights": WEIGHTS, "regex": REGEX, "mapping": {"segment_priority": SEGMENT_PRIORITY}}


# --------------------------------------------------------------------------------------
# Utilities: normalization, IDs, simple scoring and reason/pitch (skeleton versions)
# --------------------------------------------------------------------------------------

LEGAL_SUFFIX = re.compile(
    r"\b(gmbh(?:\s*&\s*co\.?\s*kg)?|ug|kg|ag|srl|spa|sas|sarl|sa|sl|bv|vof|aps|a/s|ab|s\.r\.o\.|a\.s\.|sp\. z o\.o\.|s\.a\.)\b",
    re.IGNORECASE,
)
STOPWORDS = re.compile(r"\b(automation|automatisierung|automazione|solutions?|systems?|robotics?|mechatronics?)\b", re.IGNORECASE)


def normalize_name(name: str) -> str:
    n = name.strip()
    n = LEGAL_SUFFIX.sub("", n)
    n = STOPWORDS.sub("", n)
    n = re.sub(r"\s+", " ", n)
    return n.strip().lower()


def company_id_from(domain: Optional[str], name: str, country: str) -> str:
    base = (domain or normalize_name(name)) + "|" + country.upper()
    return hashlib.sha1(base.encode()).hexdigest()[:16]


def best_source_label(sources: List[SourceHit]) -> str:
    if not sources:
        return ""
    # prefer highest strength
    s = sorted(sources, key=lambda x: x.strength, reverse=True)[0]
    mapping = {
        "ETG": "ETG product",
        "UR": "UR ecosystem",
        "SIEMENS": "Siemens Partner",
        "ROCKWELL": "Rockwell Partner",
        "FAIRS": "Trade fair exhibitor",
        "CLUSTERS": "Cluster member",
        "ODVA": "ODVA DOC",
    }
    return mapping.get(s.name, s.name)


def classify_priority(score: int) -> Priority:
    if score >= 80:
        return "HOT"
    if score >= 60:
        return "WARM"
    return "COLD"


def simple_score(lead: Lead) -> int:
    # Skeleton scoring: use strongest source + stack combos, plus country bonus
    score = 0
    # signals
    for sh in lead.sources:
        if sh.name == "ETG":
            score += WEIGHTS["signals"]["ETG_PRODUCT"]
        elif sh.name == "UR":
            score += WEIGHTS["signals"]["UR_ECO"]
        elif sh.name == "SIEMENS":
            score += WEIGHTS["signals"]["SIEMENS_PARTNER"]
        elif sh.name == "ROCKWELL":
            score += WEIGHTS["signals"]["ROK_PARTNER"]
        elif sh.name == "FAIRS":
            score += WEIGHTS["signals"]["TRADE_FAIR"]
        elif sh.name == "CLUSTERS":
            score += WEIGHTS["signals"]["CLUSTER"]
        elif sh.name == "ODVA":
            score += WEIGHTS["signals"]["ODVA_DOC"]
    # stacks
    stacks = set(lead.stack_tags)
    for tag, w in WEIGHTS["stacks"].items():
        if tag in stacks:
            score += w
    # combos
    if {"EtherCAT", "TwinCAT"}.issubset(stacks):
        score += WEIGHTS["combos"]["ECAT_TWINCAT"]
    if {"PROFINET", "TIA"}.issubset(stacks):
        score += WEIGHTS["combos"]["PN_TIA"]
    if {"EtherNet/IP", "Studio5000"}.issubset(stacks):
        score += WEIGHTS["combos"]["EIP_STUDIO5000"]
    if {"UR", "ROS2"}.issubset(stacks):
        score += WEIGHTS["combos"]["UR_ROS2"]
    if "UR" in stacks and ("EtherCAT" in stacks or "PROFINET" in stacks or "EtherNet/IP" in stacks):
        score += WEIGHTS["combos"]["UR_FIELDBUS"]
    if ("EtherCAT" in stacks or "PROFINET" in stacks or "EtherNet/IP" in stacks) and ("Vision" in stacks):
        score += WEIGHTS["combos"]["FIELDBUS_VISION"]
    # context
    if lead.country.upper() in CORE_COUNTRIES:
        score += WEIGHTS["context"]["country_core"]
    if lead.segment == "SI":
        score += WEIGHTS["context"]["segment_SI"]
    elif lead.segment == "Distributor":
        score += WEIGHTS["context"]["segment_Dist"]
    # penalties — skeleton does not compute stale/vague/no_contact
    return max(0, min(100, score))


def make_reason(lead: Lead) -> str:
    label = best_source_label(lead.sources)
    combo = None
    stacks = set(lead.stack_tags)
    if {"EtherCAT", "TwinCAT"}.issubset(stacks):
        combo = "ECAT+TwinCAT"
    elif {"PROFINET", "TIA"}.issubset(stacks):
        combo = "PN+TIA"
    elif {"EtherNet/IP", "Studio5000"}.issubset(stacks):
        combo = "EIP+Studio5000"
    elif {"UR", "ROS2"}.issubset(stacks):
        combo = "UR+ROS2"
    combo_txt = f" + {combo}" if combo else ""
    seg = lead.segment or ""
    return (f"{label}{combo_txt}; {lead.country} {seg}.")[:120]


def make_pitch(lead: Lead, tracker_base: str = "https://tracker.local/lnk/") -> str:
    stacks = set(lead.stack_tags)
    if {"EtherCAT", "TwinCAT"}.issubset(stacks):
        body = "MAC with EtherCAT/TwinCAT drop-in. Predictive health > error codes."
    elif {"PROFINET", "TIA"}.issubset(stacks):
        body = "MAC with PROFINET/TIA swap, no vendor lock."
    elif {"EtherNet/IP", "Studio5000"}.issubset(stacks):
        body = "MAC ready for EtherNet/IP + Studio 5000 AOI."
    elif {"UR", "ROS2"}.issubset(stacks):
        body = "ROS2 node + URCap; drop-in beside PLC."
    else:
        body = "MAC compatible with multiple fieldbuses; quick drop-in."
    link = tracker_base + lead.company_id
    return (f"Hi Automation team — 60-sec demo: {body} Link: {link}. If relevant, we'll tailor it to your line.")[:280]


# --------------------------------------------------------------------------------------
# ETG Adapter (stub)
# --------------------------------------------------------------------------------------

class RawCompany(TypedDict):
    name: str
    country: str
    website: Optional[str]
    source: SourceName
    source_url: str
    meta: Dict[str, Any]


class ETGAdapter:
    """ETG Product Guide/Members adapter (best‑effort, polite).
    Scrape ETG members/product guide pages filtered by country and parse cards.
    Notes:
      - Respects robots.txt by default (simple check on /robots.txt).
      - Throttles requests and uses short timeouts.
      - Structure may change: selectors are label-based with fallbacks.
    """

    SOURCE: SourceName = "ETG"
    BASE_URLS = [
        # Known entry points (may vary over time)
        "https://www.ethercat.org/en/members/members.html",
        "https://www.ethercat.org/en/products/products.html",
    ]

    def __init__(self, rps: float = 0.5, timeout: int = 10, respect_robots: bool = True):
        self.rps = rps
        self.timeout = timeout
        self.respect_robots = respect_robots
        self._last_req = 0.0

    # --------------------------- HTTP helpers ---------------------------
    def _sleep_if_needed(self):
        import time as _t
        gap = 1.0 / max(self.rps, 0.1)
        since = _t.time() - self._last_req
        if since < gap:
            _t.sleep(gap - since)

    def _fetch(self, url: str) -> str:
        import requests
        self._sleep_if_needed()
        self._last_req = __import__("time").time()
        headers = {"User-Agent": "LeadRadar/1.0 (+https://example.local)"}
        resp = requests.get(url, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    def _allowed(self, base: str) -> bool:
        if not self.respect_robots:
            return True
        from urllib.parse import urlparse, urljoin
        import requests
        p = urlparse(base)
        robots_url = urljoin(f"{p.scheme}://{p.netloc}", "/robots.txt")
        try:
            txt = self._fetch(robots_url)
        except Exception:
            return True  # be permissive if robots missing
        # naive allow: block if a Disallow:* for /
        return "Disallow: /" not in txt

    # --------------------------- parsing ---------------------------
    def _parse_members(self, html: str, country: str) -> List[RawCompany]:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        out: List[RawCompany] = []
        # Strategy: find cards/rows with company name; prefer elements that have country text nearby.
        # Try common patterns: tables with rows, div.cards, ul/li lists.
        cards = soup.select("div.card, div.member, li, tr") or []
        for el in cards:
            text = " ".join(el.get_text(" ", strip=True).split())
            if not text:
                continue
            # country filter (case-insensitive, allow localized names)
            if country.upper() not in text.upper():
                # sometimes country appears as flag icon or separate column; if missing, keep but tag later
                pass
            # name: first strong/a/h* inside element
            name_el = el.select_one("a, strong, h3, h4, .name")
            name = (name_el.get_text(strip=True) if name_el else None) or None
            if not name:
                continue
            # website if present as external link
            site = None
            for a in el.select("a"):
                href = a.get("href") or ""
                if href.startswith("http") and "ethercat.org" not in href:
                    site = href
                    break
            source_url = None
            # internal details link
            det = el.select_one("a[href*='member'], a[href*='product'], a[href*='details']")
            if det and det.get("href"):
                from urllib.parse import urljoin
                source_url = urljoin("https://www.ethercat.org/", det.get("href"))
            out.append(
                RawCompany(
                    name=name,
                    country=country,
                    website=site,
                    source=self.SOURCE,
                    source_url=source_url or "https://www.ethercat.org/",
                    meta={},
                )
            )
        return out

    def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[RawCompany]:
        from urllib.parse import urlencode
        results: List[RawCompany] = []
        # Basic allow check on first base
        if self.BASE_URLS and not self._allowed(self.BASE_URLS[0]):
            return results
        # Try product & members pages with a naive country filter in query/hash if supported
        urls = []
        for base in self.BASE_URLS:
            urls.append(base)
            urls.append(f"{base}?{urlencode({'country': country})}")
            urls.append(f"{base}#{country}")
        seen = set()
        for url in urls:
            try:
                html = self._fetch(url)
                rows = self._parse_members(html, country)
                for rc in rows:
                    key = (rc["name"], rc["country"])  # de-dup within adapter
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append(rc)
                    if len(results) >= max_items:
                        return results
            except Exception:
                continue
        return results


# --------------------------------------------------------------------------------------
# API implementation (FastAPI) — minimal, in-memory, synchronous jobs
# --------------------------------------------------------------------------------------

api = FastAPI(title="EU Lead Radar API", version="1.0.0")


def upsert_lead_from_raw(rc: RawCompany) -> Lead:
    # build company_id
    cid = company_id_from(rc.get("website"), rc["name"], rc["country"])  # type: ignore
    # create/merge lead
    lead = LEADS.get(cid)
    if not lead:
        lead = Lead(
            company_id=cid,
            company_name=rc["name"],
            country=rc["country"],
            website=rc.get("website"),
            segment="OEM",  # ETG default; updated later by other sources/enrich
            stack_tags=["EtherCAT"],  # ETG implies EtherCAT
            sources=[SourceHit(name=rc["source"], strength=0.90, source_url=rc["source_url"])],
        )
        LEADS[cid] = lead
    else:
        # merge source
        lead.sources.append(SourceHit(name=rc["source"], strength=0.90, source_url=rc["source_url"]))
        if lead.website is None and rc.get("website"):
            lead.website = rc.get("website")  # type: ignore
        if "EtherCAT" not in lead.stack_tags:
            lead.stack_tags.append("EtherCAT")
    return lead


@api.post("/v1/jobs/scan", response_model=JobAccepted)
def start_scan(req: ScanRequest):
    job_id = f"scan_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    job = JobStatus(job_id=job_id, status="running", started_at=datetime.now(timezone.utc))
    JOBS[job_id] = job

    # For skeleton: only ETG implemented, others are TODO
    found = 0
    errors = 0
    progress = {}

    for src in req.sources:
        progress[src] = 0.0
        if src == "ETG":
            adapter = ETGAdapter()
            for country in req.countries:
                try:
                    rows = adapter.scan(country, req.since_months, req.max_per_source)
                    for rc in rows:
                        upsert_lead_from_raw(rc)
                        found += 1
                except Exception as e:  # noqa
                    errors += 1
            progress[src] = 1.0
        else:
            # Mark unimplemented sources as skipped (progress 1 but with message)
            progress[src] = 1.0
            job.message = (job.message or "") + f" Source {src} not implemented in skeleton;"

    job.status = "scanned"
    job.found = found
    job.errors = errors
    job.progress = progress
    job.updated_at = datetime.now(timezone.utc)
    return JobAccepted(job_id=job_id, status=job.status)


@api.get("/v1/jobs/{job_id}", response_model=JobStatus)
def get_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, detail=ErrorModel(code="not_found", message="Job not found").dict())
    return job


@api.post("/v1/enrich", response_model=JobAccepted)
def start_enrich(req: EnrichRequest):
    job_prev = JOBS.get(req.job_id)
    if not job_prev or job_prev.status not in {"scanned", "enriched", "scored"}:
        raise HTTPException(409, detail=ErrorModel(code="conflict", message="Scan job required").dict())

    job_id = f"enrich_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    job = JobStatus(job_id=job_id, status="enriching", started_at=datetime.now(timezone.utc))
    JOBS[job_id] = job

    # Skeleton enrichment: add contact_url placeholder; tag TwinCAT occasionally
    updated = 0
    for lead in LEADS.values():
        if not lead.contact_email and not lead.contact_url:
            lead.contact_url = (lead.website or "").rstrip("/") + "/contact" if lead.website else None
        # naive tagging demo: alternate TwinCAT presence
        if "TwinCAT" not in lead.stack_tags and hash(lead.company_id) % 2 == 0:
            lead.stack_tags.append("TwinCAT")
        updated += 1
    job.status = "enriched"
    job.found = updated
    job.updated_at = datetime.now(timezone.utc)
    return JobAccepted(job_id=job_id, status=job.status)


@api.post("/v1/score", response_model=JobAccepted)
def start_score(req: ScoreRequest):
    job_prev = JOBS.get(req.job_id)
    if not job_prev or job_prev.status not in {"enriched", "scored", "scanned"}:
        raise HTTPException(409, detail=ErrorModel(code="conflict", message="Enriched job required").dict())

    job_id = f"score_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    job = JobStatus(job_id=job_id, status="scoring", started_at=datetime.now(timezone.utc))
    JOBS[job_id] = job

    for lead in LEADS.values():
        lead.score = simple_score(lead)
        lead.priority_class = classify_priority(lead.score)
        lead.reason = make_reason(lead)
        lead.pitch = make_pitch(lead)
        lead.last_seen = datetime.now(timezone.utc)
    job.status = "scored"
    job.updated_at = datetime.now(timezone.utc)
    return JobAccepted(job_id=job_id, status=job.status)


@api.get("/v1/leads", response_model=LeadPage)
def list_leads(
    country: Optional[str] = None,
    segment: Optional[Segment] = None,
    priority: Optional[Priority] = None,
    stack: Optional[StackTag] = None,
    source: Optional[SourceName] = None,
    q: Optional[str] = None,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
):
    rows = list(LEADS.values())
    if country:
        rows = [r for r in rows if r.country.upper() == country.upper()]
    if segment:
        rows = [r for r in rows if r.segment == segment]
    if priority:
        rows = [r for r in rows if r.priority_class == priority]
    if stack:
        rows = [r for r in rows if stack in r.stack_tags]
    if source:
        rows = [r for r in rows if any(sh.name == source for sh in r.sources)]
    if q:
        ql = q.lower()
        rows = [r for r in rows if ql in r.company_name.lower() or (r.website and ql in r.website.lower())]
    total = len(rows)
    start = (page - 1) * size
    end = start + size
    return LeadPage(items=rows[start:end], page=page, size=size, total=total)


@api.post("/v1/export", response_model=JobAccepted)
def export_leads(req: ExportRequest):
    # Skeleton: pretend export done, return exported status
    job_id = f"export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    JOBS[job_id] = JobStatus(job_id=job_id, status="exported", started_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc))
    return JobAccepted(job_id=job_id, status="exported")


@api.get("/v1/config")
def get_config():
    return CONFIG


# Entry point for uvicorn: uvicorn app:api --reload --port 8080

# Main runner — avvio diretto con "py lead_radar_api_skeleton_v_1.py"
if __name__ == "__main__":
    import uvicorn
    host = os.getenv("LEADR_HOST", "127.0.0.1")
    port = int(os.getenv("LEADR_PORT", "5050"))
    print(f"[LeadRadar] Starting API on http://{host}:{port}")
    print("[LeadRadar] Remember to install extra deps: pip install requests beautifulsoup4")
    uvicorn.run(api, host=host, port=port)
