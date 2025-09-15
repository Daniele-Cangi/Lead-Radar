from __future__ import annotations

# ============================== imports ==============================
import os, re, uuid, time, random, logging, hashlib, json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Literal, Any, TypedDict, Tuple
from urllib.parse import urlparse, urljoin

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field, HttpUrl

# ============================== logging ==============================
LOG_LEVEL = os.getenv("LEADR_LOG", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="[%(levelname)s] %(message)s")
log = logging.getLogger("LeadRadar")

# ============================== config ===============================
# --- Macro region tokens ---
EU_COUNTRIES = ["AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT","LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE"]
EEA_EXTRA   = ["NO","IS","LI"]
UK_CH       = ["UK","GB","CH"]
DACH        = ["DE","AT","CH"]
NORDICS     = ["DK","SE","NO","FI","IS"]
BENELUX     = ["BE","NL","LU"]
IBERIA      = ["ES","PT"]
CEE         = ["PL","CZ","SK","HU","RO","BG","SI","HR","LT","LV","EE"]

REGION_EXPANSIONS: Dict[str, List[str]] = {
    "EU": EU_COUNTRIES,
    "EU_EEA_PLUS": EU_COUNTRIES + EEA_EXTRA + UK_CH,
    "EEA": EU_COUNTRIES + EEA_EXTRA,
    "DACH": DACH,
    "NORDICS": NORDICS,
    "BENELUX": BENELUX,
    "IBERIA": IBERIA,
    "CEE": CEE,
}

# Scoring weights (conservativi)
WEIGHTS = {
    "signal": 40,  # max weight from source strength
    "stacks": {
        "EtherCAT": 25,
        "PROFINET": 20,
        "EtherNet/IP": 18,
        "ROS2": 12,
        "UR": 10,
        "TwinCAT": 8,
        "TIA": 8,
        "Studio5000": 8,
    },
}

# Regex & keyword sets
REGEX = {
    "ethercat": re.compile(r"\bEther\s*CAT\b|\bEtherCAT\b", re.I),
    "profinet": re.compile(r"\bPROFINET\b", re.I),
    "ethernetip": re.compile(r"\bEtherNet\s*/?IP\b|\bENIP\b", re.I),
    "ros2": re.compile(r"\bROS\s*2\b|\bROS2\b", re.I),
    "twincat": re.compile(r"\bTwinCAT\b|\bTwinCAT\s*3\b", re.I),
    "tia": re.compile(r"\bTIA\s*Portal\b", re.I),
    "studio5000": re.compile(r"\bStudio\s*5000\b", re.I),
    "email": re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I),
    "phones": re.compile(r"(\+\d{1,3}[\s.-]?)?(\(?\d{1,4}\)?[\s.-]?)?[\d\s.-]{6,}", re.I),
    "email_obf": re.compile(r"([A-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\sat\s|@)\s*([A-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\sdot\s|\.|\s)\s*([A-Z]{2,})", re.I),
    "fieldbus_generic": re.compile(r"\bindustrial\s+(ethernet|networks?)\b|\breal[-\s]?time\s+ethernet\b|\bfield\s*bus|fieldbus\b", re.I),
    "motion_plc": re.compile(r"\bmotion\s+control\b|\bPLC\b|\bIEC\s*61131\b|\bCodesys|CODESYS\b|\bOPC\s*UA\b|\bTSN\b", re.I),
    "contact_link": re.compile(r"(contact|kontakt|contacts|impressum|contatti|contato|about|company|team|management|leadership|case|project|reference|news|press|product|solution|technology|industr)", re.I),
}
LINK_HINTS = {
    "contact": 0.95, "kontakt": 0.95, "contacts": 0.95, "impressum": 0.95, "contatti": 0.95, "contato": 0.95,
    "about": 0.9, "company": 0.85, "chi siamo": 0.85, "über uns": 0.85, "acerca": 0.85,
    "team": 0.8, "management": 0.8, "leadership": 0.8,
    "case": 0.8, "project": 0.8, "referenc": 0.8, "success": 0.75, "customers": 0.7,
    "news": 0.65, "press": 0.65, "events": 0.5,
    "product": 0.6, "solution": 0.6, "technology": 0.7, "industr": 0.6,
    "partners": 0.7, "ecosystem": 0.6,
}
VENDOR_PARTNERS = ["Siemens","Beckhoff","ABB","FANUC","KUKA","Yaskawa","Mitsubishi","Schneider","Rexroth","B&R","Omron","Rockwell","Universal Robots","UR","ODVA","PI","EtherCAT"]
LANG_HINTS = {"de":"DE","en":"EN","it":"IT","fr":"FR","es":"ES","pt":"PT","da":"DA","no":"NO","sv":"SV","fi":"FI","nl":"NL","pl":"PL","cs":"CS","hu":"HU","ro":"RO","bg":"BG","el":"EL","lt":"LT","lv":"LV","et":"ET"}

# Networking policy
DEFAULT_TIMEOUT = 12
MAX_RETRIES = 5
BACKOFF_BASE = 1.7
MAX_WORKERS = int(os.getenv("LEADR_MAX_WORKERS", "12"))  # completion > velocità
PER_HOST_RPS = float(os.getenv("LEADR_PER_HOST_RPS", "0.5"))
RESPECT_ROBOTS = True

UA_POOL = [
    "LeadRadar/1.2 (+local; Python requests)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) LeadRadar/1.2",
    "Mozilla/5.0 (X11; Linux x86_64) LeadRadar/1.2",
]

# ============================== types ================================
SourceName = Literal[
    "ETG", "UR", "SIEMENS", "BECKHOFF", "PI_PROFINET", "ODVA_ENIP", "ROS2"
]
Segment = Literal["OEM", "SI", "Distributor", "R&D", "University", "Other"]
Priority = Literal["HOT", "WARM", "COLD"]
StackTag = Literal[
    "EtherCAT","TwinCAT","PROFINET","TIA","EtherNet/IP","Studio5000","ROS2","UR","AMR","Safety","Vision"
]
StatusType = Literal[
    "queued","running","scanned","enriching","enriched","scoring","scored","exporting","exported","failed"
]

class RawCompany(TypedDict, total=False):
    name: str
    country: str
    website: Optional[str]
    source: SourceName
    source_url: Optional[str]
    meta: Dict[str, Any]

class SourceHit(BaseModel):
    name: SourceName
    strength: float = Field(0.75, ge=0, le=1)
    source_url: Optional[str] = None

class Contact(BaseModel):
    name: Optional[str] = None
    role: Optional[str] = None
    email: Optional[str] = None
    linkedin: Optional[str] = None
    page_url: Optional[str] = None

class CompanyContext(BaseModel):
    size_hint: Optional[str] = None
    sectors: List[str] = Field(default_factory=list)
    technologies: List[str] = Field(default_factory=list)
    partners: List[str] = Field(default_factory=list)
    recent_projects: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list)

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
    contacts: List[Contact] = Field(default_factory=list)
    context: Optional[CompanyContext] = None

class LeadPage(BaseModel):
    items: List[Lead]
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

class ScanRequest(BaseModel):
    countries: List[str]
    sources: List[str]  # accetta "ALL"
    max_per_source: int = 2000
    since_months: int = 18

class EnrichRequest(BaseModel):
    job_id: str

class DeepEnrichRequest(BaseModel):
    job_id: str
    priorities: List[Priority] = Field(default_factory=lambda: ["HOT","WARM"])
    max_leads: int = 200
    max_pages_per_lead: int = 6
    same_domain_only: bool = True

class ScoreRequest(BaseModel):
    job_id: str

class ExportRequest(BaseModel):
    format: List[Literal["csv","md"]] = Field(default_factory=lambda: ["csv","md"])
    filters: Optional[Dict[str, Any]] = None

# ============================== state ================================
JOBS: Dict[str, JobStatus] = {}
LEADS: Dict[str, Lead] = {}

# ============================== HTTP client ==========================
class RateBucket:
    def __init__(self, rps: float):
        self.rps = max(0.1, rps)
        self.min_gap = 1.0 / self.rps
        self.last = 0.0
    def wait(self):
        now = time.time()
        delta = now - self.last
        if delta < self.min_gap:
            time.sleep(self.min_gap - delta)
        self.last = time.time()

class RobustHttp:
    def __init__(self, timeout=DEFAULT_TIMEOUT, rps=PER_HOST_RPS, respect_robots=RESPECT_ROBOTS):
        self.timeout = timeout
        self.rps = rps
        self.respect_robots = respect_robots
        self.buckets: Dict[str, RateBucket] = {}
        self.cache: Dict[str, str] = {}
        self.robots_cache: Dict[str, str] = {}
    def _bucket(self, host: str) -> RateBucket:
        b = self.buckets.get(host)
        if not b:
            b = RateBucket(self.rps)
            self.buckets[host] = b
        return b
    def _headers(self) -> Dict[str,str]:
        return {"User-Agent": random.choice(UA_POOL)}
    def robots_allowed(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        p = urlparse(url)
        base = f"{p.scheme}://{p.netloc}"
        robots_url = urljoin(base, "/robots.txt")
        if robots_url in self.robots_cache:
            txt = self.robots_cache[robots_url]
        else:
            try:
                txt = self.get(robots_url, cache_ok=True)
                self.robots_cache[robots_url] = txt or ""
            except Exception:
                return True
        return "Disallow: /" not in (txt or "")
    def get(self, url: str, cache_ok: bool = True) -> str:
        if cache_ok and url in self.cache:
            return self.cache[url]
        host = urlparse(url).netloc
        if not self.robots_allowed(url):
            raise RuntimeError(f"robots disallow for {host}")
        tries = MAX_RETRIES
        backoff = BACKOFF_BASE
        for i in range(tries):
            try:
                import requests
                self._bucket(host).wait()
                resp = requests.get(url, headers=self._headers(), timeout=self.timeout)
                if 200 <= resp.status_code < 300:
                    text = resp.text or ""
                    if cache_ok:
                        self.cache[url] = text
                    return text
                if resp.status_code in (403, 429, 503):
                    time.sleep((backoff ** i) + random.random())
                else:
                    break
            except Exception:
                time.sleep((backoff ** i) + random.random() * 0.5)
        raise RuntimeError(f"GET failed after retries: {url}")

HTTP = RobustHttp()

# ============================== helpers ==============================
def soup_parse(html: str):
    from bs4 import BeautifulSoup
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def company_id_from(website: Optional[str], name: str, country: str) -> str:
    base = (website or "").strip().lower()
    if base:
        host = urlparse(base).netloc or base
        host = host.replace("www.","").strip("/")
    else:
        host = name.strip().lower().replace(" ", "-")
    raw = f"{host}|{name.strip().lower()}|{country.upper()}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

def upsert_lead_from_raw(rc: RawCompany) -> Lead:
    cid = company_id_from(rc.get("website"), rc["name"], rc["country"])  # type: ignore
    lead = LEADS.get(cid)
    default_segment: Optional[Segment] = "OEM"
    base_stacks: List[StackTag] = []
    if rc["source"] == "ETG": base_stacks.append("EtherCAT")
    if rc["source"] == "SIEMENS": default_segment = "SI"; base_stacks.extend(["PROFINET","TIA"])
    if rc["source"] == "UR": default_segment = "SI"; base_stacks.append("UR")
    if rc["source"] == "BECKHOFF": base_stacks.extend(["EtherCAT","TwinCAT"])
    if rc["source"] == "PI_PROFINET": base_stacks.append("PROFINET")
    src_strength = 0.90 if rc["source"] in {"ETG","SIEMENS","BECKHOFF","PI_PROFINET"} else 0.85
    if not lead:
        lead = Lead(
            company_id=cid, company_name=rc["name"], country=rc["country"],
            website=rc.get("website"), segment=default_segment,
            stack_tags=base_stacks,
            sources=[SourceHit(name=rc["source"], strength=src_strength, source_url=rc.get("source_url"))], # type: ignore
        ); LEADS[cid] = lead
    else:
        lead.sources.append(SourceHit(name=rc["source"], strength=src_strength, source_url=rc.get("source_url")))  # type: ignore
        if lead.website is None and rc.get("website"): lead.website = rc.get("website")  # type: ignore
        for t in base_stacks:
            if t not in lead.stack_tags: lead.stack_tags.append(t)
        if lead.segment is None: lead.segment = default_segment
    return lead

def stack_points(lead: Lead) -> int:
    return sum(WEIGHTS["stacks"].get(t, 0) for t in lead.stack_tags)

def normalize_url(base: str, href: str) -> Optional[str]:
    if not href: return None
    if href.startswith("#"): return None
    try:
        return urljoin(base, href)
    except Exception:
        return None

def score_link(label: str, href: str) -> float:
    s = 0.0
    L = f"{label} {href}".lower()
    for k,w in LINK_HINTS.items():
        if k in L:
            s = max(s, w)
    return s

def candidate_links(base_url: str, soup) -> List[Tuple[float,str]]:
    cands = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        label = a.get_text(" ", strip=True) or href
        full = normalize_url(base_url, href)
        if not full: continue
        cands.append((score_link(label, full), full))
    uniq = []
    seen = set()
    for s,u in sorted(cands, key=lambda x: x[0], reverse=True):
        if u in seen: continue
        seen.add(u); uniq.append((s,u))
    return uniq

def extract_jsonld(soup) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    orgs, persons = [], []
    for tag in soup.select("script[type='application/ld+json']"):
        try:
            data = json.loads(tag.get_text() or "{}")
            if isinstance(data, dict): data = [data]
            if not isinstance(data, list): continue
            for obj in data:
                t = obj.get("@type") or obj.get("type")
                if isinstance(t, list): t = t[0] if t else None
                if t == "Organization": orgs.append(obj)
                elif t == "Person": persons.append(obj)
        except Exception:
            continue
    return orgs, persons

def extract_contacts_from_soup(base_url: str, soup) -> List[Contact]:
    contacts: List[Contact] = []
    _, persons = extract_jsonld(soup)
    for p in persons:
        contacts.append(Contact(
            name=p.get("name"), role=(p.get("jobTitle") or p.get("role")),
            email=p.get("email"), linkedin=None, page_url=base_url
        ))
    for sel in ["section", ".team", ".management", ".leaders", "article", "div"]:
        for el in soup.select(sel):
            txt = el.get_text(" ", strip=True)
            if not txt or len(txt) < 30: continue
            if re.search(r"\b(team|management|leadership|board|staff)\b", txt, re.I):
                items = el.select("li, p, .member, .person")
                for it in items[:20]:
                    name = role = email = None; link = None
                    t = it.get_text(" ", strip=True)
                    if not t or len(t) < 4: continue
                    m = re.search(r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+", t)
                    if m: name = m.group(0)
                    m2 = re.search(r"(CEO|CTO|COO|Founder|Head|Lead|Manager|Director|Engineer|R&D|Sales|Business|Automation|Robotics)", t, re.I)
                    if m2: role = m2.group(0)
                    m3 = REGEX["email"].search(t) or REGEX["email_obf"].search(t)
                    if m3:
                        try:
                            email = f"{m3.group(1)}@{m3.group(2)}.{m3.group(3)}" if m3.re == REGEX["email_obf"] else m3.group(0)
                        except Exception: pass
                    a = it.select_one("a[href*='linkedin.com']")
                    if a and a.get("href"): link = normalize_url(base_url, a.get("href"))
                    if name or email or link:
                        contacts.append(Contact(name=name, role=role, email=email, linkedin=link, page_url=base_url))
    for m in REGEX["email"].finditer(soup.get_text(" ", strip=True)):
        em = m.group(0)
        if not any(c.email == em for c in contacts):
            contacts.append(Contact(email=em, page_url=base_url))
    return contacts[:30]

def detect_languages(soup) -> List[str]:
    langs = set()
    html_tag = soup.find("html")
    if html_tag and html_tag.get("lang"):
        lg = html_tag.get("lang").split("-")[0].lower()
        if lg in LANG_HINTS: langs.add(LANG_HINTS[lg])
    text = soup.get_text(" ", strip=True).lower()
    for k,code in LANG_HINTS.items():
        if re.search(rf"\b{k}\b", text) and len(langs) < 6:
            langs.add(code)
    return sorted(langs)

def detect_partners_and_sectors(soup) -> Tuple[List[str], List[str]]:
    text = soup.get_text(" ", strip=True)
    partners = []
    for v in VENDOR_PARTNERS:
        if re.search(rf"\b{re.escape(v)}\b", text, re.I):
            partners.append(v)
    partners = sorted(set(partners))
    sectors = []
    for h in soup.select("h1, h2, h3, .headline, .title"):
        t = h.get_text(" ", strip=True).lower()
        if any(k in t for k in ["automotive","pharma","food","packaging","logistics","semiconductor","machine","energy","agri","metal","aerospace"]):
            sectors.append(t)
    return partners[:20], sectors[:20]

def detect_stacks_extended(text: str) -> List[str]:
    tags = []
    if REGEX["ethercat"].search(text): tags.append("EtherCAT")
    if REGEX["profinet"].search(text): tags.append("PROFINET")
    if REGEX["ethernetip"].search(text): tags.append("EtherNet/IP")
    if REGEX["ros2"].search(text): tags.append("ROS2")
    if REGEX["twincat"].search(text): tags.append("TwinCAT")
    if REGEX["tia"].search(text): tags.append("TIA")
    if REGEX["studio5000"].search(text): tags.append("Studio5000")
    if REGEX["fieldbus_generic"].search(text):
        if "PROFINET" not in tags: tags.append("PROFINET")
        if "EtherNet/IP" not in tags: tags.append("EtherNet/IP")
    if REGEX["motion_plc"].search(text):
        for t in ["TwinCAT","TIA","Studio5000"]:
            if t not in tags: tags.append(t)
    return tags

# ============================== adapters =============================
class ETGAdapter:
    SOURCE: SourceName = "ETG"
    BASE_URLS = [
        "https://www.ethercat.org/en/members/members.html",
        "https://www.ethercat.org/en/products/products.html",
    ]
    def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[RawCompany]:
        results: List[RawCompany] = []; seen: set[Tuple[str,str]] = set(); urls = []
        for base in self.BASE_URLS:
            urls += [base, f"{base}?country={country}", f"{base}#{country}"]
        for url in urls:
            try:
                html = HTTP.get(url)
                soup = soup_parse(html)
                cards = soup.select("div.card, div.member, li, tr, article") or []
                for el in cards:
                    name_el = el.select_one("a, strong, h3, h4, .name")
                    name = (name_el.get_text(strip=True) if name_el else None)
                    if not name: continue
                    site = None
                    for a in el.select("a"):
                        href = a.get("href") or ""
                        if href.startswith("http") and "ethercat.org" not in href: site = href; break
                    det = el.select_one("a[href*='member'], a[href*='product'], a[href*='details']")
                    src = urljoin("https://www.ethercat.org/", det.get("href")) if (det and det.get("href")) else "https://www.ethercat.org/"
                    key = (name, country)
                    if key in seen: continue
                    seen.add(key)
                    results.append(RawCompany(name=name, country=country, website=site, source=self.SOURCE, source_url=src, meta={}))
                    if len(results) >= max_items: return results
            except Exception as e:
                log.debug(f"ETG skip {url}: {e}")
        return results

class URAdapter:
    SOURCE: SourceName = "UR"
    def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[RawCompany]:
        from urllib.parse import urlencode
        urls = [
            f"https://www.universal-robots.com/find-a-distributor/?{urlencode({'country': country})}",
            f"https://www.universal-robots.com/ur-plus/all/?{urlencode({'country': country})}",
        ]
        results: List[RawCompany] = []; seen: set[Tuple[str,str]] = set()
        for url in urls:
            try:
                html = HTTP.get(url)
                soup = soup_parse(html)
                cards = soup.select(".partner, .distributor, .card, li, article")
                for el in cards:
                    name_el = el.select_one("h3, h4, .title, .name, a")
                    name = name_el.get_text(strip=True) if name_el else None
                    if not name or len(name) < 2: continue
                    site = None
                    for a in el.select("a"):
                        href = a.get("href") or ""
                        if href.startswith("http") and "universal-robots.com" not in href: site = href; break
                    a0 = el.select_one("a"); src = urljoin("https://www.universal-robots.com/", a0.get("href")) if (a0 and a0.get("href")) else "https://www.universal-robots.com/"
                    key = (name, country)
                    if key in seen: continue
                    seen.add(key)
                    results.append(RawCompany(name=name, country=country, website=site, source=self.SOURCE, source_url=src, meta={}))
                    if len(results) >= max_items: return results
            except Exception as e:
                log.debug(f"UR skip {url}: {e}")
        return results

class SiemensAdapter:
    SOURCE: SourceName = "SIEMENS"
    def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[RawCompany]:
        url = f"https://partnerfinder.siemens.com/?country={country}"
        results: List[RawCompany] = []; seen: set[Tuple[str,str]] = set()
        try:
            html = HTTP.get(url)
            soup = soup_parse(html)
            rows = soup.select(".partner, .card, li, tr, article")
            for el in rows:
                name_el = el.select_one("h3, h4, .title, .name, a"); name = name_el.get_text(strip=True) if name_el else None
                if not name: continue
                site = None
                for a in el.select("a"):
                    href = a.get("href") or ""
                    if href.startswith("http") and "siemens.com" not in href: site = href; break
                a0 = el.select_one("a"); src = urljoin("https://partnerfinder.siemens.com/", a0.get("href")) if (a0 and a0.get("href")) else "https://partnerfinder.siemens.com/"
                key = (name, country)
                if key in seen: continue
                seen.add(key)
                results.append(RawCompany(name=name, country=country, website=site, source=self.SOURCE, source_url=src, meta={}))
                if len(results) >= max_items: return results
        except Exception as e:
            log.debug(f"SIEMENS skip {url}: {e}")
        return results

class BeckhoffAdapter:
    SOURCE: SourceName = "BECKHOFF"
    def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[RawCompany]:
        from urllib.parse import urlencode
        urls = [
            "https://www.beckhoff.com/en-en/company/partners/",
            f"https://www.beckhoff.com/en-en/company/partners/?{urlencode({'country': country})}",
            "https://www.beckhoff.com/en-en/contact/global-presence/",
        ]
        results: List[RawCompany] = []; seen: set[Tuple[str,str]] = set()
        for url in urls:
            try:
                html = HTTP.get(url)
                soup = soup_parse(html)
                cards = soup.select(".card, .partner, li, article, tr")
                for el in cards:
                    name_el = el.select_one("h3, h4, .title, .name, a, strong")
                    name = name_el.get_text(strip=True) if name_el else None
                    if not name or len(name) < 2: continue
                    site = None
                    for a in el.select("a"):
                        href = a.get("href") or ""
                        if href.startswith("http") and "beckhoff.com" not in href: site = href; break
                    a0 = el.select_one("a"); src = urljoin("https://www.beckhoff.com/", a0.get("href")) if (a0 and a0.get("href")) else "https://www.beckhoff.com/"
                    key = (name, country)
                    if key in seen: continue
                    seen.add(key)
                    results.append(RawCompany(name=name, country=country, website=site, source=self.SOURCE, source_url=src, meta={}))
                    if len(results) >= max_items: return results
            except Exception as e:
                log.debug(f"BECKHOFF skip {url}: {e}")
        return results

class PROFINETAdapter:
    SOURCE: SourceName = "PI_PROFINET"
    def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[RawCompany]:
        from urllib.parse import urlencode
        urls = [
            "https://www.profibus.com/community/members",
            f"https://www.profibus.com/community/members?{urlencode({'country': country})}",
            "https://www.profibus.com/technology/pi-competence-centers",
        ]
        results: List[RawCompany] = []; seen: set[Tuple[str,str]] = set()
        for url in urls:
            try:
                html = HTTP.get(url)
                soup = soup_parse(html)
                rows = soup.select(".member, .partner, .card, li, tr, article")
                for el in rows:
                    name_el = el.select_one("h3, h4, .title, .name, a, strong")
                    name = name_el.get_text(strip=True) if name_el else None
                    if not name: continue
                    site = None
                    for a in el.select("a"):
                        href = a.get("href") or ""
                        if href.startswith("http") and "profibus.com" not in href: site = href; break
                    a0 = el.select_one("a"); src = urljoin("https://www.profibus.com/", a0.get("href")) if (a0 and a0.get("href")) else "https://www.profibus.com/"
                    key = (name, country)
                    if key in seen: continue
                    seen.add(key)
                    results.append(RawCompany(name=name, country=country, website=site, source=self.SOURCE, source_url=src, meta={"tech":"PROFINET"}))
                    if len(results) >= max_items: return results
            except Exception as e:
                log.debug(f"PI skip {url}: {e}")
        return results

class ODVAAdapter:
    SOURCE: SourceName = "ODVA_ENIP"
    def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[RawCompany]:
        return []

class ROS2Adapter:
    SOURCE: SourceName = "ROS2"
    def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[RawCompany]:
        return []

ADAPTERS: Dict[SourceName, Any] = {
    "ETG": ETGAdapter(),
    "UR": URAdapter(),
    "SIEMENS": SiemensAdapter(),
    "BECKHOFF": BeckhoffAdapter(),
    "PI_PROFINET": PROFINETAdapter(),
    "ODVA_ENIP": ODVAAdapter(),
    "ROS2": ROS2Adapter(),
}

# ============================== API =================================
api = FastAPI(title="EU Lead Radar API", version="1.3.0")

from fastapi.responses import HTMLResponse, RedirectResponse

@api.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/ui")

@api.get("/ui", response_class=HTMLResponse, include_in_schema=False)
def ui():
    return """
<!doctype html>
<html lang="en" data-theme="light">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>LeadRadar • Pro Console</title>
<style>
  :root{
    --bg:#0f1221; --panel:#151935; --muted:#1b2042; --text:#e6e8ff; --sub:#a5abff;
    --accent:#6c8eff; --accent-2:#6ff; --ok:#3ddc97; --warn:#ffcc66; --err:#ff6b6b; --border:#2a2f57;
  }
  [data-theme="light"]{
    --bg:#f6f7fb; --panel:#ffffff; --muted:#f0f2f8; --text:#1b2144; --sub:#5560a4;
    --accent:#3757ff; --accent-2:#06b6d4; --ok:#059669; --warn:#c27803; --err:#dc2626; --border:#e6e8f2;
  }
  *{box-sizing:border-box}
  html,body{height:100%}
  body{
    margin:0;background:var(--bg);color:var(--text);
    font:14px/1.45 ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial;
  }
  .container{max-width:1200px;margin:0 auto;padding:24px}
  header.sticky{
    position:sticky;top:0;z-index:50;background:linear-gradient(180deg, rgba(0,0,0,.25), rgba(0,0,0,0)), var(--bg);
    backdrop-filter:saturate(1.2) blur(6px); border-bottom:1px solid var(--border);
  }
  .row{display:grid;gap:16px}
  @media(min-width:860px){ .row.cols-3{grid-template-columns:repeat(3,1fr)} }
  .card{background:var(--panel);border:1px solid var(--border);border-radius:14px;box-shadow:0 6px 18px rgba(0,0,0,.08)}
  .card .hd{padding:14px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
  .card .bd{padding:14px 16px}
  .title{display:flex;align-items:center;gap:10px;font-weight:600}
  .muted{color:var(--sub)}
  .kpi{font-size:22px;font-weight:700}
  .btn{display:inline-flex;align-items:center;gap:8px;padding:9px 12px;border-radius:10px;border:1px solid var(--border);
       background:linear-gradient(180deg, var(--muted), transparent);color:var(--text);cursor:pointer}
  .btn:hover{border-color:var(--accent)}
  .btn.sec{background:transparent}
  .btn.pri{background:linear-gradient(180deg, var(--accent), var(--accent-2));color:#00142a;border:0}
  .btn.small{padding:7px 10px;font-size:13px}
  .input, .select{
    width:100%;padding:10px 12px;border-radius:10px;border:1px solid var(--border);background:var(--muted);color:var(--text)
  }
  .grid-2{display:grid;gap:10px}
  @media(min-width:680px){ .grid-2{grid-template-columns:1fr 1fr} }
  .bad{display:inline-block;padding:3px 8px;border-radius:999px;font-size:12px;border:1px solid var(--border)}
  .bad.ok{background:rgba(61,220,151,.14);color:var(--ok)}
  .bad.run{background:rgba(111,255,255,.12);color:var(--accent-2)}
  .bad.done{background:rgba(55,87,255,.14);color:var(--accent)}
  .bad.err{background:rgba(220,38,38,.14);color:var(--err)}
  .table-wrap{border:1px solid var(--border);border-radius:12px;overflow:auto;max-height:420px}
  table{width:100%;border-collapse:separate;border-spacing:0}
  thead th{position:sticky;top:0;background:var(--panel);z-index:1;border-bottom:1px solid var(--border);text-align:left;padding:10px}
  tbody td{padding:10px;border-bottom:1px solid var(--border)}
  tbody tr:hover{background:var(--muted)}
  .mono{font-family:ui-monospace, SFMono-Regular, Menlo, monospace}
  .flex{display:flex;gap:10px;align-items:center}
  .right{margin-left:auto}
  .pill{padding:2px 8px;border-radius:999px;background:var(--muted);border:1px solid var(--border)}
  .toolbar{display:flex;gap:10px;flex-wrap:wrap}
  .skeleton{background:linear-gradient(90deg, rgba(255,255,255,.08), rgba(255,255,255,.18), rgba(255,255,255,.08));background-size:200% 100%;animation:s 1.2s infinite}
  @keyframes s{to{background-position:-200% 0}}
  .toast{position:fixed;bottom:18px;right:18px;background:var(--panel);border:1px solid var(--border);padding:12px 14px;border-radius:12px;display:none}
  .link{color:var(--accent);text-decoration:none}
  .switch{appearance:none;width:42px;height:24px;border-radius:999px;background:var(--muted);border:1px solid var(--border);position:relative;cursor:pointer}
  .switch:checked{background:linear-gradient(180deg, var(--accent), var(--accent-2));border:0}
  .switch:before{content:\"\";position:absolute;top:2px;left:2px;width:20px;height:20px;background:#fff;border-radius:50%;transition:.2s}
  .switch:checked:before{left:20px}
  footer{color:var(--sub);text-align:center;padding:18px}
  .icon{width:18px;height:18px;display:inline-block;vertical-align:-3px;opacity:.9}
</style>
</head>
<body>
  <header class="sticky">
    <div class="container" style="display:flex;align-items:center;gap:14px;padding:10px 24px;">
      <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M3 12h7l-2 9L21 3l-7 0 2-3z" stroke-width="1.5"/></svg>
      <div style="font-weight:700">LeadRadar <span class="muted">Pro Console</span></div>
      <div class="pill mono" id="baseUrlLabel"></div>
      <div class="right toolbar">
        <label class="flex muted" style="gap:8px;">Dark
          <input type="checkbox" id="themeToggle" class="switch">
        </label>
        <a class="link" href="/docs" target="_blank">Swagger</a>
      </div>
    </div>
  </header>

  <main class="container">
    <!-- TOP ROW -->
    <div class="row cols-3">
      <div class="card">
        <div class="hd"><div class="title">Backend</div><button class="btn small sec" id="btnHealth">Check</button></div>
        <div class="bd">
          <div class="grid-2">
            <input id="baseUrl" class="input mono" value="http://127.0.0.1:5050" />
            <div style="display:flex;gap:10px;align-items:center;">
              <span class="muted">Workers</span><span class="pill mono" id="k_workers">–</span>
            </div>
          </div>
          <pre class="mono" id="healthBox" style="margin:12px 0 0;max-height:120px;overflow:auto;background:var(--muted);padding:10px;border-radius:10px;border:1px solid var(--border)">No data yet</pre>
        </div>
      </div>

      <div class="card">
        <div class="hd"><div class="title">Quick Scan</div></div>
        <div class="bd">
          <div class="grid-2">
            <div>
              <label class="muted">Countries (comma)</label>
              <input id="fCountries" class="input" value="EU_EEA_PLUS">
            </div>
            <div>
              <label class="muted">Sources (comma)</label>
              <input id="fSources" class="input" value="ALL">
            </div>
            <div>
              <label class="muted">Max/source</label>
              <input id="fMps" type="number" class="input" value="300">
            </div>
            <div>
              <label class="muted">Since months</label>
              <input id="fSince" type="number" class="input" value="18">
            </div>
          </div>
          <div class="toolbar" style="margin-top:10px">
            <button class="btn pri" id="btnScan">
              <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="#00142a"><path d="M5 12h14M12 5l7 7-7 7" stroke-width="1.75"/></svg>
              Start
            </button>
            <div class="mono muted" id="scanMsg">Idle</div>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="hd"><div class="title">Export</div></div>
        <div class="bd">
          <div class="toolbar">
            <select id="fFormat" class="select">
              <option value="csv" selected>csv</option>
              <option value="jsonl">jsonl</option>
              <option value="md">md</option>
            </select>
            <button class="btn sec" id="btnExport">
              <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M12 3v12m0 0l-4-4m4 4 4-4M4 21h16" stroke-width="1.5"/></svg>
              Export
            </button>
            <div class="mono muted" id="exportMsg">—</div>
          </div>
        </div>
      </div>
    </div>

    <!-- JOBS -->
    <div class="card" style="margin-top:16px">
      <div class="hd">
        <div class="title">Jobs</div>
        <button class="btn small sec" id="btnJobs">
          <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M4.5 12a7.5 7.5 0 1115 0m0 0h-3m3 0l-3 3m3-3l-3-3" stroke-width="1.5"/></svg>
          Refresh
        </button>
      </div>
      <div class="bd">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th><th>Type</th><th>Status</th><th>Created</th><th>Region</th><th>Sources</th>
              </tr>
            </thead>
            <tbody id="jobsBody">
              <tr class="skeleton"><td colspan="6" style="height:48px"></td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- LEADS -->
    <div class="card" style="margin-top:16px">
      <div class="hd">
        <div class="title">Leads Preview</div>
        <div class="toolbar">
          <input id="fLimit" type="number" class="input" style="width:120px" value="100" />
          <button class="btn small sec" id="btnLeads">Load</button>
          <div class="pill">Contactable: <span id="k_contactable">0%</span></div>
        </div>
      </div>
      <div class="bd">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Company</th><th>Name</th><th>Email</th><th>Phone</th><th>Country</th><th>Score</th><th>Source</th><th>URL</th>
              </tr>
            </thead>
            <tbody id="leadsBody">
              <tr class="skeleton"><td colspan="8" style="height:52px"></td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <footer>© <span id="year"></span> LeadRadar • Professional Console</footer>
  </main>

  <div class="toast mono" id="toast"></div>

<script>
(function(){
  const $ = sel => document.querySelector(sel);
  const baseUrlInput = $('#baseUrl');
  const baseUrlLabel = $('#baseUrlLabel');
  const toast = $('#toast');
  const themeToggle = $('#themeToggle');

  // Theme
  const savedTheme = localStorage.getItem('lr_theme');
  if(savedTheme){ document.documentElement.setAttribute('data-theme', savedTheme); themeToggle.checked = savedTheme==='dark'; }
  themeToggle.addEventListener('change', ()=>{
    const next = themeToggle.checked ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('lr_theme', next);
  });

  // Utils
  const api = async (path, init) => {
    const url = baseUrlInput.value.replace(/\\/$/, '') + path;
    const res = await fetch(url, init);
    if(!res.ok) throw new Error(res.status + ' ' + res.statusText);
    const ct = res.headers.get('content-type') || '';
    return ct.includes('application/json') ? res.json() : res.text();
  }
  const show = (el, html) => el.innerHTML = html;
  const badge = (status) => {
    status = String(status||'').toLowerCase();
    const cls = status==='done'?'done': status==='running'?'run': status==='error'?'err': '';
    return '<span class="bad '+cls+'">'+(status||'?')+'</span>';
  }
  const esc = s => String(s ?? '').replace(/[&<>\"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',\"'\":'&#039;'}[m]));
  const copy = async (text) => { try{ await navigator.clipboard.writeText(text); tip('Copied'); }catch(e){ tip('Copy failed'); } }
  const tip = (msg) => { toast.textContent = msg; toast.style.display='block'; clearTimeout(tip._t); tip._t=setTimeout(()=>toast.style.display='none',1800); }

  function setBaseLabel(){ baseUrlLabel.textContent = baseUrlInput.value; }
  setBaseLabel();
  baseUrlInput.addEventListener('input', setBaseLabel);

  // Health
  $('#btnHealth').addEventListener('click', async ()=>{
    show($('#healthBox'),'…');
    try{
      const d = await api('/health');
      $('#k_workers').textContent = d.max_workers ?? '—';
      show($('#healthBox'), esc(JSON.stringify(d, null, 2)));
      tip('Health OK');
    }catch(e){ show($('#healthBox'), esc(String(e))); }
  });

  // Scan
  $('#btnScan').addEventListener('click', async ()=>{
    const payload = {
      countries: $('#fCountries').value.split(',').map(s=>s.trim()).filter(Boolean),
      sources: $('#fSources').value.split(',').map(s=>s.trim()).filter(Boolean),
      max_per_source: Number($('#fMps').value||300),
      since_months: Number($('#fSince').value||18),
    };
    $('#scanMsg').textContent = 'Starting…';
    try{
      const d = await api('/v1/jobs/scan', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
      $('#scanMsg').textContent = 'OK • job_id=' + (d.job_id || '(see /v1/jobs)');
      tip('Scan started');
      loadJobs();
    }catch(e){ $('#scanMsg').textContent = 'ERROR • ' + String(e); }
  });

  // Jobs
  async function loadJobs(){
    const body = $('#jobsBody');
    show(body, '<tr class="skeleton"><td colspan="6" style="height:38px"></td></tr>');
    try{
      const d = await api('/v1/jobs');
      const rows = (d.items || d || []).slice(0,60).map(j => {
        const reg = (j.params && j.params.countries || []).join(', ');
        const src = (j.params && j.params.sources || []).join(', ');
        return '<tr>' +
          '<td class="mono">'+esc(j.id)+'</td>'+
          '<td>'+esc(j.type || j.kind || 'scan')+'</td>'+
          '<td>'+badge(j.status)+'</td>'+
          '<td class="muted mono">'+esc(j.created_at || j.created || '')+'</td>'+
          '<td>'+esc(reg)+'</td>'+
          '<td>'+esc(src)+'</td>'+
        '</tr>';
      }).join('');
      show(body, rows || '<tr><td colspan="6" class="muted">No jobs</td></tr>');
    }catch(e){ show(body, '<tr><td colspan="6" class="muted">'+esc(String(e))+'</td></tr>'); }
  }
  $('#btnJobs').addEventListener('click', loadJobs);

  // Export
  $('#btnExport').addEventListener('click', async ()=>{
    const fmt = $('#fFormat').value;
    $('#exportMsg').textContent = 'Exporting…';
    try{
      const d = await api('/v1/export', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({format:[fmt]})});
      $('#exportMsg').textContent = (typeof d === 'string') ? d : JSON.stringify(d);
      tip('Export complete');
    }catch(e){ $('#exportMsg').textContent = 'ERROR • ' + String(e); }
  });

  // Leads
  async function loadLeads(){
    const body = $('#leadsBody');
    const limit = Number($('#fLimit').value||100);
    show(body, '<tr class="skeleton"><td colspan="8" style="height:48px"></td></tr>');
    try{
      const d = await api('/v1/leads?limit='+limit);
      const arr = (d.items || d || []);
      const rows = arr.map(r => {
        const email = Array.isArray(r.emails_found) ? (r.emails_found[0] || '') : (r.email || '');
        const phone = Array.isArray(r.phones_found) ? (r.phones_found[0] || '') : (r.phone || '');
        const url = r.source_url ? '<a class="link" href="'+esc(r.source_url)+'" target="_blank">link</a>' : '';
        return '<tr>' +
          '<td>'+esc(r.company || r.org || '')+'</td>'+
          '<td>'+esc(r.name || r.contact_name || '')+'</td>'+
          '<td class="mono">'+esc(email)+'</td>'+
          '<td class="mono">'+esc(phone)+'</td>'+
          '<td>'+esc(r.country || r.region || '')+'</td>'+
          '<td><span class="bad done">'+esc(r.score || r.priority || '')+'</span></td>'+
          '<td>'+esc(r.source || r.channel || '')+'</td>'+
          '<td>'+url+'</td>'+
        '</tr>';
      }).join('');
      show(body, rows || '<tr><td colspan="8" class="muted">No leads</td></tr>');
      // KPI contactable
      const contactable = arr.filter(r => {
        const e = Array.isArray(r.emails_found) ? r.emails_found.length : (r.email?1:0);
        return e>0;
      }).length;
      const pct = arr.length ? Math.round(contactable*100/arr.length) : 0;
      $('#k_contactable').textContent = pct + '%';
    }catch(e){ show(body, '<tr><td colspan="8" class="muted">'+esc(String(e))+'</td></tr>'); }
  }
  $('#btnLeads').addEventListener('click', loadLeads);

  // Init
  $('#year').textContent = new Date().getFullYear();
  loadJobs();
})();
</script>
</body>
</html>
    """


@api.get("/health")
def health():
    return {"ok": True, "leads": len(LEADS), "jobs": len(JOBS), "max_workers": MAX_WORKERS, "per_host_rps": PER_HOST_RPS}

# ---- scan ----
@api.post("/v1/jobs/scan", response_model=JobAccepted)
def start_scan(req: ScanRequest):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    job_id = f"scan_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    job = JobStatus(job_id=job_id, status="running", started_at=datetime.now(timezone.utc))
    JOBS[job_id] = job

    # expand countries
    countries: List[str] = []
    for c in req.countries:
        cu = c.strip().upper()
        if cu in REGION_EXPANSIONS: countries.extend(REGION_EXPANSIONS[cu])
        else: countries.append(cu)
    countries = sorted(set(countries))

    # expand sources
    if any(s.upper() == "ALL" for s in req.sources):
        sources: List[SourceName] = list(ADAPTERS.keys())  # type: ignore
    else:
        sources = [s for s in req.sources if s in ADAPTERS]  # type: ignore

    def scan_task(src: SourceName, country: str):
        adapter = ADAPTERS.get(src)
        if adapter is None: return src, country, [], "no_adapter"
        last_err = None
        for i in range(MAX_RETRIES):
            try:
                rows = adapter.scan(country, req.since_months, req.max_per_source)
                return src, country, rows, None
            except Exception as e:
                last_err = str(e); time.sleep((BACKOFF_BASE ** i) + random.random())
        return src, country, [], last_err or "error"

    found = 0; errors = 0
    progress: Dict[str, float] = {s: 0.0 for s in sources}
    tasks = []
    max_workers = min(MAX_WORKERS, len(sources) * max(1, len(countries)))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for s in sources:
            for c in countries:
                tasks.append(ex.submit(scan_task, s, c))
        done_per_source = {s: 0 for s in sources}
        total_per_source = {s: len(countries) for s in sources}
        for fut in as_completed(tasks):
            src, country, rows, err = fut.result()
            if err: errors += 1
            else:
                for rc in rows:
                    upsert_lead_from_raw(rc)
                    found += 1
            done_per_source[src] += 1  # type: ignore
            progress[src] = min(1.0, done_per_source[src] / total_per_source[src])  # type: ignore

    job.status = "scanned"; job.found = found; job.errors = errors; job.progress = progress; job.updated_at = datetime.now(timezone.utc)
    log.info(f"[SCAN] sources={sources} countries={len(countries)} found={found} errors={errors}")
    return JobAccepted(job_id=job_id, status=job.status)

# ---- enrich (light) ----
@api.post("/v1/enrich")
def enrich(req: EnrichRequest):
    def fetch(url: str) -> str:
        try: return HTTP.get(url)
        except Exception: return ""
    for lead in LEADS.values():
        html = ""
        for src in lead.sources:
            if src.source_url:
                html = fetch(src.source_url)
                if html: break
        if not html and lead.website:
            html = fetch(str(lead.website))
        if not html: continue

        soup = soup_parse(html); text = soup.get_text(" ", strip=True)

        # follow one contact link if present
        if not lead.contact_url:
            a = None
            for link in soup.select("a[href]"):
                href = (link.get("href") or "")
                label = link.get_text(" ", strip=True)
                if REGEX["contact_link"].search(href) or REGEX["contact_link"].search(label):
                    a = href; break
            if a:
                base = lead.sources[0].source_url or str(lead.website) if lead.sources else str(lead.website)
                contact_url = urljoin(base, a)
                lead.contact_url = contact_url
                contact_html = fetch(contact_url)
                if contact_html and not lead.contact_email:
                    m = REGEX["email"].search(contact_html) or REGEX["email_obf"].search(contact_html)
                    if m:
                        try:
                            lead.contact_email = (f"{m.group(1)}@{m.group(2)}.{m.group(3)}" if m.re == REGEX["email_obf"] else m.group(0))  # type: ignore
                        except Exception:
                            pass

        # stacks
        for t in detect_stacks_extended(text):
            if t not in lead.stack_tags: lead.stack_tags.append(t)

        # discover website
        if not lead.website:
            a0 = soup.select_one("a[href^='http']")
            if a0 and a0.get("href"):
                href = a0.get("href")
                if all(k not in href for k in ["ethercat.org","universal-robots.com","siemens.com","partnerfinder.siemens.com","beckhoff.com","profibus.com"]):
                    try: lead.website = href  # type: ignore
                    except Exception: pass

        # fallback email
        if not getattr(lead, "contact_email", None):
            m = REGEX["email"].search(html) or REGEX["email_obf"].search(html)
            if m:
                try:
                    lead.contact_email = (f"{m.group(1)}@{m.group(2)}.{m.group(3)}" if m.re == REGEX["email_obf"] else m.group(0))  # type: ignore
                except Exception:
                    pass

        lead.last_seen = datetime.now(timezone.utc)

    job = JOBS.get(req.job_id)
    if job:
        job.status = "enriched"; job.updated_at = datetime.now(timezone.utc)
    log.info("[ENRICH] done")
    return JobAccepted(job_id=req.job_id, status="enriched")

# ---- enrich (deep) ----
@api.post("/v1/enrich/deep")
def enrich_deep(req: DeepEnrichRequest):
    def fetch(url: str) -> str:
        try: return HTTP.get(url)
        except Exception: return ""

    targets = [l for l in LEADS.values() if l.priority_class in set(req.priorities)]
    targets.sort(key=lambda x: (-x.score, x.country, x.company_name))
    targets = targets[:req.max_leads]
    visited_global: set[str] = set()

    for lead in targets:
        base_urls = []
        for s in lead.sources:
            if s.source_url: base_urls.append(s.source_url)
        if lead.website: base_urls.append(str(lead.website))
        base_urls = [u for u in base_urls if u]

        if not base_urls: continue
        if lead.context is None: lead.context = CompanyContext()

        pages_to_visit: List[str] = []
        visited_local: set[str] = set()

        for u in base_urls:
            if u not in visited_global:
                pages_to_visit.append(u)

        scored: List[Tuple[float,str]] = []
        for u in list(pages_to_visit):
            html = fetch(u)
            if not html: continue
            soup = soup_parse(html)
            scored.extend(candidate_links(u, soup))

            text = soup.get_text(" ", strip=True)
            for t in detect_stacks_extended(text):
                if t not in lead.stack_tags: lead.stack_tags.append(t)

            orgs, _ = extract_jsonld(soup)
            for org in orgs:
                size = org.get("numberOfEmployees") or org.get("employee") or org.get("employees")
                if size and not lead.context.size_hint:
                    lead.context.size_hint = str(size)
                same_as = org.get("sameAs") or []
                if isinstance(same_as, str): same_as = [same_as]
                for s_url in same_as:
                    if "linkedin.com" in s_url and not any(c.linkedin == s_url for c in lead.contacts):
                        lead.contacts.append(Contact(linkedin=s_url, page_url=u))

            for lg in detect_languages(soup):
                if lg not in lead.context.languages: lead.context.languages.append(lg)
            partners, sectors = detect_partners_and_sectors(soup)
            for p in partners:
                if p not in lead.context.partners: lead.context.partners.append(p)
            for s in sectors:
                if s not in lead.context.sectors: lead.context.sectors.append(s)

            for c in extract_contacts_from_soup(u, soup):
                if any((c.email and x.email==c.email) or (c.linkedin and x.linkedin==c.linkedin) or (c.name and x.name==c.name) for x in lead.contacts):
                    continue
                lead.contacts.append(c)

        scored.sort(key=lambda x: x[0], reverse=True)
        for _, url in scored:
            if len(visited_local) >= req.max_pages_per_lead: break
            if req.same_domain_only and lead.website:
                try:
                    if urlparse(url).netloc and urlparse(url).netloc != urlparse(str(lead.website)).netloc:
                        continue
                except Exception:
                    pass
            if url in visited_local or url in visited_global:
                continue
            visited_local.add(url); visited_global.add(url)

            html = fetch(url)
            if not html: continue
            soup = soup_parse(html); text = soup.get_text(" ", strip=True)

            for t in detect_stacks_extended(text):
                if t not in lead.stack_tags: lead.stack_tags.append(t)

            for c in extract_contacts_from_soup(url, soup):
                if any((c.email and x.email==c.email) or (c.linkedin and x.linkedin==c.linkedin) or (c.name and x.name==c.name) for x in lead.contacts):
                    continue
                lead.contacts.append(c)

            if not lead.phone:
                m = REGEX["phones"].search(text)
                if m:
                    try: lead.phone = m.group(0)
                    except Exception: pass

            for lg in detect_languages(soup):
                if lg not in lead.context.languages: lead.context.languages.append(lg)
            partners, sectors = detect_partners_and_sectors(soup)
            for p in partners:
                if p not in lead.context.partners: lead.context.partners.append(p)
            for s in sectors:
                if s not in lead.context.sectors: lead.context.sectors.append(s)
            for h in soup.select("h1, h2, h3"):
                title = h.get_text(" ", strip=True)
                if any(k in title.lower() for k in ["case","project","application","success","reference"]):
                    if len(lead.context.recent_projects) < 20 and title not in lead.context.recent_projects:
                        lead.context.recent_projects.append(title)

        lead.contacts = lead.contacts[:50]
        if lead.context:
            lead.context.partners = lead.context.partners[:25]
            lead.context.sectors = lead.context.sectors[:25]
            lead.context.recent_projects = lead.context.recent_projects[:25]
            lead.context.technologies = sorted(set(lead.context.technologies + [t for t in lead.stack_tags if t]))
        lead.last_seen = datetime.now(timezone.utc)

    job = JOBS.get(req.job_id)
    if job:
        job.status = "enriched"; job.updated_at = datetime.now(timezone.utc)
        job.message = (job.message or "") + f" deep_enrich targets={len(targets)}"
    log.info(f"[ENRICH_DEEP] targets={len(targets)}")
    return JobAccepted(job_id=req.job_id, status="enriched")

# ---- score ----
@api.post("/v1/score")
def score(req: ScoreRequest):
    for l in LEADS.values():
        sig = max([s.strength for s in l.sources], default=0)
        sig_pts = int(sig * WEIGHTS["signal"])
        st_pts = stack_points(l)
        raw = max(0, min(100, sig_pts + st_pts))
        l.score = raw
        l.priority_class = "HOT" if raw >= 70 else ("WARM" if raw >= 45 else "COLD")
        reasons = []
        if any(t in l.stack_tags for t in ["EtherCAT","PROFINET","EtherNet/IP"]): reasons.append("Fieldbus match")
        if "ROS2" in l.stack_tags: reasons.append("ROS2 present")
        if "TwinCAT" in l.stack_tags: reasons.append("TwinCAT")
        if "TIA" in l.stack_tags: reasons.append("TIA Portal")
        if "Studio5000" in l.stack_tags: reasons.append("Studio 5000")
        if any(s.name == "ETG" for s in l.sources): reasons.append("Listed on ETG")
        if any(s.name == "SIEMENS" for s in l.sources): reasons.append("Siemens partner")
        if any(s.name == "UR" for s in l.sources): reasons.append("UR ecosystem")
        if any(s.name == "BECKHOFF" for s in l.sources): reasons.append("Beckhoff ecosystem")
        if any(s.name == "PI_PROFINET" for s in l.sources): reasons.append("PI/PROFINET ecosystem")
        l.reason = "; ".join(reasons) if reasons else "Relevance match"
        l.pitch = f"Abbiamo integrazioni MAC con EtherCAT/PROFINET/EtherNet-IP, ROS2, UR e PLC (TwinCAT/TIA/Studio5000). POC rapido sul vostro stack ({', '.join(l.stack_tags)})."

    job = JOBS.get(req.job_id)
    if job:
        job.status = "scored"; job.updated_at = datetime.now(timezone.utc)
    log.info("[SCORE] done")
    return JobAccepted(job_id=req.job_id, status="scored")

# ---- leads ----
@api.get("/v1/leads")
def leads(priority: Optional[Priority] = Query(None), country: Optional[str] = Query(None),
          limit: int = 200, offset: int = 0) -> LeadPage:
    rows = list(LEADS.values())
    if priority: rows = [r for r in rows if r.priority_class == priority]
    if country: rows = [r for r in rows if r.country.upper() == country.upper()]
    total = len(rows)
    rows.sort(key=lambda r: ({"HOT":0,"WARM":1,"COLD":2}.get(r.priority_class or "COLD",3), -r.score, r.country, r.company_name))
    return LeadPage(items=rows[offset:offset+limit], total=total)

# ---- export ----
@api.post("/v1/export")
def export_leads(req: ExportRequest):
    from pathlib import Path
    import csv

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    outdir = Path("exports") / ts
    outdir.mkdir(parents=True, exist_ok=True)

    rows = list(LEADS.values())
    f = req.filters or {}
    if "priority" in f: rows = [r for r in rows if r.priority_class == f["priority"]]
    if "countries" in f:
        cc = set(c.upper() for c in f["countries"]); rows = [r for r in rows if r.country.upper() in cc]
    if "segments" in f:
        ss = set(f["segments"]); rows = [r for r in rows if r.segment in ss]
    if "stacks" in f:
        st = set(f["stacks"]); rows = [r for r in rows if any(t in st for t in r.stack_tags)]
    if "min_score" in f: rows = [r for r in rows if r.score >= int(f["min_score"])]

    def _prio(r: Lead):
        order = {"HOT":0,"WARM":1,"COLD":2}
        return order.get(r.priority_class or "COLD",3), -r.score, r.country, (r.segment or "ZZ")
    rows.sort(key=_prio)

    files = []

    if "csv" in req.format:
        csv_path = outdir / "lead_report_en.csv"
        header = [
            "company_name","country","website","segment","stack_tags","signal_sources","signal_strength",
            "partners","languages","size_hint","contacts","score","priority_class","reason","pitch","last_seen","source_url"
        ]
        with csv_path.open("w", newline="", encoding="utf-8") as fcsv:
            w = csv.writer(fcsv); w.writerow(header)
            for r in rows:
                src_names = "|".join([s.name for s in r.sources])
                src_strength = max([s.strength for s in r.sources], default=0)
                first_src_url = next((s.source_url for s in r.sources if s.source_url), "")
                partners = "|".join((r.context.partners if r.context else []))
                languages = "|".join((r.context.languages if r.context else []))
                size_hint = (r.context.size_hint if r.context else "") or ""
                contacts = "|".join(filter(None, [f"{c.name or ''}:{c.role or ''}:{c.email or ''}:{c.linkedin or ''}" for c in r.contacts]))[:500]
                w.writerow([
                    r.company_name, r.country, r.website or "", r.segment or "", "|".join(r.stack_tags),
                    src_names, f"{src_strength:.2f}",
                    partners, languages, size_hint, contacts,
                    r.score, r.priority_class or "", r.reason or "", r.pitch or "",
                    r.last_seen.isoformat(), first_src_url
                ])
        files.append({"type":"csv","path":str(csv_path)})

    if "md" in req.format:
        md_path = outdir / "lead_report.md"
        with md_path.open("w", encoding="utf-8") as fmd:
            fmd.write(f"# Lead Report — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n")
            total = len(rows); hot = sum(1 for r in rows if r.priority_class == "HOT")
            warm = sum(1 for r in rows if r.priority_class == "WARM")
            cold = sum(1 for r in rows if r.priority_class == "COLD")
            countries = ", ".join(sorted({r.country for r in rows}))
            fmd.write(f"**Summary**: {total} leads — HOT {hot} · WARM {warm} · COLD {cold}. Countries: {countries}.\n\n")
            def section(title: str, items: List[Lead]):
                fmd.write(f"\n## {title}\n\n")
                for r in items:
                    src_names = ", ".join(sorted({s.name for s in r.sources}))
                    src_strength = max([s.strength for s in r.sources], default=0)
                    fmd.write(f"### {r.company_name} ({r.country}) — {r.segment or ''}\n")
                    if r.website: fmd.write(f"- **Website:** {r.website}\n")
                    fmd.write(f"- **Stacks:** {', '.join(r.stack_tags)}\n")
                    fmd.write(f"- **Sources:** {src_names} — *strength {src_strength:.2f}*\n")
                    if r.contacts:
                        top = ", ".join(filter(None, [f"{c.name or ''} ({c.role or ''})" for c in r.contacts[:5]]))
                        fmd.write(f"- **Contacts:** {top}\n")
                    if r.context and (r.context.partners or r.context.sectors):
                        if r.context.partners: fmd.write(f"- **Partners:** {', '.join(r.context.partners[:8])}\n")
                        if r.context.sectors:  fmd.write(f"- **Sectors:** {', '.join(r.context.sectors[:8])}\n")
                    if r.reason: fmd.write(f"- **Reason:** {r.reason}\n")
                    if r.pitch: fmd.write(f"- **Pitch:** {r.pitch}\n")
                    fmd.write("\n")
            section("HOT",  [r for r in rows if r.priority_class == "HOT"])
            section("WARM", [r for r in rows if r.priority_class == "WARM"])
            section("COLD", [r for r in rows if r.priority_class == "COLD"])
        files.append({"type":"md","path":str(md_path)})

    export_id = f"exp_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    JOBS[export_id] = JobStatus(
        job_id=export_id, status="exported",
        started_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
        message=f"Exported {len(rows)} rows to {outdir}",
    )
    log.info(f"[EXPORT] {len(rows)} rows -> {outdir}")
    return {"export_id": export_id, "files": files}

# ============================== run =================================
if __name__ == "__main__":
    import uvicorn
    host = os.getenv("LEADR_HOST", "127.0.0.1")
    port = int(os.getenv("LEADR_PORT", "5050"))
    log.info(f"Starting API on http://{host}:{port}  (workers={MAX_WORKERS}, per_host_rps={PER_HOST_RPS})")
    log.info("Swagger at /docs")
    uvicorn.run(api, host=host, port=port)
