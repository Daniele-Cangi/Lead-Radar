import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from datetime import datetime, timezone
import uuid, time, random, json, hashlib, re
from typing import List, Dict, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse
import lead_radar_config as config
import lead_radar_models as models

# ============================== Global state ================================
JOBS: Dict[str, models.JobStatus] = {}
LEADS: Dict[str, models.Lead] = {}

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
	def __init__(self, timeout=config.DEFAULT_TIMEOUT, rps=config.PER_HOST_RPS, respect_robots=config.RESPECT_ROBOTS):
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
		return {"User-Agent": random.choice(config.UA_POOL)}
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
		tries = config.MAX_RETRIES
		backoff = config.BACKOFF_BASE
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

# ============================== Helpers ==============================
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

def upsert_lead_from_raw(rc: models.RawCompany) -> models.Lead:
	cid = company_id_from(rc.get("website"), rc["name"], rc["country"])
	lead = LEADS.get(cid)
	default_segment: Optional[models.Segment] = "OEM"
	base_stacks: List[models.StackTag] = []
	if rc["source"] == "ETG": base_stacks.append("EtherCAT")
	if rc["source"] == "SIEMENS": default_segment = "SI"; base_stacks.extend(["PROFINET","TIA"])
	if rc["source"] == "UR": default_segment = "SI"; base_stacks.append("UR")
	if rc["source"] == "BECKHOFF": base_stacks.extend(["EtherCAT","TwinCAT"])
	if rc["source"] == "PI_PROFINET": base_stacks.append("PROFINET")
	src_strength = 0.90 if rc["source"] in {"ETG","SIEMENS","BECKHOFF","PI_PROFINET"} else 0.85
	if not lead:
		lead = models.Lead(
			company_id=cid, company_name=rc["name"], country=rc["country"],
			website=rc.get("website"), segment=default_segment,
			stack_tags=base_stacks,
			sources=[models.SourceHit(name=rc["source"], strength=src_strength, source_url=rc.get("source_url"))],
		); LEADS[cid] = lead
	else:
		lead.sources.append(models.SourceHit(name=rc["source"], strength=src_strength, source_url=rc.get("source_url")))
		if lead.website is None and rc.get("website"): lead.website = rc.get("website")
		for t in base_stacks:
			if t not in lead.stack_tags: lead.stack_tags.append(t)
		if lead.segment is None: lead.segment = default_segment
	return lead

def stack_points(lead: models.Lead) -> int:
	return sum(config.WEIGHTS["stacks"].get(t, 0) for t in lead.stack_tags)

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
	for k,w in config.LINK_HINTS.items():
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

def extract_contacts_from_soup(base_url: str, soup) -> List[models.Contact]:
	contacts: List[models.Contact] = []
	_, persons = extract_jsonld(soup)
	for p in persons:
		contacts.append(models.Contact(
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
					m3 = config.REGEX["email"].search(t) or config.REGEX["email_obf"].search(t)
					if m3:
						try:
							email = f"{m3.group(1)}@{m3.group(2)}.{m3.group(3)}" if m3.re == config.REGEX["email_obf"] else m3.group(0)
						except Exception: pass
					a = it.select_one("a[href*='linkedin.com']")
					if a and a.get("href"): link = normalize_url(base_url, a.get("href"))
					if name or email or link:
						contacts.append(models.Contact(name=name, role=role, email=email, linkedin=link, page_url=base_url))
	for m in config.REGEX["email"].finditer(soup.get_text(" ", strip=True)):
		em = m.group(0)
		if not any(c.email == em for c in contacts):
			contacts.append(models.Contact(email=em, page_url=base_url))
	return contacts[:30]

def detect_languages(soup) -> List[str]:
	langs = set()
	html_tag = soup.find("html")
	if html_tag and html_tag.get("lang"):
		lg = html_tag.get("lang").split("-")[0].lower()
		if lg in config.LANG_HINTS: langs.add(config.LANG_HINTS[lg])
	text = soup.get_text(" ", strip=True).lower()
	for k,code in config.LANG_HINTS.items():
		if re.search(rf"\b{k}\b", text) and len(langs) < 6:
			langs.add(code)
	return sorted(langs)

def detect_partners_and_sectors(soup) -> Tuple[List[str], List[str]]:
	text = soup.get_text(" ", strip=True)
	partners = []
	for v in config.VENDOR_PARTNERS:
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
	if config.REGEX["ethercat"].search(text): tags.append("EtherCAT")
	if config.REGEX["profinet"].search(text): tags.append("PROFINET")
	if config.REGEX["ethernetip"].search(text): tags.append("EtherNet/IP")
	if config.REGEX["ros2"].search(text): tags.append("ROS2")
	if config.REGEX["twincat"].search(text): tags.append("TwinCAT")
	if config.REGEX["tia"].search(text): tags.append("TIA")
	if config.REGEX["studio5000"].search(text): tags.append("Studio5000")
	if config.REGEX["fieldbus_generic"].search(text):
		if "PROFINET" not in tags: tags.append("PROFINET")
		if "EtherNet/IP" not in tags: tags.append("EtherNet/IP")
	if config.REGEX["motion_plc"].search(text):
		for t in ["TwinCAT","TIA","Studio5000"]:
			if t not in tags: tags.append(t)
	return tags


# ============================== Adapters =============================
class ETGAdapter:
	SOURCE: models.SourceName = "ETG"
	BASE_URLS = [
		"https://www.ethercat.org/en/members/members.html",
		"https://www.ethercat.org/en/products/products.html",
	]
	def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[models.RawCompany]:
		results: List[models.RawCompany] = []; seen: set[Tuple[str,str]] = set(); urls = []
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
					results.append(models.RawCompany(name=name, country=country, website=site, source=self.SOURCE, source_url=src, meta={}))
					if len(results) >= max_items: return results
			except Exception as e:
				config.log.debug(f"ETG skip {url}: {e}")
		return results

class URAdapter:
	SOURCE: models.SourceName = "UR"
	def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[models.RawCompany]:
		from urllib.parse import urlencode
		urls = [
			f"https://www.universal-robots.com/find-a-distributor/?{urlencode({'country': country})}",
			f"https://www.universal-robots.com/ur-plus/all/?{urlencode({'country': country})}",
		]
		results: List[models.RawCompany] = []; seen: set[Tuple[str,str]] = set()
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
					results.append(models.RawCompany(name=name, country=country, website=site, source=self.SOURCE, source_url=src, meta={}))
					if len(results) >= max_items: return results
			except Exception as e:
				config.log.debug(f"UR skip {url}: {e}")
		return results

class SiemensAdapter:
	SOURCE: models.SourceName = "SIEMENS"
	def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[models.RawCompany]:
		url = f"https://partnerfinder.siemens.com/?country={country}"
		results: List[models.RawCompany] = []; seen: set[Tuple[str,str]] = set()
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
				results.append(models.RawCompany(name=name, country=country, website=site, source=self.SOURCE, source_url=src, meta={}))
				if len(results) >= max_items: return results
		except Exception as e:
			config.log.debug(f"SIEMENS skip {url}: {e}")
		return results

class BeckhoffAdapter:
	SOURCE: models.SourceName = "BECKHOFF"
	def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[models.RawCompany]:
		from urllib.parse import urlencode
		urls = [
			"https://www.beckhoff.com/en-en/company/partners/",
			f"https://www.beckhoff.com/en-en/company/partners/?{urlencode({'country': country})}",
			"https://www.beckhoff.com/en-en/contact/global-presence/",
		]
		results: List[models.RawCompany] = []; seen: set[Tuple[str,str]] = set()
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
					results.append(models.RawCompany(name=name, country=country, website=site, source=self.SOURCE, source_url=src, meta={}))
					if len(results) >= max_items: return results
			except Exception as e:
				config.log.debug(f"BECKHOFF skip {url}: {e}")
		return results

class PROFINETAdapter:
	SOURCE: models.SourceName = "PI_PROFINET"
	def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[models.RawCompany]:
		from urllib.parse import urlencode
		urls = [
			"https://www.profibus.com/community/members",
			f"https://www.profibus.com/community/members?{urlencode({'country': country})}",
			"https://www.profibus.com/technology/pi-competence-centers",
		]
		results: List[models.RawCompany] = []; seen: set[Tuple[str,str]] = set()
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
					results.append(models.RawCompany(name=name, country=country, website=site, source=self.SOURCE, source_url=src, meta={"tech":"PROFINET"}))
					if len(results) >= max_items: return results
			except Exception as e:
				config.log.debug(f"PI skip {url}: {e}")
		return results

class ODVAAdapter:
	SOURCE: models.SourceName = "ODVA_ENIP"
	def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[models.RawCompany]:
		return []

class ROS2Adapter:
	SOURCE: models.SourceName = "ROS2"
	def scan(self, country: str, since_months: int = 18, max_items: int = 2000) -> List[models.RawCompany]:
		return []

ADAPTERS: Dict[models.SourceName, Any] = {
	"ETG": ETGAdapter(),
	"UR": URAdapter(),
	"SIEMENS": SiemensAdapter(),
	"BECKHOFF": BeckhoffAdapter(),
	"PI_PROFINET": PROFINETAdapter(),
	"ODVA_ENIP": ODVAAdapter(),
	"ROS2": ROS2Adapter(),
}

# ============================== FastAPI =============================
api = FastAPI(title="EU Lead Radar API", version="1.3.0")

@api.get("/", include_in_schema=False)
def root():
	return RedirectResponse(url="/ui")

@api.get("/ui", response_class=HTMLResponse, include_in_schema=False)
def ui():
	return "LeadRadar UI"

@api.get("/health")
def health():
	return {"ok": True, "leads": len(LEADS), "jobs": len(JOBS), "max_workers": config.MAX_WORKERS, "per_host_rps": config.PER_HOST_RPS}

@api.post("/v1/jobs/scan", response_model=models.JobAccepted)
def start_scan(req: models.ScanRequest):
	from concurrent.futures import ThreadPoolExecutor, as_completed
	job_id = f"scan_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
	job = models.JobStatus(job_id=job_id, status="running", started_at=datetime.now(timezone.utc))
	JOBS[job_id] = job
	job.params = {"countries": req.countries, "sources": req.sources}

	# expand countries
	countries: List[str] = []
	for c in req.countries:
		cu = c.strip().upper()
		if cu in config.REGION_EXPANSIONS: countries.extend(config.REGION_EXPANSIONS[cu])
		else: countries.append(cu)
	countries = sorted(set(countries))

	# expand sources
	if any(s.upper() == "ALL" for s in req.sources):
		sources: List[models.SourceName] = list(ADAPTERS.keys())
	else:
		sources = [s for s in req.sources if s in ADAPTERS]

	def scan_task(src: models.SourceName, country: str):
		adapter = ADAPTERS.get(src)
		if adapter is None: return src, country, [], "no_adapter"
		last_err = None
		for i in range(config.MAX_RETRIES):
			try:
				rows = adapter.scan(country, req.since_months, req.max_per_source)
				return src, country, rows, None
			except Exception as e:
				last_err = str(e); time.sleep((config.BACKOFF_BASE ** i) + random.random())
		return src, country, [], last_err or "error"

	found = 0; errors = 0
	progress: Dict[str, float] = {s: 0.0 for s in sources}
	tasks = []
	max_workers = min(config.MAX_WORKERS, len(sources) * max(1, len(countries)))

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
			done_per_source[src] += 1
			progress[src] = min(1.0, done_per_source[src] / total_per_source[src])

	job.status = "scanned"; job.found = found; job.errors = errors; job.progress = progress; job.updated_at = datetime.now(timezone.utc)
	config.log.info(f"[SCAN] sources={sources} countries={len(countries)} found={found} errors={errors}")
	return models.JobAccepted(job_id=job_id, status=job.status)

@api.post("/v1/enrich")
def enrich(req: models.EnrichRequest):
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
				if config.REGEX["contact_link"].search(href) or config.REGEX["contact_link"].search(label):
					a = href; break
			if a:
				base = lead.sources[0].source_url or str(lead.website) if lead.sources else str(lead.website)
				contact_url = urljoin(base, a)
				lead.contact_url = contact_url
				contact_html = fetch(contact_url)
				if contact_html and not lead.contact_email:
					m = config.REGEX["email"].search(contact_html) or config.REGEX["email_obf"].search(contact_html)
					if m:
						try:
							lead.contact_email = (f"{m.group(1)}@{m.group(2)}.{m.group(3)}" if m.re == config.REGEX["email_obf"] else m.group(0))
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
					try: lead.website = href
					except Exception: pass

		# fallback email
		if not getattr(lead, "contact_email", None):
			m = config.REGEX["email"].search(html) or config.REGEX["email_obf"].search(html)
			if m:
				try:
					lead.contact_email = (f"{m.group(1)}@{m.group(2)}.{m.group(3)}" if m.re == config.REGEX["email_obf"] else m.group(0))
				except Exception:
					pass

		lead.last_seen = datetime.now(timezone.utc)

	job = JOBS.get(req.job_id)
	if job:
		job.status = "enriched"; job.updated_at = datetime.now(timezone.utc)
	config.log.info("[ENRICH] done")
	return models.JobAccepted(job_id=req.job_id, status="enriched")

@api.post("/v1/enrich/deep")
def enrich_deep(req: models.DeepEnrichRequest):
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
		if lead.context is None: lead.context = models.CompanyContext()

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
						lead.contacts.append(models.Contact(linkedin=s_url, page_url=u))

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
				m = config.REGEX["phones"].search(text)
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
	config.log.info(f"[ENRICH_DEEP] targets={len(targets)}")
	return models.JobAccepted(job_id=req.job_id, status="enriched")

@api.post("/v1/score")
def score(req: models.ScoreRequest):
	for l in LEADS.values():
		sig = max([s.strength for s in l.sources], default=0)
		sig_pts = int(sig * config.WEIGHTS["signal"])
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
	config.log.info("[SCORE] done")
	return models.JobAccepted(job_id=req.job_id, status="scored")

@api.get("/v1/leads")
def leads(priority: Optional[models.Priority] = Query(None), country: Optional[str] = Query(None), limit: int = 200, offset: int = 0) -> models.LeadPage:
	rows = list(LEADS.values())
	if priority: rows = [r for r in rows if r.priority_class == priority]
	if country: rows = [r for r in rows if r.country.upper() == country.upper()]
	total = len(rows)
	rows.sort(key=lambda r: ({"HOT":0,"WARM":1,"COLD":2}.get(r.priority_class or "COLD",3), -r.score, r.country, r.company_name))
	return models.LeadPage(items=rows[offset:offset+limit], total=total)

@api.post("/v1/export")
def export_leads(req: models.ExportRequest):
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

	def _prio(r: models.Lead):
		order = {"HOT":0,"WARM":1,"COLD":2}
		return order.get(r.priority_class or "COLD",3), -r.score, r.country, (r.segment or "ZZ")
	rows.sort(key=_prio)

	files = []

	if "jsonl" in req.format:
		jpath = outdir / "leads.jsonl"
		with jpath.open("w", encoding="utf-8") as fj:
			for r in rows:
				obj = {
					"company_name": r.company_name,
					"country": r.country,
					"website": str(r.website) if r.website else None,
					"segment": r.segment,
					"stack_tags": r.stack_tags,
					"score": r.score,
					"priority_class": r.priority_class,
					"reason": r.reason,
					"pitch": r.pitch,
					"last_seen": r.last_seen.isoformat() if r.last_seen else None,
					"contacts": [c.model_dump() for c in r.contacts],
					"sources": [s.model_dump() for s in r.sources],
				}
				fj.write(json.dumps(obj, ensure_ascii=False) + "\n")
		files.append(str(jpath))

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
			def section(title: str, items: List[models.Lead]):
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
	JOBS[export_id] = models.JobStatus(
		job_id=export_id, status="exported",
		started_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
		message=f"Exported {len(rows)} rows to {outdir}",
	)
	config.log.info(f"[EXPORT] {len(rows)} rows -> {outdir}")
	return {"export_id": export_id, "files": files}

from pydantic import BaseModel as _BM
class _JobItem(_BM):
	id: str
	type: str | None = None
	status: str | None = None
	created_at: str | None = None
	found: int | None = None
	errors: int | None = None
	progress: dict | None = None
	params: dict | None = None
class _JobList(_BM):
	items: list[_JobItem]
@api.get("/v1/jobs", response_model=_JobList)
def list_jobs():
	items: list[dict] = []
	for jid, st in JOBS.items():
		created = (st.started_at or st.updated_at)
		items.append({
			"id": jid,
			"type": ("scan" if jid.startswith("scan_") else
					 "export" if jid.startswith("export_") else
					 "score" if jid.startswith("score_") else "job"),
			"status": st.status,
			"created_at": created.isoformat() if created else None,
			"found": getattr(st, "found", None),
			"errors": getattr(st, "errors", None),
			"progress": getattr(st, "progress", None),
			"params": getattr(st, "params", None),
		})
	items.sort(key=lambda it: it.get("created_at") or "", reverse=True)
	return _JobList(items=[_JobItem(**it) for it in items])
@api.get("/v1/jobs/{job_id}", response_model=_JobItem)
def get_job(job_id: str):
	st = JOBS.get(job_id)
	if not st:
		return _JobItem(id=job_id, status="not_found")
	created = (st.started_at or st.updated_at)
	return _JobItem(
		id=job_id,
		type=("scan" if job_id.startswith("scan_") else
			  "export" if job_id.startswith("export_") else
			  "score" if job_id.startswith("score_") else "job"),
		status=st.status,
		created_at=created.isoformat() if created else None,
		found=getattr(st, "found", None),
		errors=getattr(st, "errors", None),
		progress=getattr(st, "progress", None),
		params=getattr(st, "params", None),
	)

if __name__ == "__main__":
	import uvicorn
	host = os.getenv("LEADR_HOST", "127.0.0.1")
	port = int(os.getenv("LEADR_PORT", "5050"))
	config.log.info(f"Starting API on http://{host}:{port}  (workers={config.MAX_WORKERS}, per_host_rps={config.PER_HOST_RPS})")
	config.log.info("Swagger at /docs")
	uvicorn.run(api, host=host, port=port)
