from typing import Dict, List, Optional, Literal, Any, TypedDict
from datetime import datetime, timezone
from pydantic import BaseModel, Field, HttpUrl

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
    params: dict | None = None

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
    format: List[Literal["csv","md","jsonl"]] = Field(default_factory=lambda: ["csv","md","jsonl"])
    filters: Optional[Dict[str, Any]] = None
