# 📡 Lead-Radar

![Status](https://img.shields.io/badge/status-WIP-orange)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?logo=fastapi)

Lead-Radar is a FastAPI backend for scanning, enriching, and scoring European industrial companies, with data export and a web interface.

> Stato/Status: Work in progress (WIP).   
> This project is evolving and may change frequently.

---

## Table of Contents
- Overview
- Features
- Architecture
- Quickstart
- API at a glance
- Exports
- Configuration
- Status and Roadmap
- Contributing

## Overview
Lead-Radar scans industrial sources and vendors (e.g., EtherCAT, Siemens, UR, Beckhoff), enriches company data, and computes lead scores for prioritization. Data can be exported in multiple formats and consumed via REST API or a web UI.

## Features
- Multi-source industrial scan (EtherCAT, Siemens, UR, Beckhoff, …)
- Automatic company data enrichment
- Lead scoring and classification
- REST API and basic web interface
- Export to CSV, JSONL, Markdown

## Architecture
```
Industrial Sources -> Scanner -> Normalizer -> Enrichment -> Scoring -> Storage
Storage -> Export (CSV | JSONL | Markdown)
Storage -> REST API -> Web UI
```

## Quickstart

### Prerequisites
- Python 3.10+
- pip

### Installation
```sh
git clone https://github.com/Daniele-Cangi/Lead-Radar.git
cd Lead-Radar
pip install -r requirements.txt
```

### Run
- Using the provided script:
```sh
python lead_radar_api.py
```

- Alternatively with Uvicorn (if your app object is exposed):
```sh
uvicorn lead_radar_api:app --reload --host 0.0.0.0 --port 8000
```

Once running, the API should be available at:
- http://localhost:8000
- Interactive docs (OpenAPI): http://localhost:8000/docs

## API at a glance

Main endpoints:
- POST /v1/jobs/scan — start a scan job
- POST /v1/enrich — enrich existing leads
- POST /v1/score — compute lead scores
- GET  /v1/leads — list current leads
- POST /v1/export — export data

Example: start a scan
```sh
curl -X POST http://localhost:8000/v1/jobs/scan \
  -H "Content-Type: application/json" \
  -d '{
    "sources": ["ethercat", "siemens", "ur", "beckhoff"],
    "filters": { "region": "EU" }
  }'
```

Example: list leads
```sh
curl http://localhost:8000/v1/leads
```

## Exports
- Formats: CSV, JSONL, Markdown
- Output directory: exports/

Example request:
```sh
curl -X POST http://localhost:8000/v1/export \
  -H "Content-Type: application/json" \
  -d '{ "format": "csv", "path": "exports/leads.csv" }'
```

## Configuration
- Main config file: lead_radar_config.py
- Customize sources, filters, enrichment and scoring parameters.

## Status and Roadmap
- WIP: active development
- Short-term:
  - [ ] Improve source coverage and scanning heuristics
  - [ ] Harden enrichment pipelines
  - [ ] Add pagination and filtering to /v1/leads
  - [ ] Expand export options and schemas
  - [ ] Add more examples to docs
- Mid-term:
  - [ ] Authentication/Authorization
  - [ ] CI pipeline and test coverage
  - [ ] Containerization and deployment guides

## Contributing
Contributions are welcome! Please open an issue or a pull request. For substantial changes, discuss them first in an issue to align on direction.
