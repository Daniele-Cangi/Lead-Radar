# Lead-Radar

![Status](https://img.shields.io/badge/status-prototype%20baseline-blue)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?logo=fastapi)

Lead-Radar is an **early local prototype** for discovering, enriching, and scoring European industrial-company leads from public vendor and ecosystem pages.

> **Project status — paused baseline.** This repository is kept as a documented starting point for a possible future restart. It is not a production service and it is not suitable for unattended or large-scale crawling.

## What is in the repository

- A FastAPI backend with a small in-memory lead and job store.
- Source adapters for EtherCAT (ETG), Universal Robots, Siemens, Beckhoff, and PI/PROFINET.
- Basic enrichment, scoring, and CSV/JSONL/Markdown export.
- A standalone React dashboard component (`lead_radar_pro_dashboard_react_single_file.jsx`), **not** a bundled or deployed web frontend.

The `ODVA_ENIP` and `ROS2` adapters are placeholders: selecting `ALL` includes them, but they currently return no leads.

## Current architecture

```text
Public vendor pages -> source adapters -> in-memory leads -> enrichment -> scoring -> export
                                      \-> FastAPI endpoints
```

There is no database, task queue, authentication, packaged frontend, or deployment configuration. Restarting the process clears all jobs and leads.

## Local quickstart

### Prerequisites

- Python 3.10+
- `pip`

### Install and run

```sh
git clone https://github.com/Daniele-Cangi/Lead-Radar.git
cd Lead-Radar
python -m pip install -r requirements.txt
python lead_radar_api.py
```

The server listens on `http://127.0.0.1:5050` by default. OpenAPI documentation is available at `http://127.0.0.1:5050/docs`.

Equivalent Uvicorn command:

```sh
uvicorn lead_radar_api:app --reload --host 127.0.0.1 --port 5050
```

## API: a small, honest example

Check that the process is running:

```sh
curl http://127.0.0.1:5050/health
```

Run a narrow scan first. `countries` accepts country codes or one of the named regions in `lead_radar_config.py`, such as `EU`, `DACH`, or `EU_EEA_PLUS`.

```sh
curl -X POST http://127.0.0.1:5050/v1/jobs/scan \
  -H "Content-Type: application/json" \
  -d '{
    "countries": ["IT"],
    "sources": ["ETG"],
    "max_per_source": 50,
    "since_months": 18
  }'
```

Supported source names are `ETG`, `UR`, `SIEMENS`, `BECKHOFF`, `PI_PROFINET`, `ODVA_ENIP`, `ROS2`, or `ALL`. The scan call is synchronous: it returns only when collection has finished. `max_per_source` is currently applied per source/country adapter call, not globally across a whole scan.

List the in-memory results:

```sh
curl "http://127.0.0.1:5050/v1/leads?limit=50"
```

Score them after scanning:

```sh
curl -X POST http://127.0.0.1:5050/v1/score \
  -H "Content-Type: application/json" \
  -d '{ "job_id": "scan_<returned-id>" }'
```

Export uses a list of formats and always writes below `exports/<UTC timestamp>/`:

```sh
curl -X POST http://127.0.0.1:5050/v1/export \
  -H "Content-Type: application/json" \
  -d '{ "format": ["csv", "jsonl", "md"] }'
```

## Known limitations

- The adapters use heuristic HTML selectors and need source-by-source maintenance as third-party sites change.
- Network, parser, and source failures need better reporting before any operational use.
- Jobs and leads are process-local and not safe as shared durable state.
- The dashboard component has dependencies on a separate React/shadcn application; `/ui` in the API is only a placeholder response.
- `robots.txt` is checked before requests, but that mechanism is not a substitute for confirming each source's terms, rate limits, and permitted use.
- Enrichment can collect business contact data. Before any real campaign or scale-up, define a lawful basis, retention policy, opt-out process, and a review of the relevant terms and GDPR obligations.

## Sensible restart path

If this project is resumed, start small and make one source reliable end-to-end:

1. Add fixtures and tests for one source adapter; make failures visible in the job result.
2. Persist leads, source evidence, and job metadata in a database.
3. Move scans to background workers with explicit limits, retries, and observability.
4. Validate source permissions and establish the data-governance model before collecting contacts.
5. Only then add sources, an integrated frontend, authentication, and deployment.

## License

Released under the [MIT License](LICENSE).

