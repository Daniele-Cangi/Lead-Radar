<<<<<<< HEAD


# LeadRadar API

LeadRadar is a FastAPI backend for scanning, enriching, and scoring European industrial companies, with data export and web interface.

## Main Features

- Scan industrial sources (EtherCAT, Siemens, UR, Beckhoff, etc.)
- Automatic enrichment of company data
- Lead scoring and classification
- Export to CSV, JSONL, Markdown
- REST API and web interface

## Installation

1. Clone the repository:

   ```sh
   git clone <repo-url>
   cd lead_radar
   ```

2. Install dependencies:

   ```sh
   pip install -r requirements.txt
   ```

3. Start the server:

   ```sh
   python lead_radar_api.py
   ```

## Main Dependencies

- fastapi
- uvicorn
- requests
- pydantic
- beautifulsoup4
- lxml

## Main API Endpoints

- `/v1/jobs/scan` — Start scan
- `/v1/enrich` — Enrich leads
- `/v1/score` — Calculate score
- `/v1/leads` — List leads
- `/v1/export` — Export data

## Notes

- Exported files are saved in `exports/`
- You can customize configuration in `lead_radar_config.py`
=======
# Lead-Radar
LeadRadar is a Python-based FastAPI backend for scanning, enriching, and scoring industrial automation leads. It supports multiple industrial protocols and vendors, provides RESTful endpoints for lead management, and includes adapters for sources like ETG, UR, Siemens, Beckhoff, PROFINET, ODVA, and ROS2. The project is modular.
>>>>>>> b7b41c026e6a461313fcece51a12d8a393d8d25f
