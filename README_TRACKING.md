
# Tracking + Demo (English only)

## Start tracker (FastAPI)
uvicorn connector.tracker:app --port 8787 --reload

## Start demo (Streamlit)
streamlit run connector/demo_app.py --server.port 8866

Open: http://localhost:8787/t/<paste-any-uuid>
This will log an 'open' and redirect to the demo with ?token=<same-uuid>.
