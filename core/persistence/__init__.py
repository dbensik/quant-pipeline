"""
core.persistence

Storage helpers shared by the pipeline, the services layer and the dashboard.

These lived in `dashboard_app/` until 2026-08-09, which inverted the layering:
`data_pipeline/pipeline_orchestrator.py` and
`services/graphql_gateway/schema.py` both imported from the Streamlit package,
so the backend could not run — or be tested — without the UI, and the UI could
not be removed at all.

Neither module contains any Streamlit code; they only happened to live there.

Phase 5 — decommissioning Streamlit
"""

from core.persistence.results_manager import ResultsManager

# DatabaseManager was removed on 2026-08-09 with the SQLite pipeline it served.
# Its only consumer was PipelineOrchestrator, which wrote a database nothing
# read. Ingestion goes through core/ingest.py into TimescaleDB.
__all__ = ["ResultsManager"]
