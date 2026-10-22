"""gateway: public API. Checks the catalog with metadata, then forwards queries to query-engine."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException

from sim import features
from sim.app import Service
from sim.config import ServiceConfig
from sim.services.query_engine import QueryIn


class Gateway(Service):
    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config)
        self.metadata = self.dependency("metadata")
        self.query_engine = self.dependency("query-engine")

    def setup(self, app: FastAPI) -> None:
        app.get("/api/datasets")(self.list_datasets)
        app.get("/api/datasets/{name}")(self.get_dataset)
        app.post("/api/query")(self.query)

    def list_datasets(self) -> list[dict[str, Any]]:
        return self.metadata.request("GET", "/datasets").json()

    def get_dataset(self, name: str) -> dict[str, Any]:
        resp = self.metadata.request("GET", "/datasets/{name}", f"/datasets/{name}")
        if resp.status_code == 404:
            raise HTTPException(404, f"unknown dataset {name}")
        return resp.json()

    def query(self, q: QueryIn) -> dict[str, Any]:
        # Catalog check before spending query-engine capacity.
        self.get_dataset(q.dataset)
        resp = self.query_engine.request("POST", "/query", json=q.model_dump())
        if resp.status_code >= 400:
            raise HTTPException(resp.status_code, resp.json().get("detail", "query failed"))
        result = resp.json()
        if features.enabled("response_enrichment"):
            # Next release: surface scan statistics to clients. query-engine doesn't return
            # "stats", so this raises KeyError.
            result["rows_scanned"] = result["stats"]["rows_scanned"]
        return result
