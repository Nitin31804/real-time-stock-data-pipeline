from contextlib import nullcontext
from datetime import datetime, timezone

import app as app_module
import pytest


@pytest.fixture
def client():
    app_module.app.config["TESTING"] = True
    return app_module.app.test_client()


def test_health_reports_active_services(client, monkeypatch):
    now = datetime.now(timezone.utc)

    def query_one(sql, params=None):
        if "SELECT 1 AS alive" in sql:
            return {"alive": 1}
        if "MAX(updated_at)" in sql:
            return {"last_write": now}
        if "MAX(trade_timestamp)" in sql:
            return {"last_update": now}
        raise AssertionError(f"Unexpected query: {sql}")

    monkeypatch.setattr(app_module, "query_one", query_one)
    monkeypatch.setattr(app_module.socket, "create_connection", lambda *a, **k: nullcontext())

    response = client.get("/api/health", headers={"X-Request-ID": "test-request"})

    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "test-request"
    assert response.headers["X-Data-Mode"] in {"simulation", "live"}
    payload = response.get_json()
    assert payload["status"] == "ok"
    assert {service["service"] for service in payload["services"]} == {
        "dashboard",
        "postgres",
        "kafka",
        "spark",
        "producer",
    }


def test_metrics_exposes_database_backed_pipeline_gauges(client, monkeypatch):
    responses = iter(
        [
            {"recent_events": 120, "latest_event_age": 1.25},
            {"latest_candle_age": 35.0},
        ]
    )
    monkeypatch.setattr(app_module, "query_one", lambda *args, **kwargs: next(responses))

    response = client.get("/metrics")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "stock_pipeline_events_per_second 2.0" in body
    assert "stock_pipeline_latest_event_age_seconds 1.25" in body
    assert "stock_pipeline_latest_candle_age_seconds 35.0" in body


def test_financials_fails_closed_instead_of_inventing_values(client, monkeypatch):
    monkeypatch.setattr(
        app_module,
        "get_cached_financials",
        lambda symbol: (_ for _ in ()).throw(RuntimeError("provider unavailable")),
    )

    response = client.get("/api/financials/AAPL")

    assert response.status_code == 503
    assert response.get_json()["data_source"] == "unavailable"
