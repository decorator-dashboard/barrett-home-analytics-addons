from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import threading
import time
import traceback
from typing import Any

from .builder import AnalyticsBuilder
from .config import AnalyticsConfig
from .ha_client import HAClient
from .screenlogic import import_screenlogic
from .storage import AnalyticsStorage


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_float(value: str | None) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def domain_of(entity_id: str | None) -> str:
    if not entity_id or "." not in entity_id:
        return ""
    return entity_id.split(".", 1)[0]


@dataclass
class ServiceState:
    raw_events_today: int = 0
    last_ingest_time: str | None = None
    last_compaction: str | None = None
    last_feature_build: str | None = None
    last_screenlogic_import: str | None = None
    last_error: str | None = None
    health: str = "starting"
    queue_oldest_ts: float | None = None
    summary: dict[str, Any] = field(default_factory=dict)


class AnalyticsService:
    def __init__(self) -> None:
        self.config = AnalyticsConfig.load()
        self.config.ensure_paths()
        self.storage = AnalyticsStorage(self.config)
        self.ha = HAClient(self.config.ha_base_url, self.config.ha_ws_url, self.config.supervisor_token)
        self.builder = AnalyticsBuilder(self.storage)
        self.state = ServiceState()
        self.buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.buffer_lock = threading.Lock()
        self.last_snapshot_at = 0.0
        self.last_registry_at = 0.0
        self.last_compaction_at = 0.0
        self.last_feature_build_at = 0.0
        self.last_status_publish_at = 0.0
        self.last_screenlogic_at = 0.0

    def run(self) -> None:
        status = self.storage.prepare()
        if status.healthy:
            self.state.health = "healthy"
        else:
            self.state.health = "degraded"
            self.state.last_error = status.reason
        self.bootstrap_snapshots()
        event_thread = threading.Thread(target=self._event_loop, name="ha-event-stream", daemon=True)
        event_thread.start()
        while True:
            self._maintenance_tick()
            time.sleep(1)

    def bootstrap_snapshots(self) -> None:
        self.capture_numeric_snapshots()
        self.capture_registries()
        self._maintenance_tick(force=True)

    def handle_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("event_type", "")
        time_fired = event.get("time_fired") or utc_now_iso()
        context = event.get("context") or {}
        data = event.get("data") or {}

        if event_type == "state_changed":
            entity_id = data.get("entity_id")
            domain = domain_of(entity_id)
            if domain in self.config.include_domains:
                old_state = data.get("old_state") or {}
                new_state = data.get("new_state") or {}
                self._append(
                    "entity_state_events",
                    {
                        "recorded_at": time_fired,
                        "event_type": event_type,
                        "entity_id": entity_id,
                        "domain": domain,
                        "old_state": old_state.get("state"),
                        "new_state": new_state.get("state"),
                        "old_attributes_json": json.dumps(old_state.get("attributes", {}), separators=(",", ":")),
                        "new_attributes_json": json.dumps(new_state.get("attributes", {}), separators=(",", ":")),
                        "context_id": context.get("id"),
                        "user_id": context.get("user_id"),
                        "parent_id": context.get("parent_id"),
                        "source": "ha_event_stream",
                    },
                )
                numeric_value = parse_float((new_state or {}).get("state"))
                if numeric_value is not None:
                    self._append(
                        "numeric_metric_samples",
                        {
                            "recorded_at": time_fired,
                            "entity_id": entity_id,
                            "domain": domain,
                            "value": numeric_value,
                            "unit": (new_state.get("attributes") or {}).get("unit_of_measurement"),
                            "attributes_json": json.dumps(new_state.get("attributes", {}), separators=(",", ":")),
                            "source": "ha_event_stream",
                        },
                    )
                if domain in {"person", "device_tracker"}:
                    attributes = new_state.get("attributes") or {}
                    self._append(
                        "presence_events",
                        {
                            "recorded_at": time_fired,
                            "entity_id": entity_id,
                            "old_state": old_state.get("state"),
                            "new_state": new_state.get("state"),
                            "latitude": attributes.get("latitude"),
                            "longitude": attributes.get("longitude"),
                            "source": "ha_event_stream",
                        },
                    )
                if domain == "alert" or "alert" in str(entity_id):
                    self._append(
                        "alert_events",
                        {
                            "recorded_at": time_fired,
                            "entity_id": entity_id,
                            "state": new_state.get("state"),
                            "severity": (new_state.get("attributes") or {}).get("severity", ""),
                            "title": (new_state.get("attributes") or {}).get("friendly_name", entity_id),
                            "message": (new_state.get("attributes") or {}).get("message", ""),
                            "source": "ha_event_stream",
                        },
                    )
        elif event_type == "call_service":
            domain = str(data.get("domain", ""))
            service = str(data.get("service", ""))
            service_data = data.get("service_data") or {}
            self._append(
                "service_calls",
                {
                    "recorded_at": time_fired,
                    "domain": domain,
                    "service": service,
                    "service_data_json": json.dumps(service_data, separators=(",", ":")),
                    "context_id": context.get("id"),
                    "user_id": context.get("user_id"),
                    "source": "ha_event_stream",
                },
            )
            if domain == "script":
                script_entity_id = service_data.get("entity_id") or service_data.get("target", {}).get("entity_id")
                self._append(
                    "script_runs",
                    {
                        "recorded_at": time_fired,
                        "script_entity_id": script_entity_id,
                        "service": service,
                        "service_data_json": json.dumps(service_data, separators=(",", ":")),
                        "context_id": context.get("id"),
                        "user_id": context.get("user_id"),
                        "source": "ha_event_stream",
                    },
                )
            if domain == "automation":
                self._append(
                    "automation_events",
                    {
                        "recorded_at": time_fired,
                        "automation_entity_id": service_data.get("entity_id"),
                        "event_type": service,
                        "details_json": json.dumps(service_data, separators=(",", ":")),
                        "context_id": context.get("id"),
                        "source": "ha_event_stream",
                    },
                )
        elif event_type in {"automation_triggered", "persistent_notifications_updated"}:
            self._append(
                "automation_events",
                {
                    "recorded_at": time_fired,
                    "automation_entity_id": data.get("entity_id"),
                    "event_type": event_type,
                    "details_json": json.dumps(data, separators=(",", ":")),
                    "context_id": context.get("id"),
                    "source": "ha_event_stream",
                },
            )
    def _append(self, dataset: str, payload: dict[str, Any]) -> None:
        with self.buffer_lock:
            self.buffers[dataset].append(payload)
            self.state.raw_events_today += 1
            self.state.last_ingest_time = payload.get("recorded_at", utc_now_iso())
            if self.state.queue_oldest_ts is None:
                self.state.queue_oldest_ts = time.time()

    def _flush_buffers(self) -> None:
        with self.buffer_lock:
            if not self.buffers:
                return
            for dataset, events in list(self.buffers.items()):
                if not events:
                    continue
                self.storage.append_events(dataset, events)
                self.buffers[dataset].clear()
            self.state.queue_oldest_ts = None

    def capture_numeric_snapshots(self) -> None:
        states = self.ha.get_states()
        now_iso = utc_now_iso()
        numeric_rows = []
        for state in states:
            entity_id = state.get("entity_id")
            domain = domain_of(entity_id)
            if domain not in self.config.include_domains:
                continue
            numeric_value = parse_float(state.get("state"))
            if numeric_value is None:
                continue
            numeric_rows.append(
                {
                    "recorded_at": now_iso,
                    "entity_id": entity_id,
                    "domain": domain,
                    "value": numeric_value,
                    "unit": (state.get("attributes") or {}).get("unit_of_measurement"),
                    "attributes_json": json.dumps(state.get("attributes", {}), separators=(",", ":")),
                    "source": "periodic_snapshot",
                }
            )
        self.storage.append_events("numeric_metric_samples", numeric_rows)
        self.last_snapshot_at = time.time()

    def capture_registries(self) -> None:
        now_iso = utc_now_iso()
        entities = [
            {
                "recorded_at": now_iso,
                "entity_id": item.get("entity_id"),
                "platform": item.get("platform"),
                "device_id": item.get("device_id"),
                "area_id": item.get("area_id"),
                "original_name": item.get("original_name") or item.get("name"),
                "entity_category": item.get("entity_category"),
                "disabled_by": item.get("disabled_by"),
                "hidden_by": item.get("hidden_by"),
                "labels_json": json.dumps(item.get("labels", []), separators=(",", ":")),
                "raw_json": json.dumps(item, separators=(",", ":")),
            }
            for item in self.ha.get_entity_registry()
        ]
        devices = [
            {
                "recorded_at": now_iso,
                "device_id": item.get("id"),
                "manufacturer": item.get("manufacturer"),
                "model": item.get("model"),
                "name_by_user": item.get("name_by_user") or item.get("name"),
                "area_id": item.get("area_id"),
                "via_device_id": item.get("via_device_id"),
                "configuration_url": item.get("configuration_url"),
                "hw_version": item.get("hw_version"),
                "sw_version": item.get("sw_version"),
                "raw_json": json.dumps(item, separators=(",", ":")),
            }
            for item in self.ha.get_device_registry()
        ]
        self.storage.append_events("entity_registry_snapshots", entities)
        self.storage.append_events("device_registry_snapshots", devices)
        self.last_registry_at = time.time()

    def _run_screenlogic_import(self) -> None:
        imported = import_screenlogic(self.config)
        if not imported:
            return
        metric_rows, run_rows = imported
        self.storage.append_events("screenlogic_metric_points", metric_rows)
        self.storage.append_events("screenlogic_runs", run_rows)
        self.state.last_screenlogic_import = utc_now_iso()
        self.last_screenlogic_at = time.time()

    def _maintenance_tick(self, force: bool = False) -> None:
        now = time.time()
        try:
            if self.buffers and (force or self.state.queue_oldest_ts is None or now - self.state.queue_oldest_ts >= 5):
                self._flush_buffers()
            if force or now - self.last_snapshot_at >= self.config.snapshot_interval_seconds:
                self.capture_numeric_snapshots()
            if force or now - self.last_registry_at >= 6 * 3600:
                self.capture_registries()
            if force or now - self.last_screenlogic_at >= 6 * 3600:
                self._run_screenlogic_import()
            if force or now - self.last_compaction_at >= self.config.compaction_interval_minutes * 60:
                self.storage.compact_queue()
                self.state.last_compaction = utc_now_iso()
                self.last_compaction_at = now
            if force or now - self.last_feature_build_at >= self.config.feature_build_interval_minutes * 60:
                self.state.summary = self.builder.build_all()
                self.state.last_feature_build = utc_now_iso()
                self.last_feature_build_at = now
            if force or now - self.last_status_publish_at >= 60:
                self.publish_status()
                self.last_status_publish_at = now
            if self.storage.free_gb() < self.config.ssd_min_free_gb:
                self.state.health = "degraded"
                self.state.last_error = (
                    f"analytics storage below threshold: {self.storage.free_gb():.1f} GB free"
                )
        except Exception as exc:  # noqa: BLE001
            self.state.health = "error"
            self.state.last_error = f"{exc}"
            self.publish_status()

    def _event_loop(self) -> None:
        while True:
            try:
                self.ha.subscribe_events(self.handle_event)
            except Exception as exc:  # noqa: BLE001
                self.state.last_error = f"event stream disconnected: {exc}"
                self.state.health = "degraded"
                time.sleep(5)

    def publish_status(self) -> None:
        queue_lag = 0
        if self.state.queue_oldest_ts is not None:
            queue_lag = max(0, round(time.time() - self.state.queue_oldest_ts))

        status_attributes = {
            "last_error": self.state.last_error,
            "analytics_root": str(self.storage.root),
            "catalog_path": str(self.storage.catalog_path),
        }
        self.ha.publish_state("sensor.analytics_health", self.state.health, status_attributes)
        self.ha.publish_state(
            "sensor.analytics_last_ingest_time",
            self.state.last_ingest_time or "never",
            {"device_class": "timestamp"} if self.state.last_ingest_time else {},
        )
        self.ha.publish_state("sensor.analytics_queue_lag_seconds", queue_lag, {"unit_of_measurement": "s"})
        self.ha.publish_state(
            "sensor.analytics_raw_events_today",
            self.state.raw_events_today,
            {"unit_of_measurement": "events"},
        )
        self.ha.publish_state(
            "sensor.analytics_last_compaction",
            self.state.last_compaction or "never",
            {"device_class": "timestamp"} if self.state.last_compaction else {},
        )
        self.ha.publish_state(
            "sensor.analytics_last_feature_build",
            self.state.last_feature_build or "never",
            {"device_class": "timestamp"} if self.state.last_feature_build else {},
        )
        self.ha.publish_state(
            "sensor.analytics_ssd_free_gb",
            round(self.storage.free_gb(), 2),
            {"unit_of_measurement": "GB"},
        )
        self.ha.publish_state(
            "sensor.analytics_top_active_rooms_today",
            self.state.summary.get("top_active_rooms_today") or "No data yet",
            {},
        )
        self.ha.publish_state(
            "sensor.analytics_recent_pool_spa_efficiency",
            self.state.summary.get("recent_pool_spa_efficiency") or "No data yet",
            {},
        )
        self.ha.publish_state(
            "sensor.analytics_anomaly_feed",
            self.state.summary.get("anomaly_feed") or "No anomalies detected",
            {},
        )
        self.ha.publish_state(
            "sensor.analytics_recommendation_feed",
            self.state.summary.get("recommendation_feed") or "No recommendation yet",
            {},
        )


def main() -> None:
    service = AnalyticsService()
    try:
        service.run()
    except Exception as exc:  # noqa: BLE001
        traceback.print_exc()
        try:
            service.state.health = "error"
            service.state.last_error = str(exc)
            service.publish_status()
        except Exception:  # noqa: BLE001
            pass
        raise
