from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable

from .storage import AnalyticsStorage, parse_timestamp


ACTIVE_DOMAINS = {"light", "switch", "fan", "media_player", "vacuum", "climate", "lock", "cover"}
MEDIA_VIBE_ALIASES = {"lofi", "chill", "study", "relaxing"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def entity_domain(entity_id: str | None) -> str:
    if not entity_id or "." not in entity_id:
        return ""
    return entity_id.split(".", 1)[0]


def isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def is_active_state(domain: str, state: str | None) -> bool:
    if not state:
        return False
    if domain == "media_player":
        return state == "playing"
    if domain in {"light", "switch", "fan"}:
        return state == "on"
    if domain == "vacuum":
        return state in {"cleaning", "returning"}
    if domain == "climate":
        return state not in {"off", "unknown", "unavailable"}
    if domain == "lock":
        return state == "unlocked"
    if domain == "cover":
        return state in {"opening", "open"}
    return False


class AnalyticsBuilder:
    def __init__(self, storage: AnalyticsStorage) -> None:
        self.storage = storage

    def build_all(self) -> dict[str, Any]:
        self.storage.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        summary = self._compute_summary()
        self._write_catalog(summary)
        return summary

    def _iter_dataset_rows(self, dataset: str) -> Iterable[dict[str, Any]]:
        for path in sorted((self.storage.raw_root / dataset).glob("**/*.jsonl")):
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue

    def _compute_summary(self) -> dict[str, Any]:
        now = utc_now()
        today = now.date()
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)
        current_hour = now.hour

        latest_area_for_entity: dict[str, str] = {}
        latest_registry_ts: dict[str, datetime] = {}
        for row in self._iter_dataset_rows("entity_registry_snapshots"):
            entity_id = row.get("entity_id")
            if not entity_id:
                continue
            recorded_at = parse_timestamp(row.get("recorded_at"))
            previous = latest_registry_ts.get(entity_id)
            if previous is None or recorded_at >= previous:
                latest_registry_ts[entity_id] = recorded_at
                latest_area_for_entity[entity_id] = row.get("area_id") or "unassigned"

        events_by_entity: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in self._iter_dataset_rows("entity_state_events"):
            entity_id = row.get("entity_id")
            if entity_id:
                events_by_entity[entity_id].append(row)

        room_seconds_today: dict[str, float] = defaultdict(float)
        max_duration_by_entity: dict[str, float] = defaultdict(float)
        media_habits: dict[tuple[str, int, int], list[float]] = defaultdict(list)

        for entity_id, rows in events_by_entity.items():
            rows.sort(key=lambda item: parse_timestamp(item.get("recorded_at")))
            domain = entity_domain(entity_id)
            if domain not in ACTIVE_DOMAINS:
                continue
            area_id = latest_area_for_entity.get(entity_id, "unassigned")
            for index, row in enumerate(rows):
                started_at = parse_timestamp(row.get("recorded_at"))
                ended_at = parse_timestamp(rows[index + 1].get("recorded_at")) if index + 1 < len(rows) else now
                duration_seconds = max(0.0, (ended_at - started_at).total_seconds())
                state = row.get("new_state")
                if not is_active_state(domain, state):
                    continue
                if started_at >= datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc):
                    room_seconds_today[area_id] += duration_seconds
                if started_at >= week_ago:
                    max_duration_by_entity[entity_id] = max(max_duration_by_entity[entity_id], duration_seconds)
                if domain == "media_player":
                    media_habits[(entity_id, started_at.hour, started_at.weekday())].append(duration_seconds)

        top_rooms = sorted(room_seconds_today.items(), key=lambda item: item[1], reverse=True)[:3]
        top_active_rooms_today = (
            " | ".join(f"{area}: {round(seconds / 60.0, 1)} min" for area, seconds in top_rooms)
            if top_rooms
            else "No data yet"
        )

        long_runs = sorted(
            ((entity_id, seconds) for entity_id, seconds in max_duration_by_entity.items() if seconds > 4 * 3600),
            key=lambda item: item[1],
            reverse=True,
        )[:3]
        anomaly_feed = (
            " | ".join(f"{entity_id}: {round(seconds / 3600.0, 1)} h" for entity_id, seconds in long_runs)
            if long_runs
            else "No unusual long runtimes"
        )

        likely_media = []
        for (entity_id, hour_of_day, _weekday), durations in media_habits.items():
            if abs(hour_of_day - current_hour) <= 1 or abs(hour_of_day - current_hour) >= 23:
                likely_media.append((entity_id, len(durations), sum(durations) / max(1, len(durations))))
        likely_media.sort(key=lambda item: (item[1], item[2]), reverse=True)
        recommendation_feed = (
            f"Likely media zone now: {likely_media[0][0]}" if likely_media else "No recommendation yet"
        )

        air_points = [
            {
                "source_time": parse_timestamp(row.get("source_time")),
                "value": row.get("value"),
            }
            for row in self._iter_dataset_rows("screenlogic_metric_points")
            if row.get("metric") == "airTemps" and row.get("value") is not None
        ]
        runs = []
        for row in self._iter_dataset_rows("screenlogic_runs"):
            run_type = row.get("run_type")
            if run_type not in {"poolRuns", "spaRuns"}:
                continue
            started_at = parse_timestamp(row.get("started_at"))
            if started_at < month_ago:
                continue
            ended_at = parse_timestamp(row.get("ended_at"))
            air_values = [
                float(point["value"])
                for point in air_points
                if started_at <= point["source_time"] <= ended_at
            ]
            runs.append(
                {
                    "run_type": run_type,
                    "duration_minutes": float(row.get("duration_minutes") or 0.0),
                    "avg_air_temp": sum(air_values) / len(air_values) if air_values else None,
                }
            )

        run_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for run in runs:
            run_groups[run["run_type"]].append(run)
        recent_pool_spa_efficiency_parts = []
        for run_type in ("poolRuns", "spaRuns"):
            group = run_groups.get(run_type, [])
            if not group:
                continue
            avg_minutes = sum(item["duration_minutes"] for item in group) / len(group)
            air_samples = [item["avg_air_temp"] for item in group if item["avg_air_temp"] is not None]
            avg_air = sum(air_samples) / len(air_samples) if air_samples else None
            label = "pool" if run_type == "poolRuns" else "spa"
            if avg_air is None:
                recent_pool_spa_efficiency_parts.append(f"{label}: {round(avg_minutes, 1)} min")
            else:
                recent_pool_spa_efficiency_parts.append(
                    f"{label}: {round(avg_minutes, 1)} min @ {round(avg_air, 1)}F"
                )
        recent_pool_spa_efficiency = (
            " | ".join(recent_pool_spa_efficiency_parts) if recent_pool_spa_efficiency_parts else "No data yet"
        )

        return {
            "built_at": now.isoformat(),
            "top_active_rooms_today": top_active_rooms_today,
            "recent_pool_spa_efficiency": recent_pool_spa_efficiency,
            "anomaly_feed": anomaly_feed,
            "recommendation_feed": recommendation_feed,
        }

    def _write_catalog(self, summary: dict[str, Any]) -> None:
        con = sqlite3.connect(self.storage.catalog_path)
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics_summary (
                  built_at TEXT PRIMARY KEY,
                  top_active_rooms_today TEXT NOT NULL,
                  recent_pool_spa_efficiency TEXT NOT NULL,
                  anomaly_feed TEXT NOT NULL,
                  recommendation_feed TEXT NOT NULL
                )
                """
            )
            con.execute("DELETE FROM analytics_summary")
            con.execute(
                """
                INSERT INTO analytics_summary (
                  built_at,
                  top_active_rooms_today,
                  recent_pool_spa_efficiency,
                  anomaly_feed,
                  recommendation_feed
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    summary["built_at"],
                    summary["top_active_rooms_today"],
                    summary["recent_pool_spa_efficiency"],
                    summary["anomaly_feed"],
                    summary["recommendation_feed"],
                ),
            )
            con.commit()
        finally:
            con.close()
