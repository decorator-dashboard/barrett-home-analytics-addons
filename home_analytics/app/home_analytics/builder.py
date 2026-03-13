from __future__ import annotations

import glob
from pathlib import Path
from typing import Any

import duckdb

from .storage import AnalyticsStorage, sql_quote


DATASET_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "entity_state_events": [
        ("recorded_at", "TIMESTAMP"),
        ("event_type", "VARCHAR"),
        ("entity_id", "VARCHAR"),
        ("domain", "VARCHAR"),
        ("old_state", "VARCHAR"),
        ("new_state", "VARCHAR"),
        ("old_attributes_json", "VARCHAR"),
        ("new_attributes_json", "VARCHAR"),
        ("context_id", "VARCHAR"),
        ("user_id", "VARCHAR"),
        ("parent_id", "VARCHAR"),
        ("source", "VARCHAR"),
    ],
    "numeric_metric_samples": [
        ("recorded_at", "TIMESTAMP"),
        ("entity_id", "VARCHAR"),
        ("domain", "VARCHAR"),
        ("value", "DOUBLE"),
        ("unit", "VARCHAR"),
        ("attributes_json", "VARCHAR"),
        ("source", "VARCHAR"),
    ],
    "service_calls": [
        ("recorded_at", "TIMESTAMP"),
        ("domain", "VARCHAR"),
        ("service", "VARCHAR"),
        ("service_data_json", "VARCHAR"),
        ("context_id", "VARCHAR"),
        ("user_id", "VARCHAR"),
        ("source", "VARCHAR"),
    ],
    "script_runs": [
        ("recorded_at", "TIMESTAMP"),
        ("script_entity_id", "VARCHAR"),
        ("service", "VARCHAR"),
        ("service_data_json", "VARCHAR"),
        ("context_id", "VARCHAR"),
        ("user_id", "VARCHAR"),
        ("source", "VARCHAR"),
    ],
    "automation_events": [
        ("recorded_at", "TIMESTAMP"),
        ("automation_entity_id", "VARCHAR"),
        ("event_type", "VARCHAR"),
        ("details_json", "VARCHAR"),
        ("context_id", "VARCHAR"),
        ("source", "VARCHAR"),
    ],
    "presence_events": [
        ("recorded_at", "TIMESTAMP"),
        ("entity_id", "VARCHAR"),
        ("old_state", "VARCHAR"),
        ("new_state", "VARCHAR"),
        ("latitude", "DOUBLE"),
        ("longitude", "DOUBLE"),
        ("source", "VARCHAR"),
    ],
    "alert_events": [
        ("recorded_at", "TIMESTAMP"),
        ("entity_id", "VARCHAR"),
        ("state", "VARCHAR"),
        ("severity", "VARCHAR"),
        ("title", "VARCHAR"),
        ("message", "VARCHAR"),
        ("source", "VARCHAR"),
    ],
    "entity_registry_snapshots": [
        ("recorded_at", "TIMESTAMP"),
        ("entity_id", "VARCHAR"),
        ("platform", "VARCHAR"),
        ("device_id", "VARCHAR"),
        ("area_id", "VARCHAR"),
        ("original_name", "VARCHAR"),
        ("entity_category", "VARCHAR"),
        ("disabled_by", "VARCHAR"),
        ("hidden_by", "VARCHAR"),
        ("labels_json", "VARCHAR"),
        ("raw_json", "VARCHAR"),
    ],
    "device_registry_snapshots": [
        ("recorded_at", "TIMESTAMP"),
        ("device_id", "VARCHAR"),
        ("manufacturer", "VARCHAR"),
        ("model", "VARCHAR"),
        ("name_by_user", "VARCHAR"),
        ("area_id", "VARCHAR"),
        ("via_device_id", "VARCHAR"),
        ("configuration_url", "VARCHAR"),
        ("hw_version", "VARCHAR"),
        ("sw_version", "VARCHAR"),
        ("raw_json", "VARCHAR"),
    ],
    "screenlogic_metric_points": [
        ("recorded_at", "TIMESTAMP"),
        ("metric", "VARCHAR"),
        ("source_time", "TIMESTAMP"),
        ("value", "DOUBLE"),
        ("imported_at", "TIMESTAMP"),
    ],
    "screenlogic_runs": [
        ("recorded_at", "TIMESTAMP"),
        ("run_type", "VARCHAR"),
        ("started_at", "TIMESTAMP"),
        ("ended_at", "TIMESTAMP"),
        ("duration_minutes", "DOUBLE"),
        ("imported_at", "TIMESTAMP"),
    ],
}


class AnalyticsBuilder:
    def __init__(self, storage: AnalyticsStorage) -> None:
        self.storage = storage

    def build_all(self) -> dict[str, Any]:
        self.storage.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(self.storage.catalog_path)) as con:
            self._register_raw_datasets(con)
            self._build_curated_tables(con)
            summary = self._build_summary(con)
            return summary

    def _register_raw_datasets(self, con: duckdb.DuckDBPyConnection) -> None:
        for dataset, columns in DATASET_SCHEMAS.items():
            glob_pattern = str(self.storage.raw_root / dataset / "**" / "*.parquet")
            files = glob.glob(glob_pattern, recursive=True)
            if files:
                typed_select = ", ".join(
                    f"try_cast({name} AS {kind}) AS {name}" for name, kind in columns
                )
                con.execute(
                    f"CREATE OR REPLACE VIEW raw_{dataset} AS "
                    f"SELECT {typed_select} FROM read_parquet('{sql_quote(glob_pattern)}', union_by_name=true, hive_partitioning=true)"
                )
            else:
                empty_select = ", ".join(f"CAST(NULL AS {kind}) AS {name}" for name, kind in columns)
                con.execute(
                    f"CREATE OR REPLACE VIEW raw_{dataset} AS SELECT {empty_select} WHERE FALSE"
                )

    def _build_curated_tables(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute(
            """
            CREATE OR REPLACE TABLE entity_state_timeline AS
            SELECT recorded_at, entity_id, domain, new_state AS state, new_attributes_json AS attributes_json
            FROM raw_entity_state_events
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE numeric_metrics_1m AS
            SELECT
              date_trunc('minute', recorded_at) AS bucket_start,
              entity_id,
              avg(value) AS avg_value,
              min(value) AS min_value,
              max(value) AS max_value,
              count(*) AS samples
            FROM raw_numeric_metric_samples
            GROUP BY 1, 2
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE numeric_metrics_5m AS
            SELECT
              date_trunc('minute', recorded_at) - ((extract(minute from recorded_at)::INT % 5) * INTERVAL 1 MINUTE) AS bucket_start,
              entity_id,
              avg(value) AS avg_value,
              min(value) AS min_value,
              max(value) AS max_value,
              count(*) AS samples
            FROM raw_numeric_metric_samples
            GROUP BY 1, 2
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE device_usage_sessions AS
            WITH ordered AS (
              SELECT
                entity_id,
                domain,
                recorded_at,
                new_state AS state,
                lead(recorded_at) OVER (PARTITION BY entity_id ORDER BY recorded_at) AS ended_at
              FROM raw_entity_state_events
            ),
            active AS (
              SELECT *,
                CASE
                  WHEN domain = 'media_player' AND state = 'playing' THEN TRUE
                  WHEN domain IN ('light', 'switch', 'fan') AND state = 'on' THEN TRUE
                  WHEN domain = 'vacuum' AND state IN ('cleaning', 'returning') THEN TRUE
                  WHEN domain = 'climate' AND state NOT IN ('off', 'unknown', 'unavailable') THEN TRUE
                  WHEN domain = 'lock' AND state = 'unlocked' THEN TRUE
                  WHEN domain = 'cover' AND state IN ('opening', 'open') THEN TRUE
                  ELSE FALSE
                END AS is_active
              FROM ordered
            )
            SELECT
              entity_id,
              domain,
              recorded_at AS started_at,
              ended_at,
              state,
              datediff('second', recorded_at, coalesce(ended_at, current_timestamp)) AS duration_seconds
            FROM active
            WHERE is_active
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE room_activity_sessions AS
            WITH latest_entities AS (
              SELECT *
              FROM (
                SELECT *,
                  row_number() OVER (PARTITION BY entity_id ORDER BY recorded_at DESC) AS row_num
                FROM raw_entity_registry_snapshots
              )
              WHERE row_num = 1
            )
            SELECT
              coalesce(latest_entities.area_id, 'unassigned') AS area_id,
              device_usage_sessions.entity_id,
              device_usage_sessions.domain,
              device_usage_sessions.started_at,
              device_usage_sessions.ended_at,
              device_usage_sessions.duration_seconds
            FROM device_usage_sessions
            LEFT JOIN latest_entities USING (entity_id)
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE presence_intervals AS
            WITH ordered AS (
              SELECT
                entity_id,
                recorded_at,
                new_state,
                lead(recorded_at) OVER (PARTITION BY entity_id ORDER BY recorded_at) AS ended_at
              FROM raw_presence_events
            )
            SELECT
              entity_id,
              new_state AS presence_state,
              recorded_at AS started_at,
              ended_at,
              datediff('second', recorded_at, coalesce(ended_at, current_timestamp)) AS duration_seconds
            FROM ordered
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE media_sessions AS
            SELECT *
            FROM device_usage_sessions
            WHERE domain = 'media_player'
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE pool_sessions AS
            SELECT
              started_at,
              ended_at,
              duration_minutes
            FROM raw_screenlogic_runs
            WHERE run_type = 'poolRuns'
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE spa_sessions AS
            SELECT
              started_at,
              ended_at,
              duration_minutes
            FROM raw_screenlogic_runs
            WHERE run_type = 'spaRuns'
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE hvac_sessions AS
            SELECT *
            FROM device_usage_sessions
            WHERE domain = 'climate'
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE vacuum_runs AS
            SELECT *
            FROM device_usage_sessions
            WHERE domain = 'vacuum'
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE car_charging_sessions AS
            WITH ordered AS (
              SELECT
                entity_id,
                recorded_at,
                new_state,
                lead(recorded_at) OVER (PARTITION BY entity_id ORDER BY recorded_at) AS ended_at
              FROM raw_entity_state_events
              WHERE entity_id ILIKE '%charging%'
            )
            SELECT
              entity_id,
              recorded_at AS started_at,
              ended_at,
              datediff('second', recorded_at, coalesce(ended_at, current_timestamp)) AS duration_seconds
            FROM ordered
            WHERE new_state = 'on'
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE car_climate_sessions AS
            WITH ordered AS (
              SELECT
                entity_id,
                recorded_at,
                new_state,
                lead(recorded_at) OVER (PARTITION BY entity_id ORDER BY recorded_at) AS ended_at
              FROM raw_entity_state_events
              WHERE entity_id ILIKE '%climate%'
            )
            SELECT
              entity_id,
              recorded_at AS started_at,
              ended_at,
              datediff('second', recorded_at, coalesce(ended_at, current_timestamp)) AS duration_seconds
            FROM ordered
            WHERE new_state = 'on'
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE sleep_cooling_sessions AS
            SELECT *
            FROM device_usage_sessions
            WHERE entity_id = 'climate.dock_pro_bed'
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE alert_intervals AS
            WITH alert_like AS (
              SELECT recorded_at, entity_id, new_state
              FROM raw_entity_state_events
              WHERE domain = 'alert'
                 OR entity_id ILIKE '%alert%'
                 OR entity_id ILIKE '%warning%'
              UNION ALL
              SELECT recorded_at, entity_id, state AS new_state
              FROM raw_alert_events
            ),
            ordered AS (
              SELECT
                entity_id,
                recorded_at,
                new_state,
                lead(recorded_at) OVER (PARTITION BY entity_id ORDER BY recorded_at) AS ended_at
              FROM alert_like
            )
            SELECT
              entity_id,
              new_state AS alert_state,
              recorded_at AS started_at,
              ended_at,
              datediff('second', recorded_at, coalesce(ended_at, current_timestamp)) AS duration_seconds
            FROM ordered
            WHERE new_state IN ('on', 'problem', 'warning', 'triggered')
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE hourly_home_features AS
            SELECT
              date_trunc('hour', recorded_at) AS hour_start,
              count(*) FILTER (WHERE domain = 'light' AND new_state = 'on') AS light_on_events,
              count(*) FILTER (WHERE domain = 'media_player' AND new_state = 'playing') AS media_start_events,
              count(*) FILTER (WHERE entity_id = 'person.barrett_mckinney' AND new_state = 'home') AS arrival_events,
              count(*) FILTER (WHERE entity_id = 'person.barrett_mckinney' AND new_state = 'not_home') AS departure_events,
              count(*) FILTER (WHERE entity_id ILIKE '%pentair%' AND new_state = 'on') AS pool_equipment_events,
              count(*) AS total_events
            FROM raw_entity_state_events
            GROUP BY 1
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE daily_home_features AS
            SELECT
              date_trunc('day', hour_start) AS day_start,
              sum(light_on_events) AS light_on_events,
              sum(media_start_events) AS media_start_events,
              sum(arrival_events) AS arrival_events,
              sum(departure_events) AS departure_events,
              sum(pool_equipment_events) AS pool_equipment_events,
              sum(total_events) AS total_events
            FROM hourly_home_features
            GROUP BY 1
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE arrival_features AS
            SELECT
              date_trunc('day', started_at) AS day_start,
              extract(hour FROM started_at) AS arrival_hour,
              duration_seconds
            FROM presence_intervals
            WHERE entity_id = 'person.barrett_mckinney' AND presence_state = 'home'
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE evening_features AS
            SELECT *
            FROM hourly_home_features
            WHERE extract(hour FROM hour_start) BETWEEN 17 AND 23
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE pool_spa_efficiency_features AS
            WITH air AS (
              SELECT source_time, value
              FROM raw_screenlogic_metric_points
              WHERE metric = 'airTemps'
            )
            SELECT
              runs.run_type,
              runs.started_at,
              runs.ended_at,
              runs.duration_minutes,
              avg(air.value) AS avg_air_temp
            FROM raw_screenlogic_runs AS runs
            LEFT JOIN air
              ON air.source_time BETWEEN runs.started_at AND runs.ended_at
            GROUP BY 1, 2, 3, 4
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE media_habit_features AS
            SELECT
              entity_id,
              extract(hour FROM started_at) AS hour_of_day,
              strftime(started_at, '%w') AS day_of_week,
              count(*) AS session_count,
              avg(duration_seconds) AS avg_duration_seconds
            FROM media_sessions
            GROUP BY 1, 2, 3
            """
        )

    def _build_summary(self, con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        def scalar(query: str) -> str:
            row = con.execute(query).fetchone()
            return "" if row is None or row[0] is None else str(row[0])

        return {
            "top_active_rooms_today": scalar(
                """
                WITH today AS (
                  SELECT area_id, sum(duration_seconds) AS active_seconds
                  FROM room_activity_sessions
                  WHERE started_at >= date_trunc('day', current_timestamp)
                  GROUP BY 1
                  ORDER BY active_seconds DESC
                  LIMIT 3
                )
                SELECT string_agg(area_id || ': ' || round(active_seconds / 60.0, 1)::VARCHAR || ' min', ' | ')
                FROM today
                """
            ),
            "recent_pool_spa_efficiency": scalar(
                """
                WITH recent AS (
                  SELECT run_type, avg(duration_minutes) AS avg_minutes, avg(avg_air_temp) AS avg_air
                  FROM pool_spa_efficiency_features
                  WHERE started_at >= current_timestamp - INTERVAL 30 DAY
                  GROUP BY 1
                )
                SELECT string_agg(run_type || ': ' || round(avg_minutes, 1)::VARCHAR || ' min @ ' || round(avg_air, 1)::VARCHAR || 'F', ' | ')
                FROM recent
                """
            ),
            "recommendation_feed": scalar(
                """
                WITH best_media AS (
                  SELECT entity_id, sum(session_count) AS plays
                  FROM media_habit_features
                  WHERE hour_of_day BETWEEN extract(hour FROM current_timestamp) - 1 AND extract(hour FROM current_timestamp) + 1
                  GROUP BY 1
                  ORDER BY plays DESC
                  LIMIT 1
                )
                SELECT coalesce('Likely media zone now: ' || entity_id, 'No recommendation yet')
                FROM best_media
                """
            ),
            "anomaly_feed": scalar(
                """
                WITH long_runs AS (
                  SELECT entity_id, round(max(duration_seconds) / 3600.0, 1) AS hours_active
                  FROM device_usage_sessions
                  WHERE started_at >= current_timestamp - INTERVAL 7 DAY
                  GROUP BY 1
                  HAVING max(duration_seconds) > 4 * 3600
                  ORDER BY hours_active DESC
                  LIMIT 3
                )
                SELECT coalesce(string_agg(entity_id || ': ' || hours_active::VARCHAR || ' h', ' | '), 'No unusual long runtimes')
                FROM long_runs
                """
            ),
        }
