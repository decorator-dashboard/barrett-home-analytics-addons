# Home Analytics

This add-on captures raw Home Assistant history onto local SSD storage and builds DuckDB views and tables for trend analysis and proactive features.

## What it does

- Subscribes to Home Assistant events over the websocket API
- Stores raw data as JSONL batches and compacted Parquet partitions
- Builds a local DuckDB analytics catalog
- Publishes health and summary sensors back into Home Assistant
- Optionally imports retained Pentair ScreenLogic history into the same catalog

## Important limits

- This add-on expects long-term storage to live on external media, not the Pi SD card
- If `require_external_storage` is `true` and the configured analytics root resolves to the same device as `/config`, the collector will stay in a degraded state and refuse raw writes
- Raw history is the canonical source; curated views are rebuildable

## Key options

- `analytics_root`: target analytics root, ideally on USB SSD
- `require_external_storage`: prevent accidental writes to the SD card
- `snapshot_interval_seconds`: cadence for numeric snapshots
- `compaction_interval_minutes`: JSONL to Parquet cadence
- `feature_build_interval_minutes`: DuckDB curated rebuild cadence
- `screenlogic_import_enabled`: import retained ScreenLogic history and sessions

