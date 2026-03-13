from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any

import duckdb

from .config import AnalyticsConfig


def parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def sql_quote(value: str) -> str:
    return value.replace("'", "''")


@dataclass(slots=True)
class StorageStatus:
    healthy: bool
    reason: str
    root_device: int | None
    reference_device: int | None


class AnalyticsStorage:
    def __init__(self, config: AnalyticsConfig) -> None:
        self.config = config
        self.root = config.analytics_root
        self.raw_root = config.raw_root
        self.curated_root = config.curated_root
        self.catalog_path = config.catalog_path
        self.raw_queue_root = config.raw_queue_root
        self.checkpoint_dir = config.checkpoint_dir

    def prepare(self) -> StorageStatus:
        self.root.mkdir(parents=True, exist_ok=True)
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.curated_root.mkdir(parents=True, exist_ok=True)
        self.raw_queue_root.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        return self.validate_root()

    def validate_root(self) -> StorageStatus:
        root_stat = self.root.stat()
        reference_device = None
        for candidate in (Path("/config"), Path("/homeassistant")):
            if candidate.exists():
                reference_device = candidate.stat().st_dev
                break
        same_device = reference_device is not None and root_stat.st_dev == reference_device
        if self.config.require_external_storage and same_device:
            return StorageStatus(
                healthy=False,
                reason="analytics root resolves to the same device as the Home Assistant config volume",
                root_device=root_stat.st_dev,
                reference_device=reference_device,
            )
        return StorageStatus(
            healthy=True,
            reason="analytics root is writable",
            root_device=root_stat.st_dev,
            reference_device=reference_device,
        )

    def free_gb(self) -> float:
        usage = shutil.disk_usage(self.root)
        return usage.free / (1024 ** 3)

    def append_events(self, dataset: str, events: list[dict[str, Any]], ts_key: str = "recorded_at") -> int:
        if not events:
            return 0
        grouped: dict[tuple[str, str], list[str]] = {}
        for event in events:
            ts = parse_timestamp(str(event.get(ts_key)))
            date_part = ts.strftime("%Y-%m-%d")
            hour_part = ts.strftime("%H")
            grouped.setdefault((date_part, hour_part), []).append(json.dumps(event, separators=(",", ":")))
        count = 0
        for (date_part, hour_part), lines in grouped.items():
            target_dir = self.raw_queue_root / dataset / f"date={date_part}" / f"hour={hour_part}"
            target_dir.mkdir(parents=True, exist_ok=True)
            target_file = target_dir / "events.jsonl"
            with target_file.open("a", encoding="utf-8") as handle:
                for line in lines:
                    handle.write(line)
                    handle.write("\n")
                    count += 1
        return count

    def write_checkpoint(self, name: str, payload: dict[str, Any]) -> None:
        path = self.checkpoint_dir / name
        path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    def read_checkpoint(self, name: str) -> dict[str, Any] | None:
        path = self.checkpoint_dir / name
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def compact_queue(self) -> list[Path]:
        compacted: list[Path] = []
        for source in sorted(self.raw_queue_root.glob("**/*.jsonl")):
            if source.stat().st_size == 0:
                source.unlink()
                continue
            relative = source.relative_to(self.raw_queue_root)
            parts = relative.parts
            if len(parts) < 4:
                continue
            dataset, date_part, hour_part = parts[0], parts[1], parts[2]
            target_dir = self.raw_root / dataset / date_part / hour_part
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / f"{source.stem}.parquet"
            sql = (
                "COPY ("
                f"SELECT * FROM read_json_auto('{sql_quote(str(source))}', format='newline_delimited', union_by_name=true)"
                f") TO '{sql_quote(str(target))}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            with duckdb.connect() as con:
                con.execute(sql)
            source.unlink()
            compacted.append(target)
        return compacted

