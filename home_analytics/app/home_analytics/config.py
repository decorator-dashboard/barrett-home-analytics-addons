from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path


OPTIONS_PATH = Path("/data/options.json")
DEFAULT_INCLUDE_DOMAINS = [
    "light",
    "switch",
    "climate",
    "sensor",
    "binary_sensor",
    "media_player",
    "remote",
    "vacuum",
    "fan",
    "cover",
    "lock",
    "scene",
    "script",
    "person",
    "device_tracker",
    "weather",
    "mbapi2020",
    "screenlogic",
    "input_boolean",
    "input_datetime",
    "input_number",
    "input_select",
    "input_text",
]


@dataclass(slots=True)
class AnalyticsConfig:
    analytics_root: Path = Path("/media/home_analytics")
    require_external_storage: bool = True
    snapshot_interval_seconds: int = 60
    compaction_interval_minutes: int = 15
    feature_build_interval_minutes: int = 30
    screenlogic_import_enabled: bool = True
    screenlogic_history_days: int = 365
    screenlogic_system_name: str = "Pentair: 22-8E-AD"
    screenlogic_password: str = ""
    notify_service: str = "notify.mobile_app_iphone"
    ssd_min_free_gb: int = 10
    include_domains: list[str] = field(default_factory=lambda: list(DEFAULT_INCLUDE_DOMAINS))
    ha_base_url: str = "http://supervisor/core"
    ha_ws_url: str = "ws://supervisor/core/websocket"
    supervisor_token: str = ""
    data_root: Path = Path("/data")
    runtime_root: Path = Path("/data/runtime")
    checkpoints_root: Path = Path("/data/checkpoints")

    @classmethod
    def load(cls) -> "AnalyticsConfig":
        raw: dict[str, object] = {}
        if OPTIONS_PATH.exists():
            raw = json.loads(OPTIONS_PATH.read_text())

        return cls(
            analytics_root=Path(str(raw.get("analytics_root", "/media/home_analytics"))),
            require_external_storage=bool(raw.get("require_external_storage", True)),
            snapshot_interval_seconds=int(raw.get("snapshot_interval_seconds", 60)),
            compaction_interval_minutes=int(raw.get("compaction_interval_minutes", 15)),
            feature_build_interval_minutes=int(raw.get("feature_build_interval_minutes", 30)),
            screenlogic_import_enabled=bool(raw.get("screenlogic_import_enabled", True)),
            screenlogic_history_days=int(raw.get("screenlogic_history_days", 365)),
            screenlogic_system_name=str(raw.get("screenlogic_system_name", "Pentair: 22-8E-AD")),
            screenlogic_password=str(raw.get("screenlogic_password", "")),
            notify_service=str(raw.get("notify_service", "notify.mobile_app_iphone")),
            ssd_min_free_gb=int(raw.get("ssd_min_free_gb", 10)),
            include_domains=list(raw.get("include_domains", DEFAULT_INCLUDE_DOMAINS)),
            ha_base_url=os.getenv("HA_BASE_URL", "http://supervisor/core"),
            ha_ws_url=os.getenv("HA_WS_URL", "ws://supervisor/core/websocket"),
            supervisor_token=os.getenv("SUPERVISOR_TOKEN", ""),
        )

    def ensure_paths(self) -> None:
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        self.checkpoints_root.mkdir(parents=True, exist_ok=True)

    @property
    def catalog_path(self) -> Path:
        return self.analytics_root / "catalog" / "home_analytics.duckdb"

    @property
    def raw_root(self) -> Path:
        return self.analytics_root / "raw"

    @property
    def curated_root(self) -> Path:
        return self.analytics_root / "curated"

    @property
    def checkpoint_dir(self) -> Path:
        return self.analytics_root / "checkpoints"

    @property
    def raw_queue_root(self) -> Path:
        return self.analytics_root / "raw_queue"

