from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import subprocess
from pathlib import Path
import shutil
from typing import Any

from .config import AnalyticsConfig


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _flatten_screenlogic(raw: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    imported_at = utc_now_iso()
    metric_rows: list[dict[str, Any]] = []
    run_rows: list[dict[str, Any]] = []

    for metric, points in raw.items():
        if metric.endswith("Temps"):
            for point in points or []:
                metric_rows.append(
                    {
                        "recorded_at": imported_at,
                        "metric": metric,
                        "source_time": point.get("time"),
                        "value": point.get("temp"),
                        "imported_at": imported_at,
                    }
                )
        elif metric.endswith("Runs"):
            for run in points or []:
                started_at = run.get("on")
                ended_at = run.get("off")
                duration_minutes = None
                if started_at and ended_at:
                    start_dt = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
                    end_dt = datetime.fromisoformat(str(ended_at).replace("Z", "+00:00"))
                    duration_minutes = round((end_dt - start_dt).total_seconds() / 60.0, 2)
                run_rows.append(
                    {
                        "recorded_at": imported_at,
                        "run_type": metric,
                        "started_at": started_at,
                        "ended_at": ended_at,
                        "duration_minutes": duration_minutes,
                        "imported_at": imported_at,
                    }
                )
    return metric_rows, run_rows


def import_screenlogic(config: AnalyticsConfig) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    if not config.screenlogic_import_enabled:
        return None

    node_bin = shutil.which("node")
    if not node_bin:
        return None

    output_dir = config.checkpoints_root / "screenlogic"
    output_dir.mkdir(parents=True, exist_ok=True)
    script_path = Path("/app/screenlogic_import.js")
    if not script_path.exists():
        return None

    env = {
        **dict(os.environ),
        "SCREENLOGIC_SYSTEM_NAME": config.screenlogic_system_name,
        "SCREENLOGIC_PASSWORD": config.screenlogic_password,
        "SCREENLOGIC_HISTORY_DAYS": str(config.screenlogic_history_days),
    }
    try:
        subprocess.run(
            [
                node_bin,
                str(script_path),
                "--system-name",
                config.screenlogic_system_name,
                "--password",
                config.screenlogic_password,
                "--days",
                str(config.screenlogic_history_days),
                "--out-dir",
                str(output_dir),
            ],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None

    raw_path = output_dir / "screenlogic_history_raw.json"
    if not raw_path.exists():
        return None
    raw = json.loads(raw_path.read_text())
    return _flatten_screenlogic(raw)
