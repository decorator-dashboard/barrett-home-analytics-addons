#!/usr/bin/env bash
set -euo pipefail

mkdir -p /data /data/runtime /data/checkpoints

exec python3 /app/main.py

