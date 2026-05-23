"""
src/common/config.py
====================
Reads /config/dir_config.yaml and exposes all project paths
as importable Path constants.

Usage
-----
  from common.config import BRONZE_DIR, SILVER_DIR, LOG_DIR, RAW_DIR

Migrating to a new machine
--------------------------
  Only change root_dir in /config/dir_config.yaml — nothing else.
"""

import yaml
from pathlib import Path

# ── Locate dir_config.yaml ─────────────────────────────────────────────────────
# Works regardless of where the script is invoked from
_SRC_DIR     = Path(__file__).resolve().parent        # src/common/
_PROJECT_DIR = _SRC_DIR.parent.parent                 # project root
_CONFIG_FILE = _PROJECT_DIR / "config" / "dir_config.yaml"

if not _CONFIG_FILE.exists():
    raise FileNotFoundError(
        f"dir_config.yaml not found at: {_CONFIG_FILE}\n"
        f"Expected location: config/dir_config.yaml under project root."
    )

# ── Load YAML ──────────────────────────────────────────────────────────────────
with open(_CONFIG_FILE) as f:
    _cfg = yaml.safe_load(f)

# ── Base directory ─────────────────────────────────────────────────────────────
BASE_DIR = Path(_cfg["project"]["root_dir"])

# ── Derived paths ──────────────────────────────────────────────────────────────
RAW_DIR    = BASE_DIR / _cfg["paths"]["raw_landing"]
BRONZE_DIR = BASE_DIR / _cfg["paths"]["bronze"]
SILVER_DIR = BASE_DIR / _cfg["paths"]["silver"]
LOG_DIR    = BASE_DIR / _cfg["paths"]["log"]
CHK_DIR    = BASE_DIR / _cfg["paths"]["checkpoints"]
CONFIG_DIR = BASE_DIR / _cfg["paths"]["config"]


# ── Sanity check (warn if base_dir doesn't exist yet) ─────────────────────────
if not BASE_DIR.exists():
    import warnings
    warnings.warn(f"root_dir does not exist: {BASE_DIR}")


if __name__ == "__main__":
    print(f"BASE_DIR   : {BASE_DIR}")
    print(f"RAW_DIR    : {RAW_DIR}")
    print(f"BRONZE_DIR : {BRONZE_DIR}")
    print(f"SILVER_DIR : {SILVER_DIR}")
    print(f"LOG_DIR    : {LOG_DIR}")
    print(f"CHK_DIR    : {CHK_DIR}")
    print(f"CONFIG_DIR : {CONFIG_DIR}")

# Added for gold layer
GOLD_DIR   = BASE_DIR / _cfg["paths"].get("gold", "data/gold")