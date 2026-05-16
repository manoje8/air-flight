"""
File retention policy utility.
Deletes files older than a specified number of days to prevent disk bloat.
"""

import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def cleanup_old_files(directory: str | Path, days_old: int = 7) -> int:
    """
    Deletes files in the specified directory that are older than `days_old` days.

    Returns the number of files deleted.
    """
    dir_path = Path(directory)
    if not dir_path.exists() or not dir_path.is_dir():
        logger.warning("Directory not found: %s", dir_path)
        return 0

    current_time = time.time()
    age_seconds = days_old * 86400
    deleted_count = 0

    for file_path in dir_path.iterdir():
        if file_path.is_file():
            file_mtime = file_path.stat().st_mtime
            if (current_time - file_mtime) > age_seconds:
                try:
                    file_path.unlink()
                    logger.info("Deleted old file: %s", file_path)
                    deleted_count += 1
                except Exception as e:
                    logger.error("Failed to delete %s: %s", file_path, e)

    logger.info(
        "Cleanup finished. Deleted %d files older than %d days in %s",
        deleted_count,
        days_old,
        dir_path,
    )
    return deleted_count


def run_bronze_cleanup(**context) -> int:
    """Airflow task wrapper for cleaning up old Bronze files (defaults to 7 days)."""
    bronze_dir = Path("/opt/airflow/data/bronze")
    return cleanup_old_files(directory=bronze_dir, days_old=7)
