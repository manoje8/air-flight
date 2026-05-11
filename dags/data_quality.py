"""
Sensor that validates a Silver-layer quality report before allowing
Gold-layer tasks to proceed.
"""

import logging
from typing import Any

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)

class DataQualityOperator(BaseSensorOperator):
    """
    source_task_id : str
        task_id of the upstream ``quality`` task whose XCom dict to read.
    min_row_count : int
        Minimum acceptable row count. Defaults to 1.
    fail_on_empty : bool
        If True (default), raises ValueError → DAG failure + alert.
        If False, raises AirflowSkipException → Gold tasks skipped silently.
    extra_checks : dict[str, Any] | None
        Optional ``{report_key: expected_value}`` pairs that must all match.
        Example: ``{"null_rate": 0.0, "duplicate_count": 0}``
    """

    template_fields = ("source_task_id")
    ui_color = "#f0ad4e"

    def __init__(
            self,
            *,
            source_task_id:str = "quality",
            min_row_count:int = 1,
            fail_on_empty:bool = True,
            extra_checks: dict[str, Any] | None = None,
            **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.min_row_count = min_row_count
        self.fail_on_empty = fail_on_empty
        self.extra_checks = extra_checks or {}


    def poke(self, context: Context) -> bool:
        ti = context['ti']
        report: dict[str, Any] | None = ti.xcom_pull(task_ids=self.source_task_id)

        if report is None:
            logger.warning(f"[Data Quality operator]: No Xcom from task {self.source_task_id} yet - retrying")
            return False

        row_count: int = report.get("row_count", 0)
        logger.info(f"[Data Quality operator]: Silver row count: {row_count} (min required: {self.min_row_count})")

        if row_count < self.min_row_count:
            msg = (
                f"DataQualityOperator: Silver has {row_count} rows "
                f"(minimum required: {self.min_row_count}). Blocking Gold load."
            )

            logger.error(msg)
            if self.fail_on_empty:
                raise ValueError(msg)

            raise AirflowSkipException(msg)


        failures: list[str] = []

        for key, expected in self.extra_checks.items():
            actual = report.get(key)
            if actual != expected:
                failures.append(f"{key}: {actual} != {expected}")

        if failures:
            msg = "DataQualityOperator: extra checks failed:\n" + "\n".join(failures)
            logger.error(msg)
            raise ValueError(msg)

        logger.info("DataQualityOperator: all quality checks passed — unblocking Gold.")

        return True