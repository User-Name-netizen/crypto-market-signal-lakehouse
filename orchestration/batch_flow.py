import os
import subprocess
import sys
from pathlib import Path
from typing import List

from prefect import flow, get_run_logger, task


PROJECT_ROOT = Path("/app")


def _run(cmd: List[str], cwd: Path = PROJECT_ROOT) -> None:
    """Run a command and stream its output to Prefect logs."""
    logger = get_run_logger()
    logger.info(f"Executing: {' '.join(cmd)} in {cwd}")

    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )

    if result.stdout:
        for line in result.stdout.strip().split("\n"):
            logger.info(line)

    if result.stderr:
        for line in result.stderr.strip().split("\n")[-20:]:
            logger.warning(line)

    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            cmd,
            output=result.stdout,
            stderr=result.stderr,
        )


@task(name="batch_to_bronze", retries=2, retry_delay_seconds=60)
def batch_to_bronze() -> None:
    """Read CSV files from workspace and write them into Bronze."""
    logger = get_run_logger()

    data_dir = os.getenv("BATCH_DATA_DIR", "/app/infra/workspace")
    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")] if os.path.isdir(data_dir) else []

    if not csv_files:
        logger.warning(f"Khong tim thay file CSV trong {data_dir}. Bo qua batch ingest.")
        return

    logger.info(f"Tim thay {len(csv_files)} file CSV: {csv_files}")
    _run([sys.executable, "processing/spark_batch_to_bronze.py"])
    logger.info("Batch -> Bronze hoan tat.")


@flow(name="lakehouse-batch-flow", retries=1, retry_delay_seconds=120)
def run_batch_pipeline() -> None:
    logger = get_run_logger()
    logger.info("Start batch pipeline")

    batch_to_bronze()

    logger.info("Batch pipeline done")


if __name__ == "__main__":
    run_batch_pipeline()
