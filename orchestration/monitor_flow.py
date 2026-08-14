from typing import Dict, List

import docker
from docker.errors import NotFound
from prefect import flow, get_run_logger, task


STREAM_CONTAINERS = [
    "lakehouse-stream-producer",
    "lakehouse-stream-bronze",
    "lakehouse-stream-silver",
    "lakehouse-stream-gold",
]


@task(name="inspect_streaming_containers", retries=1, retry_delay_seconds=15)
def inspect_streaming_containers() -> Dict[str, str]:
    client = docker.from_env()
    logger = get_run_logger()
    statuses: Dict[str, str] = {}

    for name in STREAM_CONTAINERS:
        try:
            container = client.containers.get(name)
            status = container.status
            statuses[name] = status
            if status != "running":
                logger.warning("Container %s status=%s. Restarting...", name, status)
                container.restart()
                container.reload()
                statuses[name] = container.status
                logger.info("Container %s restarted. New status=%s", name, container.status)
            else:
                logger.info("Container %s healthy (running)", name)
        except NotFound:
            statuses[name] = "not_found"
            logger.error("Container %s not found", name)

    client.close()
    return statuses


@flow(name="lakehouse-stream-monitor-flow")
def monitor_streaming_services() -> Dict[str, str]:
    return inspect_streaming_containers()


if __name__ == "__main__":
    monitor_streaming_services()
