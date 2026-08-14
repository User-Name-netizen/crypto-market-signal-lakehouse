#!/usr/bin/env sh
set -eu

prefect deployment run "lakehouse-stream-monitor-flow/monitor-prod"
