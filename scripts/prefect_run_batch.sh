#!/usr/bin/env sh
set -eu

prefect deployment run "lakehouse-batch-flow/batch-prod"
