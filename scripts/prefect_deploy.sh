#!/usr/bin/env sh
set -eu

cd /app

export PREFECT_API_URL="http://prefect-server:4200/api"

prefect work-pool create docker-pool --type docker >/dev/null 2>&1 || true

# deploy theo prefect.yaml, trả lời "no" cho toàn bộ prompt tương tác
prefect deploy --all --prefect-file /app/prefect.yaml <<'EOF'
n
n
n
n
n
n
n
n
n
n
n
n
EOF

echo "Done: deployments applied."
