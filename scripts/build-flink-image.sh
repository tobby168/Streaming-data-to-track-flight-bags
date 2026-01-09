#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME=${IMAGE_NAME:-baggage-flink:local}
DOCKERFILE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker build \
  -t "${IMAGE_NAME}" \
  -f "${DOCKERFILE_DIR}/docker/flink/Dockerfile" \
  "${DOCKERFILE_DIR}"

echo "Built ${IMAGE_NAME} with PyFlink jobs baked in."
