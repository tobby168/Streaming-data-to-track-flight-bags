#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="baggage-poc"
CLUSTER_NAME="${KIND_CLUSTER:-baggage}"

echo "==> Creating namespace"
kubectl apply -f k8s/namespaces.yaml

echo "==> Building images"
./scripts/build-flink-image.sh
docker build -t baggage-producer:local -f docker/producer/Dockerfile .

if command -v kind >/dev/null 2>&1 && kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "==> Loading images into kind cluster '${CLUSTER_NAME}'"
  kind load docker-image baggage-flink:local --name "${CLUSTER_NAME}"
  kind load docker-image baggage-producer:local --name "${CLUSTER_NAME}"
fi

echo "==> Deploying Kafka and UI"
kubectl apply -f k8s/kafka/cp-kafka.yaml
kubectl apply -f k8s/kafka/kafka-ui.yaml

echo "==> Deploying ClickHouse and schema init"
kubectl apply -f k8s/clickhouse/clickhouse.yaml
kubectl apply -f k8s/clickhouse-init/init-job.yaml

echo "==> Deploying Flink cluster and submitting jobs"
kubectl apply -f k8s/flink/jobmanager.yaml
kubectl apply -f k8s/flink/taskmanager.yaml
kubectl apply -f k8s/flink/job-submit.yaml

echo "==> Deploying event producer"
kubectl apply -f k8s/producer/configmap.yaml
kubectl apply -f k8s/producer/deployment.yaml

echo "==> Deploying Grafana datasource, alerts, and dashboards"
kubectl apply -f k8s/grafana/datasources.yaml
kubectl apply -f k8s/grafana/alerts/alert-rules.yaml
kubectl -n "${NAMESPACE}" create configmap grafana-dashboards \
  --from-file=k8s/grafana/dashboards --dry-run=client -o yaml | kubectl apply -f -

echo "==> All resources applied. Port-forward with:"
echo "    kubectl -n ${NAMESPACE} port-forward svc/grafana 3000:80"
echo "    kubectl -n ${NAMESPACE} port-forward svc/kafka-ui 8080:8080"
