#!/bin/bash
# Render bench-job-multiclient.template.yaml with a timestamped Job name
# and a matched MC_RUN_ID, then kubectl apply.
# Mirrors apply-bench-job.sh — same generator pattern, plus NUM_CLIENTS
# and MC_RUN_ID injection.
#
# Required env vars:
#   KUBE_CONTEXT  kubectl context to apply to (e.g. soju).
#   PVC_NAME      RWX PVC to mount at /mnt/hs (must exist in `default` ns).
#   STORAGECLASS  Backing StorageClass; recorded in the Sheet row.
#   NUM_CLIENTS   Pod count (= Job parallelism = completions). This is the
#                 experiment's operative variable.
#
# Usage:
#   KUBECONFIG=~/.kube/soju.yaml \
#   KUBE_CONTEXT=soju \
#   PVC_NAME=hammerspace-10dsx-test-pvc-01 \
#   STORAGECLASS=hammerspace-10dsx \
#   NUM_CLIENTS=4 \
#       ./apply-bench-job-multiclient.sh
set -euo pipefail

: "${KUBE_CONTEXT:?KUBE_CONTEXT env var is required (e.g. soju)}"
: "${PVC_NAME:?PVC_NAME env var is required}"
: "${STORAGECLASS:?STORAGECLASS env var is required}"
: "${NUM_CLIENTS:?NUM_CLIENTS env var is required}"

if ! [[ "${NUM_CLIENTS}" =~ ^[0-9]+$ ]] || [ "${NUM_CLIENTS}" -lt 1 ]; then
  echo "ERROR: NUM_CLIENTS must be a positive integer (got: ${NUM_CLIENTS})" >&2
  exit 2
fi

HERE="$(cd "$(dirname "$0")" && pwd)"
TEMPLATE="${HERE}/bench-job-multiclient.template.yaml"
TS="$(date -u +%Y%m%dt%H%M%Sz)"
JOB_NAME="fileio-mcbench-n${NUM_CLIENTS}-${TS}"
MC_RUN_ID="${TS}"
OUT="${HERE}/generated/${TS}-mc-n${NUM_CLIENTS}.yaml"

mkdir -p "${HERE}/generated"

sed -e "s|__JOB_NAME__|${JOB_NAME}|g" \
    -e "s|__PVC_NAME__|${PVC_NAME}|g" \
    -e "s|__STORAGECLASS__|${STORAGECLASS}|g" \
    -e "s|__NUM_CLIENTS__|${NUM_CLIENTS}|g" \
    -e "s|__MC_RUN_ID__|${MC_RUN_ID}|g" \
    "${TEMPLATE}" > "${OUT}"

echo "[gen] ${OUT}  (ctx=${KUBE_CONTEXT}, pvc=${PVC_NAME}, sc=${STORAGECLASS}, N=${NUM_CLIENTS})"
kubectl --context "${KUBE_CONTEXT}" apply -f "${OUT}"
echo "[applied] job/${JOB_NAME} (context: ${KUBE_CONTEXT}, namespace: default)"
echo "[hint] kubectl --context ${KUBE_CONTEXT} -n default logs -f -l job-name=${JOB_NAME} --max-log-requests=${NUM_CLIENTS} --prefix=true"
echo "[hint] kubectl --context ${KUBE_CONTEXT} -n default logs -f -l job-name=${JOB_NAME},batch.kubernetes.io/job-completion-index=0  # leader only"
