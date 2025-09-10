#!/bin/bash
#
# Script to install queue data to a local directory from a Kubernetes pod
# Please install kubectl and download kubeconfig from https://rancher.cloud.e-infra.cz/ and place it in ~/.kube/config

set -euo pipefail

usage() {
  echo "Usage: $0 <namespace> <kube_context_name>"
  echo "  namespace         - The Kubernetes namespace to query"
  echo "  kube_context_name - The name of the kube context in .kube/config file"
}

namespace=$1
kube_context_name=$2

if ! command -v kubectl &> /dev/null; then
  echo "kubectl could not be found, use install-kubectl-and-plugins.sh"
  exit 1
fi

if [ -z "$namespace" ]; then
  echo "Script requires a namespace argument"
  usage
  exit 1
fi

if [ -z "$kube_context_name" ]; then
  echo "Script requires a kube context name argument"
  usage
  exit 1
fi

kubectl config set-context "$kube_context_name" --namespace="$namespace"

pod_name=$(kubectl get pods -o jsonpath='{.items[0].metadata.name}')

# If this does not work check value in app/databuk/ingress_server/charts/values.yaml under deployment.queue.path
kubectl cp "$pod_name:/var/zarr_fuse" .

# Example of ~/.kube/config
# You can install kubeconfig directly from https://rancher.cloud.e-infra.cz/
# apiVersion: v1
# clusters:
#   - cluster:
#       server: https://rancher.cloud.e-infra.cz/k8s/clusters/c-m-qvndqhf6
#     name: cerit-rancher
#   - cluster:
#       server: https://other.....
#     name: other
# contexts:
#   - context:
#       cluster: cerit-rancher
#       user: cerit-rancher
#     name: cerit-rancher
#   - context:
#       cluster: other
#       user: other
#     name: other
# current-context: cerit-rancher
# kind: Config
# users:
#   - name: cerit-rancher
#     user:
#       token: ...........
#   - name: other
#     user:
#       token: ...........
