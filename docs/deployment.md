# Deployment docs

## Kubernetes basics

### Install kubectl

Install `kubectl`, you can use the installation script:

    tools/kubectl-install [debian|redhat|suse]

### Download kubectl config
Follow [CERIT doc](https://docs.cerit-sc.cz/en/docs/kubernetes/kubectl).

1. Login to [rancher](https://rancher.cloud.e-infra.cz/) using e-infra identity.

2. Click to 'kuba-cluster'. Download the kubeconfig through a document-like icon at the top right corner.
   Rename to `~/.kube/config` and set permissions to 700 (see the doc).

### Download accepted files

```
   toolts/ingress-download ingress-server kuba-cluster
```

See directory `accepted` for accepted data frames not yet processed into ZARR store.
See directory `failed` for failed data frames.
See directory `success` for processed data frames.
