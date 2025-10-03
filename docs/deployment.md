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
   tools/ingress-download ingress-server kuba-cluster
```

See directory `accepted` for accepted data frames not yet processed into ZARR store.
See directory `success` for processed data frames.
TODO: See directory `failed` for failed data frames.


### List pods
```
   kubectl get pods 
```
### Shell in the pod
```
   kubectl exec -it <pod-name> -n <namespace> -- /bin/bash
```

namespace = ingress-server


### View logs on Rancher web

1. login to [Rancher](https://rancher.cloud.e-infra.cz)
2. click to kuba-cluster
3. Workloads (left pannel) -> Pods
4. Click to particular pod instance 'ingress-server-*'
5. Dots on right top -> 'View logs'
6. Select time interval for logs on the bottom bar, a wheel icon
