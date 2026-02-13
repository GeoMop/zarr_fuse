Various highlevel S3 management scripts.
# KUBECTL basics

## Install kubectl and krew

instal ... [debian] ..

## Download kubectl config, see [CERIT doc](https://docs.cerit-sc.cz/en/docs/kubernetes/kubectl)

1. login to [rancher](https://rancher.cloud.e-infra.cz/)
   use e-infra identity

2. Click to 'kuba-cluster'. Download the kubeconfig through a document like icon at the top right corner.
   Rename to `~/.kube/config` and set permissions to 700 (see the doc).
   
3. Download files in queue:

   k3s-pvc-viewer.sh ingress-server kuba-cluster 
   
4. See directory `pending` for accepted data frames not yet processed into ZARR store.
   See directory `processed` for processed data frames.

## aws.sh - aws command wrapper, poorman profile handling

Future is `zf/cli.py` custom commandline tool for management of buckets and stores.
[According to ChatGPT](https://chatgpt.com/share/692e0024-8cd4-8004-9cb7-807bf58b210f) 
`boto3` packare represents Python API of `aws s3api` (lowerlevel cmd line api), 
`aws s3` is more high level tool, not always with its analogy in `boto3`.

