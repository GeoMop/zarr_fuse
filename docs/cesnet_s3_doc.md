# Essentials from CESNET S3 usage

## zarr-fuse project related stuff

organization name: VO_tul_scifuse
endpoint: https://s3.cl4.du.cesnet.cz

There is a "service account" to access storage from various services.
To login into gatekeeper and get secrets use the "e-infra" organization and "service_jb" username.

## Secrets
Procedure to get secrets and secrets for tests are described in Google document:
https://docs.google.com/document/d/1l30E-RoBoqZezmrh4PKMczQNGvss7I1wYVf3833lJ78/edit?tab=t.0

## ./aws.sh script
- uses "cesnet_s3_keys.json" file to set access keys
- uses correct endpoint url
usage:
```
    aws.sh <command> <options>
```

## How to create a bucket?
```
aws.sh create-bucket --bucket <bucket name>
```
Bucket name can not contain underscores.

## How to list buckets?
```
aws.sh list-buckets
```
