.PHONY: generate-helm-docs helm-template helm-lint helm-dependency-build install-helm-docs

generate-helm-docs:
	helm-docs . --skip-version-footer


helm-template:
	helm template zarr-fuse-ingress . --values ./values/minimal-required-values.yaml
helm-lint:
	helm lint . --strict --values ./values/minimal-required-values.yaml
helm-dependency-build:
	helm dependency build .

# Requires installation of go and helm plugins
install-helm-docs:
	go install github.com/norwoodj/helm-docs/cmd/helm-docs@latest
