.PHONY: generate generate-helm-docs generate-values-schema install-helm-docs install-helm-schema

generate: generate-helm-docs generate-values-schema
generate-helm-docs:
	helm-docs . --skip-version-footer
generate-values-schema:
	helm schema

install-helm-docs:
	go install github.com/norwoodj/helm-docs/cmd/helm-docs@latest
install-helm-schema:
	helm plugin install https://github.com/holgerjh/helm-schema
