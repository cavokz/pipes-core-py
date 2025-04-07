SHELL := bash
TEE_STDERR := tee >(cat 1>&2)

DOCKER ?= docker

VAULT_ADDR=http://localhost:8200
VAULT_TOKEN=test
export VAULT_ADDR VAULT_TOKEN

vault-up:
	$(DOCKER) compose up --wait --quiet-pull

vault-down:
	$(DOCKER) compose down

test: vault-up
	echo "secret: mysecret" | elastic-pipes run vault/test.yaml --log-level=debug | [ "`$(TEE_STDERR)`" = "secret: mysecret" ]
