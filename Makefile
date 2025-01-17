.PHONY: build test test-integration

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/pgguru/conduit-connector-bigquery.version=${VERSION}'" -o conduit-connector-bigquery cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -v -race ./...

test-integration:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose-template.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -v -race ./...; ret=$$?; \
		docker compose -f test/docker-compose-template.yml down; \
		exit $$ret
