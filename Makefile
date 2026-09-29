SHELL := /bin/bash

GO ?= go
PREK ?= prek
KO ?= ko
DOCKER ?= docker
TARGET_ARCH ?= $(shell $(GO) env GOARCH)
CGO_ENABLED ?= 1
BIN_DIR ?= bin
SERVICE_BIN := $(BIN_DIR)/overturetunkki
CLIENT_BIN := $(BIN_DIR)/overture-client
NATIVE_PROBE_BIN := $(BIN_DIR)/native-probe
IMAGE_REPO ?= overturetunkki/native-probe
IMAGE_TAG ?= dev
IMAGE_REF := $(IMAGE_REPO):$(IMAGE_TAG)
SERVICE_IMAGE_REPO ?= overturetunkki/service
SERVICE_IMAGE_TAG ?= dev
SERVICE_IMAGE_REF := $(SERVICE_IMAGE_REPO):$(SERVICE_IMAGE_TAG)

.PHONY: all lint test build build-linux fetch-extensions image service-image container-test smoke hooks clean

all: test

lint:
	$(PREK) run --all-files

test:
	$(GO) test ./...

build:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -trimpath -o $(SERVICE_BIN) ./cmd/overturetunkki
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -trimpath -o $(CLIENT_BIN) ./cmd/overture-client
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -trimpath -o $(NATIVE_PROBE_BIN) ./cmd/native-probe

build-linux:
	mkdir -p $(BIN_DIR)/linux-$(TARGET_ARCH)
	$(DOCKER) buildx build --platform linux/$(TARGET_ARCH) \
		--build-arg GO_VERSION=$$(awk -F= '$$1 == "GO_VERSION" { print $$2 }' build/versions.env) \
		--output type=local,dest=$(BIN_DIR)/linux-$(TARGET_ARCH) \
		-f build/native-linux.Dockerfile .

fetch-extensions:
	./scripts/fetch-duckdb-extensions $(TARGET_ARCH)

image: fetch-extensions
	KO_DOCKER_REPO=$(IMAGE_REPO) $(KO) build --local --bare --tags $(IMAGE_TAG) ./cmd/native-probe

service-image: fetch-extensions
	KO_DOCKER_REPO=$(SERVICE_IMAGE_REPO) $(KO) build --local --bare --tags $(SERVICE_IMAGE_TAG) ./cmd/overturetunkki

container-test: service-image
	OVERTURE_CONTAINER_TEST=1 OVERTURE_SERVICE_IMAGE=$(SERVICE_IMAGE_REF) $(GO) test ./integration -run '^TestContainerLifecycle$$' -count=1

smoke: image
	$(DOCKER) run --rm --network=none --read-only --cap-drop=ALL \
		--security-opt=no-new-privileges --user=65532:65532 \
		--env KO_DATA_PATH=/ko-app $(IMAGE_REF)

hooks:
	$(PREK) install

clean:
	rm -rf $(BIN_DIR)
