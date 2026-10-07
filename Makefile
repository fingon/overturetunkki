SHELL := /bin/bash

GO ?= go
PREK ?= prek
KO = $(GO) tool -modfile=build/tools/go.mod ko
PODMAN ?= podman
include build/versions.env
BUILDER_IMAGE := docker.io/library/golang:$(GO_VERSION)-bookworm
LINUX_RUN = $(PODMAN) run --rm --platform linux/$(TARGET_ARCH) --userns=keep-id -e GOCACHE=/go/build-cache -e XDG_CONFIG_HOME=/tmp/config -e GOPATH=/go -v overturetunkki-go:/go -v "$(CURDIR):/src" -w /src $(BUILDER_IMAGE)
HOST_OS := $(shell $(GO) env GOOS)
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

.PHONY: test-linux test-darwin test-tools image-linux service-image-linux all lint test test-client build build-client build-native build-linux fetch-extensions image service-image container-test smoke hooks clean

all: test

lint:
	$(PREK) run --all-files

test: test-$(HOST_OS) test-tools

test-tools:
	cd build/tools && $(GO) test ./...

test-linux:
	$(GO) test ./...

test-darwin: test-client

test-client:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) test ./cmd/overture-client ./internal/client ./internal/logging

build: build-client
ifeq ($(HOST_OS),linux)
build: build-native
endif

build-client:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -trimpath -o $(CLIENT_BIN) ./cmd/overture-client

build-native:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -trimpath -o $(SERVICE_BIN) ./cmd/overturetunkki
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -trimpath -o $(NATIVE_PROBE_BIN) ./cmd/native-probe

build-linux:
	$(LINUX_RUN) make build BIN_DIR=$(BIN_DIR)/linux-$(TARGET_ARCH)

fetch-extensions:
	./scripts/fetch-duckdb-extensions $(TARGET_ARCH)

image:
	$(LINUX_RUN) make image-linux BIN_DIR=$(BIN_DIR) TARGET_ARCH=$(TARGET_ARCH) IMAGE_REPO=$(IMAGE_REPO) IMAGE_TAG=$(IMAGE_TAG)
	$(PODMAN) load -i $(BIN_DIR)/native-probe.tar

service-image:
	$(LINUX_RUN) make service-image-linux BIN_DIR=$(BIN_DIR) TARGET_ARCH=$(TARGET_ARCH) SERVICE_IMAGE_REPO=$(SERVICE_IMAGE_REPO) SERVICE_IMAGE_TAG=$(SERVICE_IMAGE_TAG)
	$(PODMAN) load -i $(BIN_DIR)/service.tar

image-linux: fetch-extensions
	mkdir -p $(BIN_DIR)
	KO_DOCKER_REPO=$(IMAGE_REPO) $(KO) build --platform linux/$(TARGET_ARCH) --push=false --bare --tags $(IMAGE_TAG) --tarball $(BIN_DIR)/native-probe.tar ./cmd/native-probe

service-image-linux: fetch-extensions
	mkdir -p $(BIN_DIR)
	KO_DOCKER_REPO=$(SERVICE_IMAGE_REPO) $(KO) build --platform linux/$(TARGET_ARCH) --push=false --bare --tags $(SERVICE_IMAGE_TAG) --tarball $(BIN_DIR)/service-ko.tar ./cmd/overturetunkki
	cd build/tools && $(GO) run ./cache-image --input $(abspath $(BIN_DIR))/service-ko.tar --output $(abspath $(BIN_DIR))/service.tar --tag $(SERVICE_IMAGE_REF)

container-test: service-image build-client
	mkdir -p $(BIN_DIR)/container-tmp
	TMPDIR=$(abspath $(BIN_DIR)/container-tmp) OVERTURE_CONTAINER_TEST=1 OVERTURE_SERVICE_IMAGE=$(SERVICE_IMAGE_REF) OVERTURE_CLIENT_BIN=$(abspath $(CLIENT_BIN)) $(GO) test ./integration -run '^TestContainerLifecycle$$' -count=1

smoke: image
	$(PODMAN) run --rm --network=none --read-only --cap-drop=ALL \
		--security-opt=no-new-privileges --user=65532:65532 \
		$(IMAGE_REF)

hooks:
	$(PREK) install

clean:
	rm -rf $(BIN_DIR)

.PHONY: vet
vet:
ifeq ($(HOST_OS),linux)
	$(GO) vet ./...
else
	CGO_ENABLED=$(CGO_ENABLED) $(GO) vet ./cmd/overture-client ./internal/client ./internal/logging
endif
