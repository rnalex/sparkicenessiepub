# Makefile for Spark-Iceberg-Nessie image

IMAGE_NAME ?= rnalex/spark-iceberg-nessie
TAG ?= latest
POM := pom.xml

# Function to extract property from pom.xml
get_prop = $(shell sed -n 's/.*<$(1)>\(.*\)<\/$(1)>.*/\1/p' $(POM))

SPARK_VERSION := $(call get_prop,spark.version)
SCALA_BINARY := $(call get_prop,scala.binary)
ICEBERG_VERSION := $(call get_prop,iceberg.version)
NESSIE_VERSION := $(call get_prop,nessie.version)

.PHONY: build build-multi push all

all: build

build:
	@echo "Building image $(IMAGE_NAME):$(TAG) for local platform with:"
	@echo "  Spark:   $(SPARK_VERSION)"
	@echo "  Scala:   $(SCALA_BINARY)"
	@echo "  Iceberg: $(ICEBERG_VERSION)"
	@echo "  Nessie:  $(NESSIE_VERSION)"
	docker build \
		--build-arg SPARK_VERSION=$(SPARK_VERSION) \
		--build-arg SCALA_BINARY=$(SCALA_BINARY) \
		--build-arg ICEBERG_VERSION=$(ICEBERG_VERSION) \
		--build-arg NESSIE_VERSION=$(NESSIE_VERSION) \
		-t $(IMAGE_NAME):$(TAG) \
		-f Dockerfile .

build-multi:
	@echo "Building multi-platform image $(IMAGE_NAME):$(TAG) (linux/amd64,linux/arm64) with:"
	@echo "  Spark:   $(SPARK_VERSION)"
	@echo "  Scala:   $(SCALA_BINARY)"
	@echo "  Iceberg: $(ICEBERG_VERSION)"
	@echo "  Nessie:  $(NESSIE_VERSION)"
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--build-arg SPARK_VERSION=$(SPARK_VERSION) \
		--build-arg SCALA_BINARY=$(SCALA_BINARY) \
		--build-arg ICEBERG_VERSION=$(ICEBERG_VERSION) \
		--build-arg NESSIE_VERSION=$(NESSIE_VERSION) \
		-t $(IMAGE_NAME):$(TAG) \
		-f Dockerfile --push .

push:
	docker push $(IMAGE_NAME):$(TAG)
