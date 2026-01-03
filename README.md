# Spark-Iceberg-Nessie Publisher

This project provides a robust setup for building and publishing a customized Apache Spark Docker image pre-configured with Apache Iceberg and Project Nessie.

## Project Structure

```text
.
├── .github/workflows/
│   └── publish-spark.yml    # GitHub Action for multi-platform build/publish
└── docker/
    └── spark/
        ├── Dockerfile       # Spark + Iceberg + Nessie image definition
        ├── Makefile         # Build and publish automation
        ├── pom.xml          # Version management for dependencies
        └── build-image.sh   # Utility script for building locally
```

## Features

- **Multi-platform support**: Build and push images for both `amd64` and `arm64` using Docker Buildx.
- **Dependency management**: Easily update Spark, Iceberg, and Nessie versions in `pom.xml`.
- **Pre-configured shells**: Includes wrapper scripts (`spark-sql-iceberg-nessie-shell`, etc.) with built-in configurations for Nessie and S3 (MinIO).
- **Automated CI/CD**: Seamlessly publish to Docker Hub via GitHub Actions.

## Getting Started

### Local Build

To build the image locally for your current platform:

```bash
cd docker/spark
make build
```

### Multi-platform Build

To build and push for multiple platforms:

```bash
cd docker/spark
make build-multi
```

## GitHub Actions Configuration

To enable automated publishing to Docker Hub:

1.  Push your code to a GitHub repository.
2.  Add the following secrets to your repository (`Settings > Secrets and variables > Actions`):
    - `DOCKERHUB_USERNAME`: Your Docker Hub username.
    - `DOCKERHUB_TOKEN`: A Personal Access Token from Docker Hub.
3.  On every push to `main` affecting the `docker/spark` directory, the `Publish Spark Image` workflow will run.
4.  Pushing a tag starting with `v` (e.g., `v1.0.0`) will also trigger a release.

## Customization

You can update the versions of Spark, Scala, Iceberg, and Nessie in `docker/spark/pom.xml`:

```xml
<properties>
  <spark.version>3.5.2</spark.version>
  <scala.binary>2.12</scala.binary>
  <iceberg.version>1.10.1</iceberg.version>
  <nessie.version>0.106.0</nessie.version>
</properties>
```
