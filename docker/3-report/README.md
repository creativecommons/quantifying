# GCS Report Container

## Prerequisites
- Fetch container has been run to generate input data
- Process container has been run to process the data

## Build
```bash
# From project root
docker build -t gcs-report:latest -f docker/3-report/Dockerfile.report .
```

## Run Pipeline
```bash
# 1. Run fetch container first
docker run --rm \
  --dns 8.8.8.8 \
  -e GCS_DEVELOPER_KEY="your-key" \
  -e GCS_CX="your-cx-id" \
  -v "$(pwd)/data:/app/data" \
  gcs-fetch:latest

# 2. Run process container
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/.env/:/app/.env" \
  gcs-process:latest

# 3. Run report container
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/.env/:/app/.env" \
  gcs-report:latest
```

## Using docker-compose
```bash
# From project root, with docker-compose.yml
docker-compose --profile report up gcs-report
```

## Data Flow
- Reads processed data from: `data/YYYYQQ/2-process/`
- Generates reports in: `data/YYYYQQ/3-report/`
