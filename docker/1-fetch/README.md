# GCS fetch Container

## Building the image must be done from the project root directory

```bash
docker build -t gcs-fetch:latest -f docker/1-fetch/Dockerfile.fetch .
```
## Run the container also from the projects root directory

```bash
# replace the variables with appropriate values
# the  --dns 8.8.8.8 flag overides docker DNS resolution and allows GCS fetch script make HTTP requests to Google's API. Using public DNS 8.8.8.8 ensures these requests resolve properly
docker run --rm \
   --dns 8.8.8.8 \
   -v "$(pwd)/data:/app/data" \
   -v "$(pwd)/.env:/app/.env" \
   gcs-fetch:latest --enable-save
```

## using docker-compose with simplified command

```bash
# run from the project root directory
docker-compose --profile fetch up gcs-fetch
```

Note: The container runs a batch job to fetch data using Google custom Search API, process and reports on data gathered.
