# RTV Survey Data Pipeline

A streamlined data pipeline for processing RTV household survey data.

## Setup
```bash
docker-compose up -d
```

## Components
- MinIO for data storage
- PostgreSQL for data warehouse
- Python pipeline for ETL

## Features
- Automated data transformation
- Detailed metrics calculation
- Efficient data loading

## Localhost endpoints
- Dashboard: http://localhost:8501
- MinIO: http://localhost:9001
- PostgreSQL: http://localhost:5432
- Pipeline: http://localhost:8081


## Usage

Login to minio console at http://localhost:9001 with the following credentials:
- Access Key: rtv-test-user
- Secret Key: rtv-test-password

Upload the survey data to the `rtv-survey` bucket.
Create an empty bucket named `rtv-survey-results`.

Access the pipeline at http://localhost:8081 and start a new job.

Click reload button on the pipeline page to see the job status.

Access the dashboard at http://localhost:8501 to see the results.

