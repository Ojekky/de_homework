Used Kestra to add the parquet files to big query.
```yaml
id: new_addition
namespace: zoomcamp
description: |
  The parquet Data used in the course: https://d37ci6vzurychx.cloudfront.net/trip-data/

inputs:
  - id: taxi
    type: SELECT
    displayName: Select taxi type
    values: [yellow, green]
    defaults: yellow

  - id: year
    type: SELECT
    displayName: Select year
    values: ["2019", "2024"]
    defaults: "2024"
    allowCustomValue: true

  - id: month
    type: SELECT
    displayName: Select month
    values: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    defaults: "01"

variables:
  file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.parquet"
  gcs_file: "gs://{{kv('GCP_BUCKET_NAME')}}/{{vars.file}}"
  source_url: "https://d37ci6vzurychx.cloudfront.net/trip-data/{{vars.file}}"

tasks:
  - id: set_label
    type: io.kestra.plugin.core.execution.Labels
    labels:
      file: "{{render(vars.file)}}"
      taxi: "{{inputs.taxi}}"

  - id: debug_vars
    type: io.kestra.core.tasks.log.Log
    message: |
      File: {{render(vars.file)}}
      Source URL: {{render(vars.source_url)}}
      GCS Path: {{render(vars.gcs_file)}}

  - id: download_file
    type: io.kestra.plugin.core.http.Download
    uri: "{{render(vars.source_url)}}"

  - id: upload_to_gcs
    type: io.kestra.plugin.gcp.gcs.Upload
    from: "{{outputs.download_file.uri}}"
    to: "{{render(vars.gcs_file)}}"

pluginDefaults:
  - type: io.kestra.plugin.gcp
    values:
      serviceAccount: "{{kv('GCP_CREDS')}}"
      projectId: "{{kv('GCP_PROJECT_ID')}}"
      location: "{{kv('GCP_LOCATION')}}"
      bucket: "{{kv('GCP_BUCKET_NAME')}}"

```

Afer it was uploaded, used SQL to answer the question.

``` SQL
-- Created the Parquet Table
CREATE OR REPLACE EXTERNAL TABLE `global-rookery-448215-m8.zoomcamp.external_yellow_tripdata_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://global-rookery-448215-m8-anotherone/yellow_tripdata_2024-*.parquet']
);

-- Create a non partitioned table from external parquet table
CREATE OR REPLACE TABLE global-rookery-448215-m8.zoomcamp.yellow_tripdata_parquet_non_partitioned AS
SELECT * FROM global-rookery-448215-m8.zoomcamp.external_yellow_tripdata_parquet;

--Count for the parquet file
SELECT COUNT(*)
FROM global-rookery-448215-m8.zoomcamp.external_yellow_tripdata_parquet;

-- count the distinct number of PULocationIDs for external
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_location_ids
FROM global-rookery-448215-m8.zoomcamp.external_yellow_tripdata_parquet;

-- count the distinct number of PULocationIDs for the non partitioned
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_location_ids
FROM global-rookery-448215-m8.zoomcamp.yellow_tripdata_parquet_non_partitioned;

-- Retrive the PU Location
SELECT PULocationID
FROM global-rookery-448215-m8.zoomcamp.yellow_tripdata_parquet_non_partitioned;

-- Retrive the PU Location and the DOLocationID
SELECT PULocationID, DOLocationID
FROM global-rookery-448215-m8.zoomcamp.yellow_tripdata_parquet_non_partitioned;

--The records of fare amount
SELECT COUNT(*)
FROM global-rookery-448215-m8.zoomcamp.external_yellow_tripdata_parquet
WHERE fare_amount = 0;

-- partition table
CREATE OR REPLACE TABLE global-rookery-448215-m8.zoomcamp.yellow_tripdata_parquet_partitioned
PARTITION BY DATE(tpep_dropoff_datetime) AS
SELECT * FROM global-rookery-448215-m8.zoomcamp.external_yellow_tripdata_parquet;

-- Impact of partition
SELECT DISTINCT(VendorID)
FROM global-rookery-448215-m8.zoomcamp.yellow_tripdata_parquet_non_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Impact of partition
SELECT DISTINCT(VendorID)
FROM global-rookery-448215-m8.zoomcamp.yellow_tripdata_parquet_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- the request.
SELECT count(*)
FROM global-rookery-448215-m8.zoomcamp.yellow_tripdata_parquet_partitioned
```

