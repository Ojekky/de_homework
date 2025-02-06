1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?
- 128.3 MB

Answer was seen in the bucket on GCP.

2) What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?
- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` 

Baseds on our flow used on Kestra.

3) How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?

- 24,648,499

ANSWER: SELECT COUNT(*) FROM `global-rookery-448215-m8.zoomcamp.yellow_tripdata` WHERE EXTRACT (YEAR FROM tpep_pickup_datetime) = 2020

4) How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?

- 1,734,051 ---- I got 1734039

Answer: SELECT COUNT(*) FROM `global-rookery-448215-m8.zoomcamp.green_tripdata` WHERE EXTRACT (YEAR FROM lpep_pickup_datetime) = 2020 

5) How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?

- 1,925,152 -----1925130

SELECT COUNT(*) FROM `global-rookery-448215-m8.zoomcamp.yellow_tripdata` WHERE EXTRACT (MONTH FROM tpep_pickup_datetime) = 3 AND EXTRACT (YEAR FROM tpep_pickup_datetime) = 2021


6) How would you configure the timezone to New York in a Schedule trigger?

- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration

thats my answer
