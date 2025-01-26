# de_homework
Homework for my DE Course

## Question 1. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.
$ python --version
Python 3.12.8

What's the version of `pip` in the image?

**Answer**
pip                       24.3.1



## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

**Answer**
The host name is postgres as seen in the image: postgres:17-alpine on the db. Also, the host port is 5433.



##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

**Answer**
I used the code from the course and used Jupyter notebook.
I first ran the pgcli 

```bash
  pgcli -h localhost -p 5432 -u root -d ny_taxi
Password for root:
Server: PostgreSQL 13.18 (Debian 13.18-1.pgdg120+1)
Version: 4.1.0
Home: http://pgcli.com
root@localhost:ny_taxi> \dt
+--------+-------------------+-------+-------+
| Schema | Name              | Type  | Owner |
|--------+-------------------+-------+-------|
| public | green_taxi_data   | table | root  |
| public | green_taxi_drive  | table | root  |
| public | green_taxi_trips  | table | root  |
| public | yellow_taxi_trips | table | root  |
| public | zones             | table | root  |
+--------+-------------------+-------+-------+
SELECT 2
Time: 0.264s
```
Also downloaded the table using wget
```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```
Then entered notebook on bash $ jupyter notebook
```jupyter
import pandas as pd
df_greene = pd.read_csv('green_tripdata_2019-10.csv')
len(df_greene)
from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
from time import time
df_dop = pd.read_csv('green_tripdata_2019-10.csv', iterator=True, chunksize=100000)
while True: 
    t_start = time()

    df_greene = next(df_dop)

    df_greene.lpep_pickup_datetime = pd.to_datetime(df_greene.lpep_pickup_datetime)
    df_greene.lpep_dropoff_datetime = pd.to_datetime(df_greene.lpep_dropoff_datetime)
    
    df_greene.to_sql(name='green_taxi_drive', con=engine, if_exists='append')

    t_end = time()

    print('inserted another chunk, took %.3f second' % (t_end - t_start))
```
Opened my pgadmin and refreshed my schemas and the green_tripdata_2019-10.csv.gz is loaded as the green_taxi_data on pgadmin.

--------------------------------------------------------------------------------------------
```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.

**Answer**
This was already a part of my pgadmin database as followed during the course.


## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles

**Answer**
```SQL
1. SELECT
     COUNT(lpep_pickup_datetime) AS trip_count
 FROM
     green_taxi_drive
 WHERE
     lpep_pickup_datetime >= '2019-10-01' AND
     lpep_dropoff_datetime < '2019-11-01' AND trip_distance <= 1;
2. SELECT
     COUNT(lpep_pickup_datetime) AS trip_count
 FROM
     green_taxi_drive
 WHERE
     lpep_pickup_datetime >= '2019-10-01' 
	 AND lpep_dropoff_datetime < '2019-11-01' 
	 AND trip_distance > 1
	 AND trip_distance <= 3;
3. SELECT
     COUNT(lpep_pickup_datetime) AS trip_count
 FROM
     green_taxi_drive
 WHERE
     lpep_pickup_datetime >= '2019-10-01' 
	 AND lpep_dropoff_datetime < '2019-11-01' 
	 AND trip_distance > 3
	 AND trip_distance <= 7;
4. SELECT
     COUNT(lpep_pickup_datetime) AS trip_count
 FROM
     green_taxi_drive
 WHERE
     lpep_pickup_datetime >= '2019-10-01' 
	 AND lpep_dropoff_datetime < '2019-11-01' 
	 AND trip_distance > 7
	 AND trip_distance <= 10;
5.SELECT
     COUNT(lpep_pickup_datetime) AS trip_count
 FROM
     green_taxi_drive
 WHERE
     lpep_pickup_datetime >= '2019-10-01' 
	 AND lpep_dropoff_datetime < '2019-11-01' 
	 AND trip_distance > 10;
```
-----------
## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

**Answer**
Using the code below the answer is 2019-10-31 23:23:41
```SQL
SELECT
	lpep_pickup_datetime,
	lpep_dropoff_datetime,
	Max(trip_distance) as distance
FROM
	green_taxi_data t
GROUP BY
	1, 2
ORDER BY
	"distance" DESC;
```

## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.





