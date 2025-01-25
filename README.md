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
df_green = pd.read_csv('green_tripdata_2019-10.csv', nrows=100) -- confirmed i could read the row.
print(pd.io.sql.get_schema(df_green, name='green_taxi_data')) --confirmed the datetime to be text also.
from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
from time import time
df_doper = pd.read_csv('green_tripdata_2019-10.csv', iterator=True, chunksize=50000) --did 50000 because i could
while True: 
    t_start = time()

    df_green = next(df_doper)

    df_green.lpep_pickup_datetime = pd.to_datetime(df_green.lpep_pickup_datetime)
    df_green.lpep_dropoff_datetime = pd.to_datetime(df_green.lpep_dropoff_datetime)
    
    df_green.to_sql(name='green_taxi_data', con=engine, if_exists='append')

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








