```py 
{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "b0b8bddf-4341-4ee5-b558-a5f94066c4c8",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "3.5.5\n"
     ]
    }
   ],
   "source": [
    "import pyspark\n",
    "print(pyspark.__version__)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "106b34ff-8bbe-495d-a338-f714a7372591",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "4ec010a7-0f34-49bc-916c-43c8a5896656",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "Setting default log level to \"WARN\".\n",
      "To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).\n",
      "25/03/06 22:24:33 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable\n"
     ]
    }
   ],
   "source": [
    "spark = SparkSession.builder \\\n",
    "    .master(\"local[*]\") \\\n",
    "    .appName('test') \\\n",
    "    .getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "5d93f952-394d-4943-8f62-382dcf341896",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "--2025-03-06 22:36:15--  http://wget/\n",
      "Resolving wget (wget)... failed: Temporary failure in name resolution.\n",
      "wget: unable to resolve host address ‘wget’\n",
      "--2025-03-06 22:36:15--  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet\n",
      "Resolving d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)... 18.155.128.187, 18.155.128.46, 18.155.128.6, ...\n",
      "Connecting to d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)|18.155.128.187|:443... connected.\n",
      "HTTP request sent, awaiting response... 200 OK\n",
      "Length: 64346071 (61M) [binary/octet-stream]\n",
      "Saving to: ‘yellow_tripdata_2024-10.parquet’\n",
      "\n",
      "yellow_tripdata_202 100%[===================>]  61.36M  74.3MB/s    in 0.8s    \n",
      "\n",
      "2025-03-06 22:36:16 (74.3 MB/s) - ‘yellow_tripdata_2024-10.parquet’ saved [64346071/64346071]\n",
      "\n",
      "FINISHED --2025-03-06 22:36:16--\n",
      "Total wall clock time: 0.9s\n",
      "Downloaded: 1 files, 61M in 0.8s (74.3 MB/s)\n"
     ]
    }
   ],
   "source": [
    "!wget wget https://d37ci6vzurychx.cloudfront.net/trip-data/df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "3a48c20f-928b-405f-be71-08bfbbc8f013",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "df = spark.read.parquet('yellow_tripdata_2024-10.parquet')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "c3cf017c-0861-4506-a78d-3a35fecb8c86",
   "metadata": {},
   "outputs": [],
   "source": [
    "df = df.repartition(4)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "5622f187-7754-47e0-aee4-ae86deb3dfe3",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "df.write.parquet('data/pq/yellow/2024/10/', mode='overwrite')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "a3fce0d0-e2dc-41c5-b214-8755572765ac",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- VendorID: integer (nullable = true)\n",
      " |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)\n",
      " |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)\n",
      " |-- passenger_count: long (nullable = true)\n",
      " |-- trip_distance: double (nullable = true)\n",
      " |-- RatecodeID: long (nullable = true)\n",
      " |-- store_and_fwd_flag: string (nullable = true)\n",
      " |-- PULocationID: integer (nullable = true)\n",
      " |-- DOLocationID: integer (nullable = true)\n",
      " |-- payment_type: long (nullable = true)\n",
      " |-- fare_amount: double (nullable = true)\n",
      " |-- extra: double (nullable = true)\n",
      " |-- mta_tax: double (nullable = true)\n",
      " |-- tip_amount: double (nullable = true)\n",
      " |-- tolls_amount: double (nullable = true)\n",
      " |-- improvement_surcharge: double (nullable = true)\n",
      " |-- total_amount: double (nullable = true)\n",
      " |-- congestion_surcharge: double (nullable = true)\n",
      " |-- Airport_fee: double (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 55,
   "id": "6be591a3-5dfd-481e-abe5-7e56d9d2f6f9",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_yellow = spark.read.parquet('data/pq/yellow/2024/10')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 57,
   "id": "b5b2b9a9-e712-49a5-a214-24b6ba24d207",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_yellow.registerTempTable('yellow')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 59,
   "id": "f21b1adf-67ea-4530-abc2-59949e7a8f25",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- VendorID: integer (nullable = true)\n",
      " |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)\n",
      " |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)\n",
      " |-- passenger_count: long (nullable = true)\n",
      " |-- trip_distance: double (nullable = true)\n",
      " |-- RatecodeID: long (nullable = true)\n",
      " |-- store_and_fwd_flag: string (nullable = true)\n",
      " |-- PULocationID: integer (nullable = true)\n",
      " |-- DOLocationID: integer (nullable = true)\n",
      " |-- payment_type: long (nullable = true)\n",
      " |-- fare_amount: double (nullable = true)\n",
      " |-- extra: double (nullable = true)\n",
      " |-- mta_tax: double (nullable = true)\n",
      " |-- tip_amount: double (nullable = true)\n",
      " |-- tolls_amount: double (nullable = true)\n",
      " |-- improvement_surcharge: double (nullable = true)\n",
      " |-- total_amount: double (nullable = true)\n",
      " |-- congestion_surcharge: double (nullable = true)\n",
      " |-- Airport_fee: double (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df_yellow.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 68,
   "id": "08e8c196-f4ee-4ae5-865a-2020fed526bd",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------+\n",
      "|number_records|\n",
      "+--------------+\n",
      "|        128893|\n",
      "+--------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df_yellow_tripcount = spark.sql(\"\"\"\n",
    "SELECT \n",
    "    COUNT(*) AS number_records\n",
    "FROM\n",
    "    yellow\n",
    "WHERE\n",
    "    tpep_pickup_datetime >= '2024-10-15 00:00:00' \n",
    "    AND tpep_pickup_datetime <= '2024-10-15 23:59:59'\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 82,
   "id": "70e92934-16d6-49c8-b55c-300fbf613a8b",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "[Stage 76:>                                                         (0 + 4) / 4]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------------+\n",
      "|duration_hour|\n",
      "+-------------+\n",
      "|          162|\n",
      "|          143|\n",
      "|          137|\n",
      "|          114|\n",
      "|           89|\n",
      "|           89|\n",
      "|           70|\n",
      "|           67|\n",
      "|           66|\n",
      "|           46|\n",
      "|           42|\n",
      "|           38|\n",
      "|           33|\n",
      "|           26|\n",
      "|           25|\n",
      "|           25|\n",
      "|           24|\n",
      "|           23|\n",
      "|           23|\n",
      "|           23|\n",
      "+-------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "df_yellow_longesttrip = spark.sql(\"\"\"\n",
    "SELECT \n",
    "    TIMESTAMPDIFF(hour, tpep_pickup_datetime, tpep_dropoff_datetime) AS duration_hour\n",
    "FROM\n",
    "    yellow\n",
    "ORDER BY\n",
    "    duration_hour DESC\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 83,
   "id": "f965199a-499d-4ef6-80a8-1bfba0855b45",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "--2025-03-07 00:07:07--  http://wget/\n",
      "Resolving wget (wget)... failed: Temporary failure in name resolution.\n",
      "wget: unable to resolve host address ‘wget’\n",
      "--2025-03-07 00:07:07--  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv\n",
      "Resolving d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)... 18.155.128.6, 18.155.128.222, 18.155.128.187, ...\n",
      "Connecting to d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)|18.155.128.6|:443... connected.\n",
      "HTTP request sent, awaiting response... 200 OK\n",
      "Length: 12331 (12K) [text/csv]\n",
      "Saving to: ‘taxi_zone_lookup.csv.1’\n",
      "\n",
      "taxi_zone_lookup.cs 100%[===================>]  12.04K  --.-KB/s    in 0s      \n",
      "\n",
      "2025-03-07 00:07:08 (106 MB/s) - ‘taxi_zone_lookup.csv.1’ saved [12331/12331]\n",
      "\n",
      "FINISHED --2025-03-07 00:07:08--\n",
      "Total wall clock time: 0.09s\n",
      "Downloaded: 1 files, 12K in 0s (106 MB/s)\n"
     ]
    }
   ],
   "source": [
    "!wget wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 84,
   "id": "aae549cc-4d0e-404e-b7b9-cc40121d0eb4",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_zones = spark.read.parquet('zones/')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 132,
   "id": "ee26044f-3230-4cc0-94f3-e7d76e365409",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+-------------+--------------------+------------+\n",
      "|LocationID|      Borough|                Zone|service_zone|\n",
      "+----------+-------------+--------------------+------------+\n",
      "|         1|          EWR|      Newark Airport|         EWR|\n",
      "|         2|       Queens|         Jamaica Bay|   Boro Zone|\n",
      "|         3|        Bronx|Allerton/Pelham G...|   Boro Zone|\n",
      "|         4|    Manhattan|       Alphabet City| Yellow Zone|\n",
      "|         5|Staten Island|       Arden Heights|   Boro Zone|\n",
      "|         6|Staten Island|Arrochar/Fort Wad...|   Boro Zone|\n",
      "|         7|       Queens|             Astoria|   Boro Zone|\n",
      "|         8|       Queens|        Astoria Park|   Boro Zone|\n",
      "|         9|       Queens|          Auburndale|   Boro Zone|\n",
      "|        10|       Queens|        Baisley Park|   Boro Zone|\n",
      "|        11|     Brooklyn|          Bath Beach|   Boro Zone|\n",
      "|        12|    Manhattan|        Battery Park| Yellow Zone|\n",
      "|        13|    Manhattan|   Battery Park City| Yellow Zone|\n",
      "|        14|     Brooklyn|           Bay Ridge|   Boro Zone|\n",
      "|        15|       Queens|Bay Terrace/Fort ...|   Boro Zone|\n",
      "|        16|       Queens|             Bayside|   Boro Zone|\n",
      "|        17|     Brooklyn|             Bedford|   Boro Zone|\n",
      "|        18|        Bronx|        Bedford Park|   Boro Zone|\n",
      "|        19|       Queens|           Bellerose|   Boro Zone|\n",
      "|        20|        Bronx|             Belmont|   Boro Zone|\n",
      "+----------+-------------+--------------------+------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df_zones.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 138,
   "id": "ce9c3516-3e5d-4538-a623-b844fee10945",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_yellow_loca = spark.sql(\"\"\"\n",
    "SELECT \n",
    "    PULocationID AS zonal,\n",
    "    SUM(total_amount) AS yellow2024_amount,\n",
    "    COUNT(1) AS yellow2024_number_records\n",
    "FROM\n",
    "    yellow\n",
    "GROUP BY\n",
    "    1\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 139,
   "id": "b91d456f-e6e4-4d65-a9fe-dc9a1062e940",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_yellow_loca \\\n",
    "    .repartition(4) \\\n",
    "    .write.parquet('data/report/loca/yellow', mode='overwrite')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 121,
   "id": "79c24ddf-0912-4c96-a35d-5c44b70cc03c",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_yellow_loca = spark.read.parquet('data/report/loca/yellow')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 140,
   "id": "e35dba33-ff19-44e8-a28c-54fae22fe8c6",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_combine_tmp = df_yellow_loca.join(df_zones, df_yellow_loca.zonal == df_zones.LocationID)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 141,
   "id": "f3614a36-ec97-4821-85ea-17bad7d547a4",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_combine_tmp.registerTempTable('yellow24_join_zone')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 142,
   "id": "a4c6b167-f27e-45ff-adb6-2e135e9fc316",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- zonal: integer (nullable = true)\n",
      " |-- yellow2024_amount: double (nullable = true)\n",
      " |-- yellow2024_number_records: long (nullable = false)\n",
      " |-- LocationID: string (nullable = true)\n",
      " |-- Borough: string (nullable = true)\n",
      " |-- Zone: string (nullable = true)\n",
      " |-- service_zone: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df_combine_tmp.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 150,
   "id": "16686ecd-ecec-4387-8da9-58868ac1eefd",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------+\n",
      "|                Zone|  freq|\n",
      "+--------------------+------+\n",
      "|Governor's Island...|     1|\n",
      "|           Homecrest|   263|\n",
      "|              Corona|    36|\n",
      "|    Bensonhurst West|   312|\n",
      "|         Westerleigh|    12|\n",
      "|Charleston/Totten...|     4|\n",
      "|      Newark Airport|   555|\n",
      "|          Douglaston|    74|\n",
      "|      Pelham Parkway|   178|\n",
      "|          Mount Hope|   339|\n",
      "|East Concourse/Co...|   683|\n",
      "|         Marble Hill|    73|\n",
      "|           Rego Park|   471|\n",
      "|Upper East Side S...|191011|\n",
      "|Heartland Village...|     7|\n",
      "|       Dyker Heights|   172|\n",
      "|   Kew Gardens Hills|   245|\n",
      "|       Rikers Island|     2|\n",
      "|             Bayside|   117|\n",
      "|     Jackson Heights|  1760|\n",
      "+--------------------+------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df_least_frequest = spark.sql(\"\"\"\n",
    "SELECT\n",
    "    Zone,\n",
    "    MIN(yellow2024_number_records) AS freq\n",
    "FROM\n",
    "    yellow24_join_zone\n",
    "Group BY\n",
    "    1\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "3d6858a3-6317-46ea-a58b-ad9229b1039b",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.2"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}


```
