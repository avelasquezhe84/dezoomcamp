from sqlalchemy import create_engine
from time import time
import pandas as pd

## explore: read first 100 rows from csv and parse dates
# df = pd.read_csv('ny_taxi_csv_data/yellow_tripdata_2021-01.csv', nrows=100)
# df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
# df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
# print(df.head())

## create connection to db
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()
## explore table schema based on type of db
# create_query = pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine)
# print(create_query)

## read whole csv in chunks
## create table in db
# df_iter = pd.read_csv('ny_taxi_csv_data/yellow_tripdata_2021-01.csv', iterator=True, chunksize=10_000)
# df = next(df_iter)
# df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
# df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
# df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

## insert all data in chunks
## https://pandas.pydata.org/docs/user_guide/io.html#iterating-through-files-chunk-by-chunk
with pd.read_csv('ny_taxi_csv_data/yellow_tripdata_2021-01.csv', chunksize=100_000, low_memory=False) as df_iter:
    total_start = time()
    for df in df_iter:
        t_start = time()
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        t_end = time()
        print('inserted chunk, took %.3f seconds...' % (t_end - t_start) )
    total_end = time()
    print('whole process took %.3f seconds...' % (total_end - total_start))
    
## using pyarrow engine: no improvement
# df = pd.read_csv('ny_taxi_csv_data/yellow_tripdata_2021-01.csv', engine='pyarrow')
# t_start = time()
# df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
# df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
# df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
# t_end = time()
# print('inserted data, took %.3f seconds...' % (t_end - t_start))
