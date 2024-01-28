from sqlalchemy import create_engine
from dask import dataframe as dd
from time import time
import pandas as pd

## timing decorator
def time_it(f):
    def w(*args, **kwargs):
        total_start = time()
        f(*args, **kwargs)
        total_end = time()
        print('whole process took %.3f seconds...' % (total_end - total_start))
    return w
        
## explore: read first 100 rows from csv and parse dates
def explore():
    df = pd.read_csv('ny_taxi_csv_data/yellow_tripdata_2021-01.csv', nrows=100)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(df.head())

## create connection to db
def engine():
    engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    return engine
    # create_query = pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine)
    # print(create_query)
    # explore table schema based on type of db

## create table in db
def create_table(engine):
    df_iter = pd.read_csv('ny_taxi_csv_data/yellow_tripdata_2021-01.csv', iterator=True, chunksize=10_000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

## insert all data in chunks: ~120 seconds
## https://pandas.pydata.org/docs/user_guide/io.html#iterating-through-files-chunk-by-chunk
@time_it
def insert_in_chunks(engine):
    with pd.read_csv('ny_taxi_csv_data/yellow_tripdata_2021-01.csv', chunksize=100_000, low_memory=False) as df_iter:
        for df in df_iter:
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    
## using pyarrow engine: ~140 seconds
@time_it
def insert_with_pyarrow(engine):
    df = pd.read_csv('ny_taxi_csv_data/yellow_tripdata_2021-01.csv', engine='pyarrow')
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

## using dask: ~150 seconds
@time_it
def insert_with_dask():
    df = dd.read_csv('ny_taxi_csv_data/yellow_tripdata_2021-01.csv', assume_missing=True, low_memory=False)
    df.tpep_pickup_datetime = dd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = dd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name='yellow_taxi_data', uri='postgresql://root:root@localhost:5432/ny_taxi', if_exists='replace', parallel=True)
    
def main():
    engine = engine()
    insert_in_chunks(engine)    

if __name__ == '__main__':
    main()