import pandas as pd
import bodo

@bodo.jit()
def read_parquet():
    df = pd.read_parquet("s3://bodo-example-data/nyc-taxi/yellow_tripdata_2019_half.pq")
    df.to_parquet("yellow_tripdata_2019_half.pq")
    print(len(df))
    print(df.shape)
    df = df.iloc[::2]
    # df = bodo.rebalance(df)
    df.to_parquet("sample_data.pq")
    print(df.shape)
    print(len(df))


read_parquet()

