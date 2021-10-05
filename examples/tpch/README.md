# TPC-H Queries 

TPC-H is a benchmark for business oriented ad-hoc queries that is used to simulate real questions and is usually used to benchmark performance of database tools for answering them.

More information can be found [here](http://www.tpc.org/tpch/)

## Generating TPC-H Data in Parquet Format

### 1. Download and Install tpch-dbgen

```
    git clone https://github.com/Bodo-inc/tpch-dbgen
    cd tpch-dbgen
    make
    cd ../
```

### 2. Generate Data


Usage

```
usage: python generate_data_pq.py [-h] --folder FOLDER [--SF N] [--validate_dataset]

    -h, --help       Show this help message and exit
    folder FOLDER: output folder name (can be local folder or S3 bucket)
    SF N: data size number in GB (Default 1)
    validate_dataset: Validate each parquet dataset with pyarrow.parquet.ParquetDataset (Default True)
```
Example:

Generate 1GB data locally: 

`python generate_data_pq.py --SF 1 --folder SF1`

Generate 1TB data and upload to S3 bucket: 

`python generate_data_pq.py --SF 1000 --folder s3://bucket-name/`

NOTES:

- Script assumes `tpch-dbgen` is in the same directory. If you downloaded it in another location, make sure to update `tpch_dbgen_location` in the script with new location.

- If using S3 bucket, install `s3fs` and add your AWS credentials.


## Bodo

### Installation

Follow the intstructions [here](https://docs.bodo.ai/dev/source/install.html).

### Running queries

Use 

`mpiexec -n N python bodo_queries.py --folder folder_path`

```
usage: python bodo_queries.py [-h] --folder FOLDER

arguments:
  -h, --help       Show this help message and exit
  --folder FOLDER  The folder containing TPCH data

```
Example:

Run with 4 cores on a local data

`mpiexec -n 4 python bodo_queries.py --folder SF1`

Run with 288 cores on S3 bucket data

`mpiexec -n 288 python bodo_queries.py --folder s3://bucket-name/`

## Spark

### Installation

Here, we show instructions for using PySpark with EMR cluster. 

For other cluster configurations, please follow corresponding vendor's instructions.

Follow steps outlined in "Launch an Amazon EMR cluster" section  of [AWS guide](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-launch-sample-cluster.html)

In **Software configuration** step, make sure `Hadoop`, `Hive`, `JupyterEnterpriseGateway`, and `Spark` are selected.

In **Cluster Nodes and Instances** step, choose same instance type for both master and workers. Don't create any task instances. 

### Running queries

Attach [pyspark\_notebook.ipynb](./pyspark_notebook.ipynb) to your EMR cluster following the examples on [AWS documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-notebooks-create.html)

