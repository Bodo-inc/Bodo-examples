# TPC-H Queries 

TPC-H is a benchmark suite for business-oriented ad-hoc queries that are used to simulate real questions and is usually used to benchmark the performance of database tools for answering them.

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

This script assumes `tpch-dbgen` is in the same directory. If you downloaded it at another location, make sure to update `tpch_dbgen_location` in the script with the new location.

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

Here, we show the instructions for using PySpark with an EMR cluster. 

For other cluster configurations, please follow corresponding vendor's instructions.

Follow the steps outlined in the "Launch an Amazon EMR cluster" section of the [AWS guide](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-launch-sample-cluster.html)

In the **Software configuration** step, select `Hadoop`, `Hive`, `JupyterEnterpriseGateway`, and `Spark`.

In the **Cluster Nodes and Instances** step, choose the same instance type for both master and workers. Don't create any task instances. 

### Running queries

Attach [pyspark\_notebook.ipynb](./pyspark_notebook.ipynb) to your EMR cluster following the examples in the [AWS documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-notebooks-create.html)

## Dask

### Installation

To install dask, dask.distributed and dask-mpi, run

```
conda install dask -c conda-forge
conda install dask-mpi -c conda-forge
```

### Spawn the scheduler


Spawn the scheduler using dask-mpi following the examples [here](https://mpi.dask.org/en/latest/) and more details [here](https://docs.dask.org/en/latest/how-to/deploy-dask/hpc.html).

Note: 
1. Because Dask scheduler takes up one process, we opted to spawn one extra process than the total number of physical cores to get the same number of cores used for computation. 
2. We found the optimal number of threads correspond to the number of vCPU each core has. 
3. When running with mpi processes, --no-nanny is required to prevent forking other processes. see here for more detail.

Creating a 8 cores cluster on local machine

`mpiexec -n 9 dask-mpi --scheduler-file [Path]/scheduler.json --no-nanny --nthreads 2`

Creating a 288 cores cluster 

`mpiexec -n 289 -f [Path]/machinefile dask-mpi --scheduler-file [Path]/scheduler.json --no-nanny --nthreads 2`

### Running queries

Create a new terminal, then run the following commands. Note that mpiexec is not needed here as the distribution is handled by creating a client that connects to the scheduler file.

Using data from local

`python dask_queries.py --folder SF1 --scheduler-file [Path]/scheduler.json --worker 8`

Using data from S3

`python dask_queries.py --folder s3://[bucket-name]/SF100 --scheduler-file [Path]/scheduler.json --worker 288`
 


