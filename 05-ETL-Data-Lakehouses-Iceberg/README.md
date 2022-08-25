# Data Lakehouses: Iceberg
This repository shows you how to leverage bodo to carry out your ETL jobs with a Data Lakehouse like Iceberg. 

We used Dremio to create and run these examples. Bodo is used as the compute-engine, and Dremio's Arctic is used for meta data storage. The actual files are on AWS S3. 

To use Dremio, you need to set up a Nessie project URL and a dremio token. Follow these steps to set them up:

`Nessie_endpoint_url:`
1. Go to your Dremio account
2. click on Settings -> Project Settings -> Nessie Endpoint

`dremio token:`
   
1. Click on your Username botton left 
2. Account Settings -> Personal Access Tokens -> Generate Token ->  copy the value generated. 


You can write your data to dremio using Spark. We used Spark on EMR to write TPC-H data (i.e, SF1, SF10, SF100, SF1000)

While creating a cluster on AWS EMR, the following configurations has to be added to bootstrap step:

```python 
[
{
"classification":"spark-defaults",
"properties":{
"spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.1,software.amazon.awssdk:bundle:2.15.40,software.amazon.awssdk:url-connection-client:2.15.40,org.projectnessie:nessie-spark-3.2-extensions:0.30.0",
"spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
"spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
"spark.sql.catalog.nessie.uri":”<Nessie_endpoint_url>”,
"spark.sql.catalog.nessie.warehouse": "s3://bodo-iceberg-test2/arctic_test",
"spark.sql.catalog.nessie.ref": "main",
"spark.sql.catalog.nessie.authentication.type": "BEARER",
"spark.sql.catalog.nessie.authentication.token": "<dremio token>",
"spark.sql.catalog.nessie.cache-enabled": "false",
"spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
"spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
"spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSpark32SessionExtensions",
"spark.sql.execution.pyarrow.enabled": "true"
}
}
]
```

