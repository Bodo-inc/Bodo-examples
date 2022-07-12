 To find Nessie project url and dremio token -

<Nessie_endpoint_url> -  Go to your Dremio account,  click on Settings -> Project Settings -> Nessie Endpoint
<dremio token> - Click on your Username botton left -> Account Settings -> Personal Access Tokens -> Generate Token ->  copy the value generated. 

While creating a cluster on EMR, Below configu has to be added edit software settings :


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


