![](bodo-gray-green.svg)

# Let's Learn Bodo through Examples!

Welcome to the Bodo Examples Repo! This is where you can find examples to help you get started using Bodo.

Bodo is the next generation big-data processing engine that brings supercomputing-style performance and scalability to native Python and SQL codes automatically. Bodo has several advantages over other big data transformation systems that makes it one of the most performant and cost-effective solutions for large scale data analytics, particularly ETL and ELT.

This repository teaches you to use Bodo effectively through examples. If you know SQL and Python, you already know how to use Bodo, and you don't need any new language or API. You will just `import bodo` and learn some programming tricks to improve your existing applications to save $$$ on compute resources while delivering value in a much shorter time-frame. Benchmarks have shown that Bodo can be [orders of magnitude faster](https://www.bodo.ai/blog/performance-and-cost-of-bodo-vs-spark-dask-ray) than its competitors like Spark.


## How to run these examples?

We recommend that you run these examples on the [Bodo Platform](https://platform.bodo.ai/account/sign-up). You can [sign up for free]((https://platform.bodo.ai/account/sign-up)) to try it out. Some examples like modules 1 to 3 can run on small clusters, e.g., 2 nodes of c5.2xlarge with total of 8 physical cores (16 vCPU) and 32GB RAM, and some examples need larger clusters. The description provided with each example indicates the size of cluster that is required to run it. When you sign up for Bodo, we provide a hosted trial where you can run the examples which only require the 2 node c5.2xlarge cluster for free, upto 30 hours a month. For any other cluster size, you can use Bodo for free on your AWS or Azure account for 14 days after you sign up, and only pay your cloud provider for the resources used. Please [Contact Us](https://www.bodo.ai/contact) if you want a longer trial.

You can also run these examples locally by [installing bodo](https://docs.bodo.ai/latest/installation_and_setup/install/#install)  on your laptop. However, we strongly recommend using the [Bodo Platform](https://platform.bodo.ai) as it provides a notebook environment with all the code available and required packages already installed for you. 

## What if I wanted to test my code with my data?

If you wanted to run your application codes with your own data, please refer to the instructions [here](https://docs.bodo.ai/latest/installation_and_setup/bodo_platform_aws/#setup-iam-role) on how to set up the identity access management, policies, and credentials to integrate your cloud provider with bodo platform. This allows bodo to spin up EC2 instances, create a cluster, and enable you to access your data within your VPC. Everything, including your data stays in your VPC.


## Modules outline


[Modules 1](01-ETL-S3-Operational-Databases) and [2](02-ETL-Snowflake) focus on compute heavy data transformations through ETL applications. You will find examples with operational databases like PostgreSQL, Oracle, MySQL in module 01, a data warehouse like  Snowflake in module 02, and a data Lakehouse example with Iceberg in [module 03](05-ETL-S3-Dremio-Iceberg).

 Modules [04](04-ML-at-Scale) and [05](05-Business-Usecases-at-Scale) contain larger scale examples with [Machine Learning](04-ML-at-Scale), [Business use cases](05-Business-Usecases-at-Scale) (financial, transportation, etc.). And finally, [module 6](06-BodoSDK) contains an example for running jobs using BodoSDK.


This is an open-source repository, so please consider adding your Bodo examples to it! You can contribute by creating a feature branch and submit a pull request for us to review.

