![](bodo-gray-green.svg)

# Let's Learn Bodo through Examples!

Welcome to the Bodo Examples Repo! If you are here, that means you have heard about Bodo's next-generation high performance big data processing engine. This is where you can find examples to help you get started using Bodo. 

Bodo is the next generation compute engine that brings supercomputing-style performance and scalability to native Python and SQL codes automatically. Bodo has several advantages over other big data transformation systems that makes it one of the most performant and cost-effective solutions for large scale data analytics, particularly ETL and ELT. 

As the name suggests, this repository teaches you how to become a professional Bodo programmer through examples. Think of Bodo as SQL and Python, and you don't need any new language or API. You will just `import bodo` and learn some programming tricks to improve your existing applications to save $$$ on compute resources while delivering value in a much shorter time-frame. Benchmarks have shown that Bodo can be [orders of magnitude faster](https://www.bodo.ai/blog/performance-and-cost-of-bodo-vs-spark-dask-ray) than its competitors like Spark. 


## How to run these examples?

We recommend that you run these examples on the [Bodo Platform](https://platform.bodo.ai/account/sign-up). You can [sign up for free]((https://platform.bodo.ai/account/sign-up)) to try it out. Some examples like modules 1 to 6 can run on small clusters, e.g., 2 nodes of c5.2xlarge with total of 8 physical cores (16 vCPU) and 32GB RAM, and some examples need larger clusters. The description provided with each example indicates the size of cluster that is required to run it. When you sign up for Bodo, we provide a hosted trial which lets you run the examples which only require the 2 node c5.2xlarge cluster for free, for 30 hours a month. For any other cluster size, you can use Bodo for free on your AWS or Azure account for 14 days after you sign up, and only pay your cloud provider for the resources used. Please [Contact Us](https://www.bodo.ai/contact) if you want a longer trial.

You can also run these examples locally by [installing bodo](https://docs.bodo.ai/latest/installation_and_setup/install/#install)  on your laptop. However, we strongly recommend using the [Bodo Platform](https://platform.bodo.ai) as it provides a notebook environment with all the code available and required packages already installed for you. 

## What if I wanted to test my code with my data?

If you wanted to run your application codes with your own data, please refer to the instruction [here](https://docs.bodo.ai/latest/installation_and_setup/bodo_platform_aws/#setup-iam-role) on how to set up the identity access management, policies, and credentials to integrate your cloud provider with bodo platform. This allows bodo to spin up EC2 instances, create a cluster, and let you access your data within your VPC. Everything, including your data stays in your VPC.


## Modules outline

You will first learn about the fundamentals of high performance computing using Python and Bodo in [module 01](01-Basics-of-HPC-with-Python-SQL). 

Then you will learn some best practices to write your Bodo code in [module 02](02-Getting-Started-with-Bodo). 

[Modules 3](03-ETL-Data-Lakes-Operational-Databases) and [4](04-ETL-Data-Warehouses-Snowflake) focus on Bodo's sweet spot, i.e. compute heavy data transformations, through ETL applications. You will find examples with operational databases like PostgreSQL, Oracle, MySQL, a data warehouse like Snowflake in module 04, and a data Lakehouse example with Iceberg in [module 05](05-ETL-Data-Lakehouses-Iceberg). 

[Module 06](06-Bodo-Free-Trial) contains some example use-cases. Modules [07](07-Machine-Learning-at-Scale) and [08](08-Business-Usecases-at-Scale) contain larger scale examples with [Machine Learning](07-Machine-Learning-at-Scale), [Business use cases](08-Business-Usecases-at-Scale) (financial, transportation, etc.). [Module 09](09-Compare-Bodo-with-Spark-Dask-Ray) demonstrates a head-to-head competition with Spark, Dask and Ray. And finally, [module 10](10-Advanced) contains more advanced topics like deploying Bodo applications in Kubernetes, containers, Streamlit, or on your cloud provider through Terraform (AWS and Azure). 


This is an open-source repository. You can contribute by creating a feature branch and submit a pull request. We will review and merge your PR!

