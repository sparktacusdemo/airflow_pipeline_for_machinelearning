# Airflow pipeline for machinelearnig
how to build a efficient a big data pipeline for machine learning projects: Airflow, aws s3, Spark, aws Redshift, Jupyter/Spark ML


## what are the requirements:
Let's say we have mutliples sources of data (files, events, logs, relational databases, documents database,...) first we need to collect, clean, aggregate and store it in a system,for example a datawarehouse. Once the data is stored, we can process the data in Jupyter, using Spark framework. <br> Then we can build a dashboard in Jupyter to visualize the the results of the ML calculations.<br>
Here i will handle the case when the data sources are in the form of csv files, but it is easy to update the solution if the data sources types are different(json, avro files, or if the souce is a database). The entry point is aws S3, as shown on the scheme below

![alt text](https://github.com/sparktacusdemo/demo1_airflow_pipeline_for_machinelearnig/blob/main/1.png)

The main challenge is to build a efficient pipeline to aggregate, clean the data ingested, and store it in the datawarehouse; Airflow will help to build such a pipeline.

## What are the tools

Folowing tools are required: 
<br>
- Storage systems: aws S3, aws Redshift
- Programming script: python (2.7, 3), Pyspark
- packages: Spark aws S3 driver (hadoop), aws Redshift JDBC driver (read, write Redshift data in Spark environment). This presentation is a demo, so we will use ![Boto3](https://github.com/boto/boto3) python package to transfer the data, from sources point to aws s3; instead of tools such Kafka, Spark Streaming.
- Framework: Pyspark, Airflow, Jupyter
- OS: linux ubuntu focal 20.04

About how to to set up a Redshift cluster in a VPC, see my other use case ![right here](https://github.com/sparktacusdemo/redshift_and_vpc): the access to a redshift is a important point, because you can read or write data in the cluster if it runs outside a VPC, and configuration is not done to allow access from outside the VPC (clients).

## Action planning

- set up the Airflow dag for the pipeline: collect the data from s3, aggregate and store in Redshift
- set up and configure Jupyter: Pyspark and Redshift JDBC Driver (Jupyter/Redshift connector)
- build a Spark ML pipeline in Jupyter, to process the data


## Airflow dag pipeline

In the Airflow dag, 2 type of tasks are provided:
- type 1: the tasks to collect the data and store in the aws s3 buckets
- type 2: the task to aggregate, transform the data, and store in Redshift datawarehouse

###### Airflow task: Collect and store in aws s3 

Here, i use a PythonOperator.<br>

example:

```
#----callable functions 
def load_s3(bucket_name,source_file_path,dest_aws_file_name,**kwargs)
    s3  = boto3.resource('s3')
    s3.Bucket(bucket_name).upload_file(source_file_path,dest_aws_file_name)

#tasks
task1 = PythonOperator(
    task_id='load_s3_1',
    python_callable=load_s3,
    op_kwargs={'bucket_name': 'mybucket-test2', 'source_file_path': 'source_data/purchases_per_user.csv', 'dest_aws_file_name':'purchases/purchases_per_user.csv'},
    dag=mydag,
)
```
A callable function is implemented, and this function is called by the task. This function use the Boto3 package to handle the data transfer. Notice the way the arguments are eclared in the function and how they are passed into the task, through 'op_kwargs' parameter. The function requires3 arguments:'bucket_name' the aws s3 bucket where the data must be stored, 'source_file_path' : the data source path, 'dest_aws_file_name':the aws object name.<br>

![alt text](https://github.com/sparktacusdemo/demo1_airflow_pipeline_for_machinelearning/blob/main/2.png)

##### Airflow task: Aggregate and store in Redshift

To complete this task, i build a pyspark application, described ![here](https://github.com/sparktacusdemo/demo1_airflow_pipeline_for_machinelearning/blob/main/pyspark_s3toredshift.py).

In Airflow, the task is implemented as following: we use a SparkSubmit operator

```
task5 = SparkSubmitOperator(
    task_id='task_pyspark_s3toredshift',
    application='pyspark_s3toredshift.py',
    dag=mydag,
    packages='com.amazon.redshift:redshift-jdbc42-no-awssdk:1.2.45.1069,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-auth:2.7.4,org.apache.hadoop:hadoop-common:2.7.4,com.google.code.findbugs:jsr305:3.0.2,asm:asm:3.2,org.slf4j:slf4j-api:1.7.30,org.xerial.snappy:snappy-java:1.1.7.5,org.slf4j:slf4j-log4j12:1.7.30,org.apache.hadoop:hadoop-aws:2.7.3',
    conn_id= 'spark_standalone_airflow_connection'
)
```
Notice the important points:
- use the task parameter 'application' to call the pyspark application
- use the 'conn_id' parameter to set the connection Airflow/Spark cluster (the connection is created in airflow UI (port 8080))
- provide the jar packages through 'packages' task parameter

the complete Airflow pipeline dag is available ![here](https://github.com/sparktacusdemo/demo1_airflow_pipeline_for_machinelearning/blob/main/pipeline_dag.py)


#### Jupyter notebook


