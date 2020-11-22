# Airflow pipeline for machinelearnig
how to build a efficient a big data pipeline for machine learning projects: Airflow, aws s3, Spark, aws Redshift, Jupyter/Spark ML


## what are the requirements
Let's say we have mutliples sources of data (files, events, logs, relational databases, documents database,...) first we need to collect, clean, aggregate and store it in a system,for example a datawarehouse. Once the data is stored, we can process the data in Jupyter, using Spark framework. <br> Then we can build a dashboard in Jupyter to visualize the the results of the ML calculations.<br>
Here i will handle the case when the data sources are in the form of csv files, but it is easy to update the solution if the data sources types are different(json, avro files, or if the souce is a database). The entry point is aws S3, as shown on the scheme below

![alt text](https://github.com/sparktacusdemo/demo1_airflow_pipeline_for_machinelearnig/blob/main/1.png)

## 
