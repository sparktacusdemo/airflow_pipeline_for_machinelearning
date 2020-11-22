from __future__ import print_function

#import time
#from builtins import range
#from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import boto3


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'conn_id':'conn_aws_251490606181',
    #'conn_id': 'my_spark_standalone',
    #'packages': 'com.amazon.redshift:redshift-jdbc42-no-awssdk:1.2.45.1069,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-auth:2.7.4,org.apache.hadoop:hadoop-common:2.7.4,com.google.code.findbugs:jsr305:3.0.2,asm:asm:3.2,org.slf4j:slf4j-api:1.7.30,org.xerial.snappy:snappy-java:1.1.7.5,org.slf4j:slf4j-log4j12:1.7.30,org.apache.hadoop:hadoop-aws:2.7.3'
}

mydag = DAG(
    dag_id='dag_aws_s3_pyspark',
    default_args=args,
    schedule_interval=None,
    tags=['aws_s3_pyspark']
)

#----callable functions 
def load_s3(bucket_name,source_file_path,dest_aws_file_name,**kwargs):#bucket_name,source_file_path,dest_aws_file_name):
    s3  = boto3.resource('s3')
    s3.Bucket(bucket_name).upload_file(source_file_path,dest_aws_file_name)


#---------------------------------
#tasks
task1 = PythonOperator(
    task_id='load_s3_1',
    python_callable=load_s3,
    op_kwargs={'bucket_name': 'mybucket-test2', 'source_file_path': 'source_data/purchases_per_user.csv', 'dest_aws_file_name':'purchases/purchases_per_user.csv'},
    dag=mydag,
)

task2 = PythonOperator(
    task_id='load_s3_2',
    python_callable=load_s3,
    op_kwargs={'bucket_name': 'mybucket-test2', 'source_file_path': 'source_data/user_info.csv', 'dest_aws_file_name':'users_info/user_info.csv'},
    dag=mydag,
)

task3 = PythonOperator(
    task_id='load_s3_3',
    python_callable=load_s3,
    op_kwargs={'bucket_name': 'mybucket-test2', 'source_file_path': 'source_data/prices_1.csv', 'dest_aws_file_name':'prices/prices_1.csv'},
    dag=mydag,
)

task4 = PythonOperator(
    task_id='load_s3_4',
    python_callable=load_s3,
    op_kwargs={'bucket_name': 'mybucket-test2', 'source_file_path': 'source_data/sales_per_user.csv', 'dest_aws_file_name':'sales/sales_per_user.csv'},
    dag=mydag,
)

task5 = SparkSubmitOperator(
    task_id='task_aws_s3_pyspark',
    application='s3redshift.py',
    dag=mydag,
    packages='com.amazon.redshift:redshift-jdbc42-no-awssdk:1.2.45.1069,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-auth:2.7.4,org.apache.hadoop:hadoop-common:2.7.4,com.google.code.findbugs:jsr305:3.0.2,asm:asm:3.2,org.slf4j:slf4j-api:1.7.30,org.xerial.snappy:snappy-java:1.1.7.5,org.slf4j:slf4j-log4j12:1.7.30,org.apache.hadoop:hadoop-aws:2.7.3',
    conn_id= 'my_spark_standalone'
)

#----dependencies

[task1,task2,task3,task4] >> task5
