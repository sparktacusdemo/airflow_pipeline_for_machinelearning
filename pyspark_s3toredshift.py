from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

#-------------------------------------------------------------------------------
if __name__ == "__main__":
    
    #configure pyspark environment
    myconf = (SparkConf()\
        .setMaster("spark://<master_ip_instance>:7077")\
        .setAppName("pysparks3toredshift")\
        .set("spark.executor.memory","2g"))

    myconf.set("spark.driver.memory","1g")
    #set the jar packages to include on the Spark driver and executor classpaths
    myconf.set("spark.jars.packages","com.amazon.redshift:redshift-jdbc42-no-awssdk:1.2.45.1069,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-auth:2.7.4,org.apache.hadoop:hadoop-common:2.7.4,com.google.code.findbugs:jsr305:3.0.2,asm:asm:3.2,org.slf4j:slf4j-api:1.7.30,org.xerial.snappy:snappy-java:1.1.7.5,org.slf4j:slf4j-log4j12:1.7.30,org.apache.hadoop:hadoop-aws:2.7.3")
    myconf.set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    myconf.set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    
    #set up SparkContext
    sc = SparkContext(conf = myconf)
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    
    #Redshift JDBC driver requires hadoop configuration (S3a method)
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "xxx")
    hadoopConf.set("fs.s3a.secret.key", "xxxxx")
    hadoopConf.set("fs.s3a.endpoint", "xxxxxx.amazonaws.com")
    hadoopConf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoopConf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

    #set up SparkSession
    spark_session = SparkSession(sc)
    
    #read s3 data as dataframe
    df = spark_session.read.option("header",True).csv("s3a://mybucket-test2/users_info/user_info.csv")
    df2 = spark_session.read.option("header",True).csv("s3a://mybucket-test2/sales/sales_per_user.csv")
    df3= spark_session.read.option("header",True).csv("s3a://mybucket-test2/purchases/")
    df4= spark_session.read.option("header",True).csv("s3a://mybucket-test2/prices/prices_1.csv")
    
    df.createOrReplaceTempView("dftemp")
    df2.createOrReplaceTempView("df2temp")
    df3.createOrReplaceTempView("df3temp")
    df4.createOrReplaceTempView("df4temp")
    
    #aggregate
    df5 = spark_session.sql("SELECT A.client_id, A.username, A.city, A.country, A.phone, IFNull(B.sales,0) AS sales, IFNull(C.purchases,0) AS purchase FROM dftemp A LEFT JOIN df2temp B ON A.client_id = B.client_id LEFT JOIN df3temp C ON A.client_id = C.client_id")

    df5.createOrReplaceTempView("df5temp")

    df_join = spark_session.sql("SELECT A.client_id, A.username, A.city, A.country, A.phone, A.sales, A.purchase, (B.price * A.purchase) AS cost, B.currency FROM df5temp A LEFT JOIN df4temp B ON A.country=B.country")
    
    #write data to redshift
    df_join.write \
        .format("jdbc") \
        .option("url","jdbc:redshift://xxxxxx.redshift.amazonaws.com:5439/mydatabase") \
        .option("dbtable","myredshiftschema.myredshifttable00") \
        .option("UID","xxxx") \
        .option("PWD","xxxxx") \
        .option("driver","com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()
