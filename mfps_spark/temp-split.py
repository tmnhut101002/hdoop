from pyspark.sql import SparkSession

conf = SparkConf()\
    .setAppName("MFPS")\
    .set("spark.executor.instances", "10")\
    .set("spark.executor.cores", "12")\
    .set("spark.driver.cores", "12")\
    .set("spark.executor.memory", "15g")\
    .set("spark.driver.memory", "20g")\
    .set("spark.network.timeout","360100s")\
    .set("spark.executor.heartbeatInterval","360000s")\
    .set("spark.shuffle.registration.timeout","360000s")\
    .set("spark.sql.shuffle.partitions",200)\
    .setMaster("local[*]")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
