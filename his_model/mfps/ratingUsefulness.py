from pyspark.sql import SparkSession
from pyspark import SparkConf

def extractInputFile(line):
    userItem,  ratingTime = line.strip().split('\t')
    user, item = userItem.strip().split(';')
    return (user, item)

def extractRC_CalRU(x, User_ItemList):
    ru = []
    for i in User_ItemList:
        if  x[0] != i[0]:
            ru.append((x[0],i[0],len(i[1].difference(set(x[1])))))
    return ru


def RatingUsefulness(input_file):
    conf = SparkConf()\
    .setAppName("RU")\
    .set("spark.executor.instances", "10")\
    .set("spark.executor.cores", "5")\
    .set("spark.executor.memory", "6g")\
    .set("spark.driver.memory", "6g")\
    .set("spark.driver.cores", "5")\
    .set("spark.default.parallelism", "20")\
    .set("spark.network.timeout","3601s")\
    .set("spark.executor.heartbeatInterval","3600s")\
    .setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    input_rdd = spark.sparkContext.textFile(input_file).map(extractInputFile).groupByKey().map(lambda x: (x[0], set(x[1])))
    user_num_item_list = input_rdd.collect() #count item

    ru_rdd = input_rdd.map(lambda x: extractRC_CalRU(x,user_num_item_list))
    ru_rdd = ru_rdd.flatMap(lambda x:x).map(lambda x: (x[0]+';'+x[1],str(x[2]) +';'+'ru'))
    ru_df = ru_rdd.toDF(["U1_U2","ru"])
    pd_df = ru_df.toPandas()
    
    spark.stop()
    return pd_df
