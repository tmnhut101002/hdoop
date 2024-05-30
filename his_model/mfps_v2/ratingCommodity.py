from pyspark.sql import SparkSession
from pyspark import SparkConf
import timeit
def extractInputFile(line):
    userItem,  ratingTime = line.strip().split('\t')
    user, item = userItem.strip().split(';')
    return (user, item)

def mapRDD(line, userItemList):
    rc = []
    user, item = line[0], line[1]
    for u,il in userItemList:
        if user != u:
            rc.append(((user, u), len(set(item).intersection(set(il)))))
    return rc


def RatingCommodity(input_file):

    conf = SparkConf()\
    .setAppName("RC")\
    .set("spark.executor.instances", "10")\
    .set("spark.executor.cores", "5")\
    .set("spark.executor.memory", "6g")\
    .set("spark.driver.memory", "6g")\
    .set("spark.driver.cores", "5")\
    .set("spark.default.parallelism", "20")\
    .setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    input_rdd = spark.sparkContext.textFile(input_file).map(extractInputFile).groupByKey()
    input_list = input_rdd.collect()

    rc_rdd = input_rdd.map(lambda x: mapRDD(x, input_list))
    rc_rdd = rc_rdd.flatMap(lambda x: x).map(lambda x: (x[0][0] + ';' + x[0][1], str(x[1]) + ';'+'rc'))
    rc_df = rc_rdd.toDF(["U1_U2", "rc"])
    
    pdrc_df = rc_df.toPandas()
    spark.stop()

    return pdrc_df

if __name__ =='__main__':
    start = timeit.default_timer()
    pd = RatingCommodity('../input_file.txt',"hdfs://localhost:9000/MFPS/rc.csv")

    stop = timeit.default_timer()
    with open('./output/time_.txt', 'a') as out:
        out.write('rc ' +str(stop-start)+'\n')
