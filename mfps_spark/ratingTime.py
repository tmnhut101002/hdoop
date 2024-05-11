from pyspark.sql import SparkSession
from pyspark import SparkConf
import timeit
from math import exp

def extractInputFile(line):
    userItem,  ratingTime = line.strip().split('\t')
    user, item = userItem.strip().split(';')
    return (user, item)

def extractInputFileWithTime(line):
    userItem,  ratingTime = line.strip().split('\t')
    user, item = userItem.strip().split(';')
    rate, time = ratingTime.strip().split(';')
    
    return ((user, item), time)

def extractAVG(line):
    user, avg = line.strip().split('\t')
    return (user, avg)

def mapRDD(line, userItemList):
    rc = []
    user, item = line[0], line[1]
    for u,il in userItemList:
        if user != u:
            rc.append(((user, u), set(item).intersection(set(il))))
    return rc

def calRT(time1, time2):
    alpha = 10**-6
    return exp(-alpha * abs(float(time1) - float(time2)))

def findInput(user, item, input_with_rate_list):
    for  t in input_with_rate_list:
        if user==t[0] and item==t[1]:
            return t[2]
    return None

def mapRtRDD(line, input_with_time_dict):
    rt = []

    user1 = line[0][0]
    user2 = line[0][1]
    count_rt = 0

    if len(line[1]) <= 0:
        rt.append((user1,user2, 0))
    else:
        for item in list(line[1]):
            time1 = input_with_time_dict.get((user1,item))[0]
            time2 = input_with_time_dict.get((user1,item))[0]
            
            count_rt += calRT(time1, time2)
        rt.append((user1,user2, count_rt))
    
    return rt

def Convert(tup, di):
    for a, b in tup:
        di.setdefault(a, []).append(b)
    return di

def RatingTime(input_file, output_file):

    conf = SparkConf()\
    .setAppName("RT")\
    .set("spark.executor.instances", "10")\
    .set("spark.executor.cores", "5")\
    .set("spark.executor.memory", "6g")\
    .set("spark.driver.memory", "6g")\
    .set("spark.default.parallelism", "20")\
    .setMaster("local[*]")

    # .set("spark.executor.memory", "4g")\
    # .set("spark.driver.memory", "4g")\
    # .set("spark.executor.cores", "5")\
    # .set("spark.dynamicAllocation.enabled","true")\
    # .set("spark.memory.offHeap.size", "4g")\
    # .set("spark.memory.offHeap.enabled","true")\
    # .set("spark.default.parallelism", "20")\
    # .set("spark.shuffle.file.buffer", "128m")\
    # .set("spark.network.timeout","3601s")\
    # .set("spark.executor.heartbeatInterval","3600s")\
    # .set("spark.io.compression.lz4.blockSize","5m")\
    # .set("spark.shuffle.registration.timeout","120000ms").set("spark.driver.maxResultSize","0")\
    # .setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    input_rdd = spark.sparkContext.textFile(input_file).map(extractInputFile).groupByKey()
    input_list = input_rdd.collect()

    input_with_time_rdd = spark.sparkContext.textFile(input_file).map(extractInputFileWithTime)
    input_with_time_list = input_with_time_rdd.collect()

    input_with_time_dict = Convert(input_with_time_list, {})

    #ak = input_with_rate_rdd.collect()

    rc_rdd = input_rdd.map(lambda x: mapRDD(x, input_list)).flatMap(lambda x : x)
    

    rt_rdd = rc_rdd.map(lambda x: mapRtRDD(x,input_with_time_dict)).flatMap(lambda x : x).map(lambda x: (x[0]+';'+x[1],str(x[2])+';'+'rt'))
    aa= rt_rdd.take(5)

    rt_df = rt_rdd.toDF()
    rt_df.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file)


    # with open(output_file,'w') as out:
    #     for i in rt_rdd.collect():
    #             for re in i:
    #                 out.write(f'{re[0]};{re[1]}\t{re[2]};rt\n')

    spark.stop()

if  __name__ == "__main__":
    start = timeit.default_timer()
    RatingTime('../input_file.txt', "hdfs://localhost:9000/MFPS/rt.csv")
    stop = timeit.default_timer()
    with open('./output/time_.txt', 'a') as out:
        out.write('rt ' +str(stop-start)+'\n')
