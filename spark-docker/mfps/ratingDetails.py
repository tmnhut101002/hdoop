from pyspark.sql import SparkSession
from pyspark import SparkConf
import timeit

def extractInputFile(line):
    userItem,  ratingTime = line.strip().split('\t')
    user, item = userItem.strip().split(';')
    return (user, item)

def extractInputFileWithRating(line):
    userItem,  ratingTime = line.strip().split('\t')
    user, item = userItem.strip().split(';')
    rate, time = ratingTime.strip().split(';')
    
    return ((user, item), float(rate))

def extractAVG(line):
    user, avg = line.strip().split('\t')
    return (user, avg)

def mapRcRDD(line, userItemList):
    rc = []
    user, item = line[0], line[1]
    for u,il in userItemList:
        if user != u:
            rc.append(((user, u), set(item).intersection(set(il))))
    return rc

def calRD(rate1, rate2, avg1, avg2):
    rate1 = float(rate1)
    rate2 =  float(rate2)
    avg1 = float(avg1)
    avg2 = float(avg2)
    if rate1 >= avg1 and rate2 >= avg2:
        return 1
    elif rate1 <= avg1 and rate2 <= avg2:
        return 1
    else:
        return 0


def mapRdRDD(line, avg_dict, input_with_rate_dict):
    rd = []
    user1 = line[0][0]
    user2 = line[0][1]
    avg_check_1 = avg_dict.get(user1)[0]
    avg_check_2 = avg_dict.get(user2)[0]
    count_rd = 0
    if len(line[1]) <= 0:
        rd.append((user1,user2, 0))
    else:
        for item in list(line[1]):
            rate1 = input_with_rate_dict.get((user1,item))[0]
            rate2 = input_with_rate_dict.get((user2,item))[0]
            count_rd += calRD(rate1, rate2, avg_check_1, avg_check_2)
        rd.append((user1,user2, count_rd))
    return rd

def Convert(tup, di):
    for a, b in tup:
        di.setdefault(a, []).append(b)
    return di

def RatingDetails(input_file, avg_rating):

    conf = SparkConf()\
    .setAppName("RD")\
    .set("spark.executor.instances", "10")\
    .set("spark.executor.cores", "15")\
    .set("spark.executor.memory", "6g")\
    .set("spark.driver.memory", "6g")\
    .set("spark.default.parallelism", "12")\
    .set("spark.network.timeout","360100s")\
    .set("spark.executor.heartbeatInterval","360000s")\
    .set("spark.shuffle.registration.timeout","120000ms")\
    .set("spark.sql.shuffle.partitions",100)\
    .setMaster("local[*]")

    # .set("spark.executor.instances", "20")\
    # .set("spark.executor.memory", "4g")\
    # .set("spark.driver.memory", "4g")\
    # .set("spark.executor.cores", "5")\
    # .set("spark.dynamicAllocation.enabled","true")\
    # .set("spark.memory.offHeap.size", "4g")\
    # .set("spark.memory.offHeap.enabled","true")\
    # .set("spark.default.parallelism", "20")\
    # .set("spark.shuffle.file.buffer", "128m")\
    # .set("spark.network.timeout","3601s")\
    # .set("spark.executor.heartbeatInterval","3000s")\
    # .set("spark.io.compression.lz4.blockSize","5m")\
    # .set("spark.shuffle.registration.timeout","120000ms").set("spark.driver.maxResultSize","0")\
    # .setMaster("local[*]")
  
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    sc = spark.sparkContext.textFile(input_file)
    input_rdd = sc.map(extractInputFile).groupByKey()

    input_with_rate_rdd = sc.map(extractInputFileWithRating)
    avg_rdd = spark.sparkContext.textFile(avg_rating).map(extractAVG)
    
    input_list = input_rdd.collect()
    

    avg_list = avg_rdd.collect()
    avg_dict = Convert(avg_list,{})

    input_with_rate_list = input_with_rate_rdd.collect()
    input_with_rate_dict = Convert(input_with_rate_list,{})

    aaa = input_with_rate_dict.get(('1','919'))
    
    rc_rdd = input_rdd.map(lambda x: mapRcRDD(x, input_list)).flatMap(lambda x : x)
    rc_rddl = rc_rdd.take(5)

    rd_rdd = rc_rdd.map(lambda x: mapRdRDD(x,avg_dict,input_with_rate_dict)).flatMap(lambda x : x).map(lambda x: (x[0]+';'+x[1],str(x[2])+';'+'rd'))
    
    rd_rddl = rd_rdd.take(5)
    rd_df = rd_rdd.toDF(["U1_U2", "rd"])

    pd_df = rd_df.toPandas()
    # rd_df.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file)

    a = 0

    # with open(output_file,'w') as out:
    #     for i in rd_rdd.collect():
    #             for re in i:
    #                 out.write(f'{re[0]};{re[1]}\t{re[2]};rd\n')

    spark.stop()
    return pd_df

if  __name__ == "__main__":
    start = timeit.default_timer()
    pd = RatingDetails('../input_file.txt', "hdfs://localhost:9000/MFPS/rd.csv",'./output/avg.txt')
    stop = timeit.default_timer()
    with open('./output/time_.txt', 'a') as out:
        out.write('rd ' +str(stop-start)+'\n')
