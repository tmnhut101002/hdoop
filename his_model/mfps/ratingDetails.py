from pyspark.sql import SparkSession
from pyspark import SparkConf

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
    
    rc_rdd = input_rdd.map(lambda x: mapRcRDD(x, input_list)).flatMap(lambda x : x)
    rd_rdd = rc_rdd.map(lambda x: mapRdRDD(x,avg_dict,input_with_rate_dict)).flatMap(lambda x : x).map(lambda x: (x[0]+';'+x[1],str(x[2])+';'+'rd'))
    rd_df = rd_rdd.toDF(["U1_U2", "rd"])
    pd_df = rd_df.toPandas()

    spark.stop()
    return pd_df

