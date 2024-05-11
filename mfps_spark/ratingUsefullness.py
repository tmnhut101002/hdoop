from pyspark.sql import SparkSession
from pyspark import SparkConf
import timeit

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


def RatingUsefullness(input_file, output_file):
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
    
    # .set("spark.executor.instances", "20")\
    # .set("spark.executor.memory", "4g")\
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

    input_rdd = spark.sparkContext.textFile(input_file).map(extractInputFile).groupByKey().map(lambda x: (x[0], set(x[1])))
    user_num_item_list = input_rdd.collect() #count item

    ru_rdd = input_rdd.map(lambda x: extractRC_CalRU(x,user_num_item_list))
    ru_rdd = ru_rdd.flatMap(lambda x:x).map(lambda x: (x[0]+';'+x[1],str(x[2]) +';'+'ru'))
    ru_df = ru_rdd.toDF(["U1_U2","ru"])

    ru_df.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file)


    # with open(output_file,'w') as out:
    #     for re in ru_rdd.collect():
    #         for el in  re:
    #             out.write(f'{el[0]};{el[1]}\t{el[2]};ru\n')
    
    spark.stop()
       


if  __name__ == "__main__":
    start = timeit.default_timer()
    RatingUsefullness('../input_file.txt',"hdfs://localhost:9000/MFPS/ru.csv")
    stop = timeit.default_timer()
    with open('./output/time_.txt', 'a') as out:
        out.write('ru '+str(stop-start)+'\n')