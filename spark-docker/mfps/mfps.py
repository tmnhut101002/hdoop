from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark import SparkConf
from ratingCommodity import RatingCommodity
from ratingDetails import RatingDetails
from ratingUsefullness import RatingUsefullness
from ratingTime import RatingTime

def mapRC(line):
    key, value = line.strip().split("\t")
    user1, user2 = key.strip().split(';')
    rc, flag = value.strip().split(';')

    return (user1, user2, rc, flag)

def mapRU(line):
    key, value = line.strip().split("\t")
    user1, user2 = key.strip().split(';')
    ru, flag = value.strip().split(';')

    return (user1, user2, ru, flag)

def mapRD(line):
    key, value = line.strip().split("\t")
    user1, user2 = key.strip().split(';')
    rd, flag = value.strip().split(';')

    return (user1, user2, rd, flag)

def mapRT(line):
    key, value = line.strip().split("\t")
    user1, user2 = key.strip().split(';')
    rt, flag = value.strip().split(';')

    return (user1, user2, rt, flag)

def mapCalMFPS (rc,ru,rd,rt):
    mfps = 0
    rc,f1 = rc.split(';')
    ru,f2 = ru.split(';')
    rd,f3 = rd.split(';')
    rt,f4 = rt.split(';')


    if float(rc) != 0:
        mfps += 1/float(rc)
    
    if float(ru) != 0:
        mfps += 1/float(ru)
    
    if float(rd) != 0:
        mfps += 1/float(rd)
    
    if float(rt) != 0:
        mfps += 1/float(rt)
    
    return 1/(1 + mfps)

    

def mfps():
    # pandas df
    rc_df = RatingCommodity('../input_file.txt')
    ru_df = RatingUsefullness('../input_file.txt')
    rd_df = RatingDetails('../input_file.txt','./output/avg.txt')
    rt_df = RatingTime('../input_file.txt')

    conf = SparkConf()\
    .setAppName("MFPS")\
    .set("spark.eventLog.enabled","true")\
    .set("spark.eventLog.dir","./")\
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
    
    rc_df = spark.createDataFrame(rc_df)

    ru_df = spark.createDataFrame(ru_df)

    mfps1_df = rc_df.join(ru_df,['U1_U2'])

    rt_df = spark.createDataFrame(rt_df)
    rd_df = spark.createDataFrame(rd_df)

    mfps2_df = rt_df.join(rd_df,['U1_U2'])

    mapCalMFPS_udf = udf(mapCalMFPS)

    mfps_df = mfps1_df.join(mfps2_df,['U1_U2'])

    mfps_df = mfps_df.withColumn("mfps", mapCalMFPS_udf(mfps_df.rc, mfps_df.ru,mfps_df.rd,mfps_df.rt)).select(['U1_U2','mfps'])

    pd_df = mfps_df.toPandas()
    spark.stop()
    return pd_df


if  __name__ == "__main__":
    pd_df = mfps()
    print(pd_df)
