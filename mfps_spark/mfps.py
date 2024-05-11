from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, desc, split, col
from pyspark import SparkConf
import timeit
from hdfs import InsecureClient
import pandas as pd



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


def mfps(rc, ru, rd, rt, output_file):
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

    rc_df = spark.read.options(header='False', delimiter = '\t').csv(rc)
    ru_df = spark.read.options(header='False', delimiter = '\t').csv(ru)

    mfps1_df = rc_df.withColumnRenamed('_c1','rc').join(ru_df.withColumnRenamed('_c1','ru'),['_c0'])

    rt_df = spark.read.options(header='False', delimiter = '\t').csv(rt)
    rd_df = spark.read.options(header='False', delimiter = '\t').csv(rd)
    mfps2_df = rt_df.withColumnRenamed('_c1','rt').join(rd_df.withColumnRenamed('_c1','rd'),['_c0'])

    mapCalMFPS_udf = udf(mapCalMFPS)

    mfps_df = mfps1_df.join(mfps2_df,['_c0'])

    mfps_df = mfps_df.withColumn("mfps", mapCalMFPS_udf(mfps_df.rc, mfps_df.ru, mfps_df.rd, mfps_df.rt)).select(['_c0','mfps'])

    #mfps_df.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file)
    
    user_u_df = mfps_df.withColumn('_c0', split(col('_c0'), ';')[0])

    users = user_u_df.select('_c0').distinct().rdd.flatMap(lambda x: x).collect()
    
    # print(users)
    for user in users:
        user_df = mfps_df.filter(split(col('_c0'), ';')[0] == user)
        user_sorted = user_df.orderBy(desc('mfps'))
        output_file_user = output_file + f"/{user}.csv"
        user_sorted.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file_user)

    spark.stop()


if  __name__ == "__main__":
    mfps("hdfs:///MFPS/group_3/rc","hdfs:///MFPS/group_3/ru","hdfs:///MFPS/group_3/rd","hdfs:///MFPS/group_3/rt","hdfs:///MFPS/full_sim")
