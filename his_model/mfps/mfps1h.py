from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark import SparkConf
import timeit

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

def mapUserID (U1_U2, mfps):
    u1,u2 = U1_U2.strip().split(';')
    return [u1,u2,mfps]

def mfps1h(rc_df, ru_df, rd_df, rt_df, u_list = []):

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
    
    rc_df = spark.createDataFrame(rc_df).rdd.map(lambda x : mapUserID(x[0],x[1])).toDF(['u1','u2','rc'])
    rc_df = rc_df.filter((rc_df.u1.isin(u_list)) | (rc_df.u2.isin(u_list)))
    ru_df = spark.createDataFrame(ru_df).rdd.map(lambda x : mapUserID(x[0],x[1])).toDF(['u1','u2','ru'])
    ru_df = ru_df.filter((ru_df.u1.isin(u_list)) | (ru_df.u2.isin(u_list)))

    mfps1_df = rc_df.join(ru_df,['u1','u2'])

    rt_df = spark.createDataFrame(rt_df).rdd.map(lambda x : mapUserID(x[0],x[1])).toDF(['u1','u2','rt'])
    rt_df = rt_df.filter((rt_df.u1.isin(u_list)) | (rt_df.u2.isin(u_list)))

    rd_df = spark.createDataFrame(rd_df).rdd.map(lambda x : mapUserID(x[0],x[1])).toDF(['u1','u2','rd'])
    rd_df = rd_df.filter((rd_df.u1.isin(u_list)) | (rd_df.u2.isin(u_list)))

    mfps2_df = rt_df.join(rd_df,['u1','u2'])

    mapCalMFPS_udf = udf(mapCalMFPS)

    mfps_df = mfps1_df.join(mfps2_df,['u1','u2'])
    mfps_df = mfps_df.withColumn("mfps", mapCalMFPS_udf(mfps_df.rc, mfps_df.ru,mfps_df.rd,mfps_df.rt)).select(['u1','u2','mfps'])
    pd_df = mfps_df.toPandas()
    pd_df.rename({"u1": "user_u",  
            "u2": "user_v"},
            axis = "columns", inplace = True) 

    spark.stop()
    return pd_df