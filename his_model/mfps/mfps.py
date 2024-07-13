from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split, col
from pyspark import SparkConf
from pyspark.sql.types import FloatType
import mysql.connector

# Map Rating Commodity
def mapRC(line):
    key, value = line.strip().split("\t")
    user1, user2 = key.strip().split(';')
    rc, flag = value.strip().split(';')

    return (user1, user2, rc, flag)

# Map Rating Usefulness
def mapRU(line):
    key, value = line.strip().split("\t")
    user1, user2 = key.strip().split(';')
    ru, flag = value.strip().split(';')

    return (user1, user2, ru, flag)

# Map Rating Detail
def mapRD(line):
    key, value = line.strip().split("\t")
    user1, user2 = key.strip().split(';')
    rd, flag = value.strip().split(';')

    return (user1, user2, rd, flag)

# Map Rating Time
def mapRT(line):
    key, value = line.strip().split("\t")
    user1, user2 = key.strip().split(';')
    rt, flag = value.strip().split(';')

    return (user1, user2, rt, flag)

# Map tính sim
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

# Tính độ tương đồng giữa các cặp user
def mfps(rc_df, ru_df, rd_df, rt_df):
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
    mfps_df = mfps_df.withColumn("user_u", split(col("U1_U2"), ";").getItem(0)) \
                    .withColumn("user_v", split(col("U1_U2"), ";").getItem(1))
    mfps_df = mfps_df.select("user_u", "user_v", "mfps")
    mfps_df = mfps_df.withColumn("mfps", mfps_df["mfps"].cast(FloatType()))
    
    pd_df = mfps_df.toPandas()
    spark.stop()
    
    return pd_df

# Lưu sim vào data base
def load_sim_to_mysql(df, db_config):
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    sl = 0
    for row in df.itertuples():
        user_u = str(row.user_u)
        user_v = str(row.user_v)
        sim = float(row.mfps)
        cursor.execute('INSERT INTO Sim (user_u, user_v, sim) VALUES (%s, %s, %s);',(user_u, user_v, sim))
        sl+=1
    cnx.commit()
    print(f'>>> Load {sl} value')