from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split, col
from pyspark import SparkConf
import timeit
import redis
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.field import TagField, NumericField
import os
from pyspark.sql.types import FloatType
import mysql.connector

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

    # Select the required columns and cast 'mfps' to FloatType
    mfps_df = mfps_df.select("user_u", "user_v", "mfps")
    mfps_df = mfps_df.withColumn("mfps", mfps_df["mfps"].cast(FloatType()))
    
    pd_df = mfps_df.toPandas()
    print(pd_df.info())
    spark.stop()
    return pd_df

def initialize_redis_index(redis_client, model_index_name, model_key_base):
    # Tạo một chỉ mục mới
    redis_client.ft(model_index_name).create_index(
        [
            TagField("$.user_u", as_name="user_id_u"),
            TagField("$.user_v", as_name="user_id_v"),
            NumericField("$.mfps", as_name="sim")
        ],
        definition=IndexDefinition(
            index_type=IndexType.JSON,
            prefix=[f"{model_key_base}:"]
        )
    )
    
def load_sim_to_redis(df, redis_client, model_key_base):
    MFPSs_loaded = 0
    
    try:
        pipeline = redis_client.pipeline(transaction=False)

        for index, row in df.iterrows():
            user_id_u = row['user_u']
            user_id_v = row['user_v']
            mfps = float(row['mfps'])
            
            data = {
                'user_id_u': user_id_u,
                'user_id_v': user_id_v,
                'mfps': mfps
            }
            
            redis_key = f"{model_key_base}:{user_id_u}:{user_id_v}"
            pipeline.json().set(redis_key, "$", data)
            MFPSs_loaded += 1
            print(f"{redis_key} - MFPS: {mfps}")

        pipeline.execute()
    except Exception as e:
        print("Failed to load MFPS data:")
        print(e)
        os._exit(1)

    print(f"Loaded {MFPSs_loaded} MFPSs into Redis.")

def load_sim_to_mysql(df):
    cnx = mysql.connector.connect(user='root', password='Password@123',host='localhost', database='ecommerce')
    cursor = cnx.cursor()
    sl = 0
    for row in df.itertuples():
        user_u = str(row.user_u)
        user_v = str(row.user_v)
        sim = float(row.mfps)
        print(f'Insert {user_u}:{user_v}:{sim}')
        cursor.execute('INSERT INTO Sim (user_u, user_v, sim) VALUES (%s, %s, %s);',(user_u, user_v, sim))
        sl+=1
    cnx.commit()
    print(f'Load {sl} into database')
    
if  __name__ == "__main__":
    start = timeit.default_timer()
    pd_df = mfps("hdfs://localhost:9000/MFPS/rc.csv","hdfs://localhost:9000/MFPS/ru.csv","hdfs://localhost:9000/MFPS/rd.csv","hdfs://localhost:9000/MFPS/rt.csv","hdfs://localhost:9000/MFPS/mfps.csv")
    stop = timeit.default_timer()
    with open('./output/time.txt', 'a') as out:
        out.write('mfps end: ' +str(stop-start)+'\n')
    