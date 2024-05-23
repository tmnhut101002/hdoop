from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, desc, split, col
from pyspark import SparkConf
import timeit
from hdfs import InsecureClient
import pandas as pd
import redis
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.field import TagField, NumericField
import os
import json
import mysql.connector
from pyspark.sql.types import StructType, StructField, StringType, FloatType

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
    rc = rc
    ru = ru
    rd = rd
    rt = rt


    if float(rc) != 0:
        mfps += 1/float(rc)

    if float(ru) != 0:
        mfps += 1/float(ru)

    if float(rd) != 0:
        mfps += 1/float(rd)
    
    if float(rt) != 0:
        mfps += 1/float(rt)
    
    return 1/(1 + mfps)


def mfps(db_config, label):
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
    
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    query_mfps = f"SELECT user_u, user_v, rc, ru, rd, rt from PreSIM where label = {label}"
    cursor.execute(query_mfps)
    rows = cursor.fetchall()
    # Fetching the results
    mfps_df_db = [tuple(row) for row in rows]
    schema = StructType([
        StructField("user_u", StringType(), True),
        StructField("user_v", StringType(), True),
        StructField("rc", FloatType(), True),
        StructField("ru", FloatType(), True),
        StructField("rd", FloatType(), True),
        StructField("rt", FloatType(), True),
    ])
    mfps_df = spark.createDataFrame(mfps_df_db, schema=schema)
    mapCalMFPS_udf = udf(mapCalMFPS)

    mfps_df = mfps_df.withColumn("mfps", mapCalMFPS_udf(mfps_df.rc, mfps_df.ru, mfps_df.rd, mfps_df.rt)).select(['user_u','user_v','mfps'])
    mfps_df = mfps_df.withColumn("mfps", mfps_df["mfps"].cast(FloatType()))
    load_sim_to_redis(mfps_df)

    spark.stop()

def load_sim_to_redis(df_mfps):
    redis_client = redis.from_url('redis://localhost:6379/?decode_responses=True')
    MODEL_INDEX_NAME = "idx:sim"
    MODEL_KEY_BASE = "ecommerce:sim"

    try:
        redis_client.ft(MODEL_INDEX_NAME).dropindex(delete_documents=False)
    except:
        pass

    redis_client.ft(MODEL_INDEX_NAME).create_index(
        [
            TagField("$.user_id_u", as_name="user_id_u"),
            TagField("$.user_id_v", as_name="user_id_v"),
            NumericField("$.sim", as_name="sim")
        ],
        definition=IndexDefinition(
            index_type=IndexType.JSON,
            prefix=[f"{MODEL_KEY_BASE}:"]
        )
    )

    sim_df = df_mfps.toPandas()
    MFPSs_loaded = 0

    try:
        pipeline = redis_client.pipeline(transaction=False)

        for index, row in sim_df.iterrows():
            user_id_u = row['user_u']
            user_id_v = row['user_v']
            mfps = row['mfps']
            
            data = {
                'user_id_u': user_id_u,
                'user_id_v': user_id_v,
                'mfps': mfps
            }
            
            redis_key = f"{MODEL_KEY_BASE}:{user_id_u}:{user_id_v}"
            pipeline.json().set(redis_key, "$", data)
            MFPSs_loaded += 1
            print(f"{redis_key} - MFPS: {mfps}")

        pipeline.execute()
    except Exception as e:
        print("Failed to load MFPS data:")
        print(e)
        os._exit(1)

    print(f"Loaded {MFPSs_loaded} MFPSs into Redis.")

if  __name__ == "__main__":
    start = timeit.default_timer()
    db_config = {
        'user': 'root',
        'password': 'Password@123',
        'host': 'localhost',
        'database': 'ecommerce',
        'raise_on_warnings': True
    }
    
    NUMBER_OF_CLUSTERS = 4
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    # Query to fetch distinct labels from the label column
    query = "SELECT DISTINCT label FROM TrainingData_1"
    cursor.execute(query)
    ListLabel = [row[0] for row in cursor.fetchall()]
    
    for index in range(len(ListLabel)): 
        mfps(db_config, ListLabel[index])
        
    stop = timeit.default_timer()
    print(str(stop-start))
    with open('/home/hdoop/his_model/mfps_spark/time.txt', 'a') as out:
        out.write('sim ' +str(stop-start)+'\n')
