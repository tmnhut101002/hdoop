from pyspark.sql import SparkSession
from pyspark import SparkConf
import timeit
from math import exp
import mysql.connector
import os


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

def save_to_mysql(df, db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    i=1
    # Insert or update data
    for row in df.collect():
        user_u, user_v = row['U1_U2'].split(';')
        rt = float(row['rt'])
        label = row['label']

        # Check if record exists
        cursor.execute("SELECT 1 FROM PreSIM WHERE user_u = %s AND user_v = %s", (user_u, user_v))
        exists = cursor.fetchone()

        if exists:
            # Update existing record
            print(f'{i} - {label}. Update {user_u},{user_v}')
            print(f'Update {user_u},{user_v}')
            update_query = "UPDATE PreSIM SET rt = %s, label = %s WHERE user_u = %s AND user_v = %s"
            cursor.execute(update_query, (rt, label, user_u, user_v))
        else:
            # Insert new record
            print(f'{i} - {label}. {user_u},{user_v}')
            insert_query = "INSERT INTO PreSIM (user_u, user_v, rt, label) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (user_u, user_v, rt, label))
        i+=1
        
    conn.commit()
    cursor.close()
    conn.close()

def RatingTime(input_file, output_file):

    conf = SparkConf()\
    .setAppName("RT")\
    .set("spark.executor.instances", "10")\
    .set("spark.executor.cores", "5")\
    .set("spark.executor.memory", "6g")\
    .set("spark.driver.memory", "6g")\
    .set("spark.default.parallelism", "20")\
    .setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    label = os.path.splitext(os.path.basename(input_file))[0].replace("input_", "")

    input_rdd = spark.sparkContext.textFile(input_file).map(extractInputFile).groupByKey()
    input_list = input_rdd.collect()

    input_with_time_rdd = spark.sparkContext.textFile(input_file).map(extractInputFileWithTime)
    input_with_time_list = input_with_time_rdd.collect()

    input_with_time_dict = Convert(input_with_time_list, {})

    #ak = input_with_rate_rdd.collect()

    rc_rdd = input_rdd.map(lambda x: mapRDD(x, input_list)).flatMap(lambda x : x)
    

    rt_rdd = rc_rdd.map(lambda x: mapRtRDD(x,input_with_time_dict)).flatMap(lambda x : x).map(lambda x: (x[0]+';'+x[1], str(x[2]), label))

    rt_df = rt_rdd.toDF(["U1_U2", "rt", "label"])
    save_to_mysql(rt_df, db_config)
    spark.stop()

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
        INPUT = f'file:///home/hdoop/his_model/mfps_spark/temp_preSIM/input_{ListLabel[index]}.txt'
        AVG = f'file:///home/hdoop/his_model/mfps_spark/temp_preSIM/avg_{ListLabel[index]}.txt'
        
        RatingTime(INPUT, db_config)
    # RatingTime('file:///home/hdoop/his_model/mfps_spark/temp_preSIM/input_632553.txt', db_config)
    stop = timeit.default_timer()
    print(str(stop-start))
    with open('/home/hdoop/his_model/mfps_spark/time.txt', 'a') as out:
        out.write('rt ' +str(stop-start)+'\n')
