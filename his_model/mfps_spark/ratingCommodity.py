from pyspark.sql import SparkSession
from pyspark import SparkConf
import timeit
import mysql.connector
from mysql.connector import errorcode
import os

def extractInputFile(line):
    userItem,  ratingTime = line.strip().split('\t')
    user, item = userItem.strip().split(';')
    return (user, item)

def mapRDD(line, userItemList):
    rc = []
    user, item = line[0], line[1]
    for u,il in userItemList:
        if user != u:
            rc.append(((user, u), len(set(item).intersection(set(il)))))
    return rc

def save_to_mysql(df, db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Insert or update data
    for row in df.collect():
        user_u, user_v = row['U1_U2'].split(';')
        rc = float(row['rc'].split(';')[0])
        label = row['label']

        # Check if record exists
        cursor.execute("SELECT 1 FROM PreSIM WHERE user_u = %s AND user_v = %s", (user_u, user_v))
        exists = cursor.fetchone()

        if exists:
            # Update existing record
            print(f'Update {user_u},{user_v}')
            update_query = "UPDATE PreSIM SET rc = %s, label = %s WHERE user_u = %s AND user_v = %s"
            cursor.execute(update_query, (rc, label, user_u, user_v))
        else:
            # Insert new record
            print(f'Insert {user_u},{user_v}')
            insert_query = "INSERT INTO PreSIM (user_u, user_v, rc, label) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (user_u, user_v, rc, label))

    conn.commit()
    cursor.close()
    conn.close()

def RatingCommodity(input_file, db_config):

    conf = SparkConf()\
    .setAppName("RC")\
    .set("spark.executor.instances", "10")\
    .set("spark.executor.cores", "5")\
    .set("spark.executor.memory", "6g")\
    .set("spark.driver.memory", "6g")\
    .set("spark.driver.cores", "5")\
    .set("spark.default.parallelism", "20")\
    .setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    label = os.path.splitext(os.path.basename(input_file))[0].replace("input_", "")

    input_rdd = spark.sparkContext.textFile(input_file).map(extractInputFile).groupByKey()
    input_list = input_rdd.collect()

    rc_rdd = input_rdd.map(lambda x: mapRDD(x, input_list))
    rc_rdd = rc_rdd.flatMap(lambda x: x).map(lambda x: (x[0][0] + ';' + x[0][1], str(x[1]) , label))
    rc_df = rc_rdd.toDF(["U1_U2", "rc", "label"])
    save_to_mysql(rc_df, db_config)
    # rc_df.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file)
    
    spark.stop()

if __name__ =='__main__':
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
        
        RatingCommodity(INPUT, db_config)
    # RatingCommodity('file:///home/hdoop/his_model/mfps_spark/temp_preSIM/input_632553.txt', db_config)
    stop = timeit.default_timer()
    with open('/home/hdoop/his_model/mfps_spark/time.txt', 'a') as out:
        out.write('rc ' +str(stop-start)+'\n')
