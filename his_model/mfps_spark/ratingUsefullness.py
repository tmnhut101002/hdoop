from pyspark.sql import SparkSession
from pyspark import SparkConf
import timeit
import mysql.connector
import os

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

def save_to_mysql(df, db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Insert or update data
    for row in df.collect():
        user_u, user_v = row['U1_U2'].split(';')
        ru = float(row['ru'])
        label = row['label']

        # Check if record exists
        cursor.execute("SELECT 1 FROM PreSIM WHERE user_u = %s AND user_v = %s", (user_u, user_v))
        exists = cursor.fetchone()

        if exists:
            # Update existing record
            print(f'Update {user_u},{user_v}')
            update_query = "UPDATE PreSIM SET ru = %s, label = %s WHERE user_u = %s AND user_v = %s"
            cursor.execute(update_query, (ru, label, user_u, user_v))
        else:
            # Insert new record
            print(f'Insert {user_u},{user_v}')
            insert_query = "INSERT INTO PreSIM (user_u, user_v, ru, label) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (user_u, user_v, ru, label))

    conn.commit()
    cursor.close()
    conn.close()

def RatingUsefullness(input_file, db_config):
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
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    label = os.path.splitext(os.path.basename(input_file))[0].replace("input_", "")

    input_rdd = spark.sparkContext.textFile(input_file).map(extractInputFile).groupByKey().map(lambda x: (x[0], set(x[1])))
    user_num_item_list = input_rdd.collect() #count item

    ru_rdd = input_rdd.map(lambda x: extractRC_CalRU(x,user_num_item_list))
    ru_rdd = ru_rdd.flatMap(lambda x:x).map(lambda x: (x[0] + ';' + x[1], str(x[2]), label))
    ru_df = ru_rdd.toDF(["U1_U2", "ru", "label"])
    save_to_mysql(ru_df, db_config)
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
        
        RatingUsefullness(INPUT, db_config)
    # RatingUsefullness('file:///home/hdoop/his_model/mfps_spark/temp_preSIM/input_632553.txt',db_config)
    stop = timeit.default_timer()
    with open('/home/hdoop/his_model/mfps_spark/time.txt', 'a') as out:
        out.write('ru '+str(stop-start)+'\n')