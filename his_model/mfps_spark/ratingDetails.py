from pyspark.sql import SparkSession
from pyspark import SparkConf
import timeit
import mysql.connector
import os

def extractInputFile(line):
    userItem,  ratingTime = line.strip().split('\t')
    user, item = userItem.strip().split(';')
    return (user, item)

def extractInputFileWithRating(line):
    userItem,  ratingTime = line.strip().split('\t')
    user, item = userItem.strip().split(';')
    rate, time = ratingTime.strip().split(';')
    
    return ((user, item), float(rate))

def extractAVG(line):
    user, avg = line.strip().split('\t')
    return (user, avg)

def mapRcRDD(line, userItemList):
    rc = []
    user, item = line[0], line[1]
    for u,il in userItemList:
        if user != u:
            rc.append(((user, u), set(item).intersection(set(il))))
    return rc

def calRD(rate1, rate2, avg1, avg2):
    rate1 = float(rate1)
    rate2 =  float(rate2)
    avg1 = float(avg1)
    avg2 = float(avg2)
    if rate1 >= avg1 and rate2 >= avg2:
        return 1
    elif rate1 <= avg1 and rate2 <= avg2:
        return 1
    else:
        return 0


def mapRdRDD(line, avg_dict, input_with_rate_dict):
    rd = []
    user1 = line[0][0]
    user2 = line[0][1]
    avg_check_1 = avg_dict.get(user1)[0]
    avg_check_2 = avg_dict.get(user2)[0]
    count_rd = 0
    if len(line[1]) <= 0:
        rd.append((user1,user2, 0))
    else:
        for item in list(line[1]):
            rate1 = input_with_rate_dict.get((user1,item))[0]
            rate2 = input_with_rate_dict.get((user2,item))[0]
            count_rd += calRD(rate1, rate2, avg_check_1, avg_check_2)
        rd.append((user1,user2, count_rd))
    return rd

def Convert(tup, di):
    for a, b in tup:
        di.setdefault(a, []).append(b)
    return di

def save_to_mysql(df, db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    i = 1
    # Insert or update data
    for row in df.collect():
        user_u, user_v = row['U1_U2'].split(';')
        rd = float(row['rd'].split(';')[0])
        label = row['label']

        # Check if record exists
        cursor.execute("SELECT 1 FROM PreSIM WHERE user_u = %s AND user_v = %s", (user_u, user_v))
        exists = cursor.fetchone()

        if exists:
            # Update existing record
            print(f'{i} - {label}. Update {user_u},{user_v}')
            update_query = "UPDATE PreSIM SET rd = %s, label = %s WHERE user_u = %s AND user_v = %s"
            cursor.execute(update_query, (rd, label, user_u, user_v))
        else:
            # Insert new record
            print(f'{i} - {label}. Insert {user_u},{user_v}')
            insert_query = "INSERT INTO PreSIM (user_u, user_v, rd, label) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (user_u, user_v, rd, label))
        i+=1

    conn.commit()
    cursor.close()
    conn.close()

def RatingDetails(input_file, db_config, avg_rating):

    conf = SparkConf()\
    .setAppName("RD")\
    .set("spark.executor.instances", "10")\
    .set("spark.executor.cores", "15")\
    .set("spark.executor.memory", "6g")\
    .set("spark.driver.memory", "6g")\
    .set("spark.default.parallelism", "12")\
    .set("spark.network.timeout","360100s")\
    .set("spark.executor.heartbeatInterval","360000s")\
    .set("spark.shuffle.registration.timeout","120000ms")\
    .set("spark.sql.shuffle.partitions",100)\
    .setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    label = os.path.splitext(os.path.basename(input_file))[0].replace("input_", "")

    sc = spark.sparkContext.textFile(input_file)
    input_rdd = sc.map(extractInputFile).groupByKey()

    input_with_rate_rdd = sc.map(extractInputFileWithRating)
    avg_rdd = spark.sparkContext.textFile(avg_rating).map(extractAVG)
    
    input_list = input_rdd.collect()
    

    avg_list = avg_rdd.collect()
    avg_dict = Convert(avg_list,{})

    input_with_rate_list = input_with_rate_rdd.collect()
    input_with_rate_dict = Convert(input_with_rate_list,{})
    
    rc_rdd = input_rdd.map(lambda x: mapRcRDD(x, input_list)).flatMap(lambda x : x)

    rd_rdd = rc_rdd.map(lambda x: mapRdRDD(x,avg_dict,input_with_rate_dict)).flatMap(lambda x : x).map(lambda x: (x[0]+';'+x[1], str(x[2]), label))
    
    rd_df = rd_rdd.toDF(["U1_U2", "rd", "label"])
    
    save_to_mysql(rd_df, db_config)
    # rd_df.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file)


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
        
        RatingDetails(INPUT, db_config, AVG)
    
    # RatingDetails('file:///home/hdoop/his_model/mfps_spark/temp_preSIM/input_21558077.txt', db_config,'file:///home/hdoop/his_model/mfps_spark/temp_preSIM/avg_21558077.txt')
    stop = timeit.default_timer()
    print(str(stop-start))
    with open('/home/hdoop/his_model/mfps_spark/time.txt', 'a') as out:
        out.write('rd ' +str(stop-start)+'\n')
