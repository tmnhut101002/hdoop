from pyspark.sql import SparkSession
from pyspark import SparkConf

def extract_user_item_rating(df):
    user = df[0]
    item = df[1]
    rating = df[2]
    return (user, item, float(rating))

def mapValueResult(x):
    user_avg = x[0]
    item_rating = x[1]
    result_list, real_rating = item_rating[0], item_rating[1]

    for i in range(len(real_rating)):
        for j in range(len(result_list)):
            if str(result_list[j]) == str(real_rating[i]):
                result_list[j] = str(result_list[j])  + ";" + str(real_rating[i+1])

    for i in range(len(result_list)):
        a = str(result_list[i]).split(';')
        if len(a) == 1:
            result_list[i]= str(result_list[i]) +';'+ str(user_avg[1])
    
    return  ((user_avg[0]), '|'.join(result_list))
    
def createUserItemMatrix(iList, table_name, mysql_url, mysql_properties, avg_df):
    conf = SparkConf() \
        .setAppName('UserItemMatrix') \
        .set("spark.executor.memory", "4g") \
        .set("spark.driver.memory", "4g") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .set("spark.network.timeout", "3601s") \
        .set("spark.executor.heartbeatInterval", "3600s") \
        .setMaster("local[4]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    #map lines user-item-rating
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    
    lines_input_file = df.rdd.map(lambda row: (row.user_id, row.item_id, row.rating))
    user_item_rating_rdd = lines_input_file.map(extract_user_item_rating).repartition(100)
    
    user_avg_rating_rdd = spark.createDataFrame(avg_df).rdd.repartition(100)

    #user_rdd
    user_rdd = user_avg_rating_rdd.keys()
    #user /t item1; item2;...
    oneuser_moreitem_rdd = user_avg_rating_rdd. join(user_rdd.map(lambda x: (x,iList))).map(lambda x: ((x[0],x[1][0]),x[1][1])) # [0] (user,avg) [(item,rate(-1)),...]

    # user (avg(item,rating))
    user_avg_item_rdd_0 = user_avg_rating_rdd.join(user_item_rating_rdd)#user (avg, item) -- user trung
    user_avg_item_rdd_0 = user_avg_item_rdd_0.map(lambda x: ((x[0],x[1][1]),x[1][0])) #(user,item),(avg)
    user_avg_item_rdd_0 = user_avg_item_rdd_0.join(user_item_rating_rdd.map(lambda x: ((x[0],x[1]),x[2])))# (u,i),(a,r)
    user_avg_item_rdd_0 = user_avg_item_rdd_0.map (lambda x : ((x[0][0],x[1][0]),(x[0][1],x[1][1])))#[0](u,a),(i,r); [,](u,a),(i,r);...
    user_avg_item_rdd_0 = user_avg_item_rdd_0.sortByKey().reduceByKey(lambda x,y : x + y).map(lambda x: list(x))

    result = oneuser_moreitem_rdd.join(user_avg_item_rdd_0)
    #rdd to df
    result = result.map(mapValueResult).toDF()
    result = result.toPandas()
    
    spark.stop()
    return result

if __name__ == '__main__':
    spark = SparkSession.builder.appName('UserItemMatrix').getOrCreate()
    input_file = '../input_file.txt'
    avg_file = "hdfs://localhost:9000/Clustering/AverageRating.csv"
    items_file = "hdfs://localhost:9000/Clustering/Item.csv"
    output_file = "hdfs://localhost:9000/Clustering/UserItemMatrix.csv"
    uiMatrix = createUserItemMatrix(spark, items_file, input_file, avg_file, output_file)
    spark.stop()

