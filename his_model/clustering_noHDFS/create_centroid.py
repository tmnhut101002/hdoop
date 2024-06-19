from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def create_centroid_reducer(user, values):
    found_value = None
    for value in values:
        value = value.strip()
        if len(value.split('|')) > 1:
            found_value = value
            break
    if found_value:
        yield user, found_value


def createCentroid(user_item_matrix_df, most_important_user_df):
    # Đọc dữ liệu đầu vào thành RDD
    conf =  SparkConf().setAppName("CreateCentroid") \
        .set("spark.network.timeout","3601s") \
        .set("spark.executor.heartbeatInterval","3600s") \
        .set("spark.executor.memory", "4g") \
        .set("spark.driver.memory", "4g") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .setMaster("local[4]")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    num_partitions = 100
    user_item_matrix_rdd = spark.createDataFrame(user_item_matrix_df).rdd.repartition(num_partitions)
    most_important_user_rdd = spark.createDataFrame(most_important_user_df).rdd.repartition(num_partitions)

    # Kết hợp dữ liệu từ hai RDD
    combined_rdd = user_item_matrix_rdd.union(most_important_user_rdd)

    # Kết hợp dữ liệu từ hai RDD và áp dụng reduce
    reduced_rdd = combined_rdd.groupByKey().flatMap(lambda kv: create_centroid_reducer(kv[0], kv[1]))

    newCentroid = reduced_rdd.toDF(["User", "UIRateData"])
    newCentroid = newCentroid.toPandas()
    
    spark.stop()
    return newCentroid

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CreateCentroid").getOrCreate()

    user_item_matrix_path = "hdfs://localhost:9000/Clustering/UserItemMatrix.csv"
    most_important_user_path = "hdfs://localhost:9000/Clustering/MaxImportance.csv"
    output_path = "hdfs://localhost:9000/Clustering/Centroids.csv"
    output_path_new = "hdfs://localhost:9000/Clustering/NewCentroids.csv"

    createCentroid(spark, user_item_matrix_path, most_important_user_path, output_path, output_path_new)

    spark.stop()
