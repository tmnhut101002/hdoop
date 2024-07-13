from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def create_centroid_mapper(row):
    user, value = row.strip().split('\t')
    return user, value

def create_centroid_reducer(user, values):
    values = list(values)
    if (len(values) > 1):
        for value in values:
            value = value.strip()
            if (len(value.split('|')) > 1):
                yield user, value
                return

def createCentroid(user_item_matrix_path, most_important_user_path, output_path_centroids, output_path_new_centroid):
    conf =  SparkConf().setAppName("CreateCentroid") \
        .set("spark.network.timeout","3601s") \
        .set("spark.executor.heartbeatInterval","3600s") \
        .setMaster("local[4]")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    # Đọc dữ liệu đầu vào thành RDD
    user_item_matrix_rdd = spark.sparkContext.textFile(user_item_matrix_path)
    most_important_user_rdd = spark.sparkContext.textFile(most_important_user_path)

    # Bước ánh xạ cho user_item_matrix_rdd
    mapped_user_item_matrix_rdd = user_item_matrix_rdd.map(create_centroid_mapper)

    # Bước ánh xạ cho most_important_user_rdd
    mapped_most_important_user_rdd = most_important_user_rdd.map(create_centroid_mapper)

    # Kết hợp dữ liệu từ hai RDD
    combined_rdd = mapped_user_item_matrix_rdd.union(mapped_most_important_user_rdd)

    # Kết hợp dữ liệu từ hai RDD và áp dụng reduce
    reduced_rdd = combined_rdd.groupByKey().flatMap(lambda kv: create_centroid_reducer(kv[0], kv[1]))

    # Lưu kết quả
    results = reduced_rdd.toDF()
    results.write.mode("overwrite").options(header='False', delimiter='\t').csv(output_path_new_centroid)

    results = reduced_rdd.toDF(["User", "Centroid"])
    results.write.mode("append").options(header='False', delimiter='\t').csv(output_path_centroids)
    spark.stop()
