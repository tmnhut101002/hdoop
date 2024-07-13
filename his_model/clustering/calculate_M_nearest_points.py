from pyspark.sql import SparkSession

def calculateNearestPoints(users_file, distance_file, num_cluster, output_file):
    spark = SparkSession.builder.appName("MNearestPoints").getOrCreate()
    # Lấy dữ liệu từ D
    distance_data = spark.sparkContext.textFile(distance_file)
    parsed_data = distance_data.map(lambda line: tuple(map(float, line.split('\t'))))

    # Tính số lượng điểm loại bỏ
    users_data= spark.sparkContext.textFile(users_file)
    number_of_users = users_data.count()
    
    # Tính M nearest
    M = int(number_of_users / num_cluster / 1.5) + 1

    # Sắp xếp, lấy top M
    result = parsed_data.sortBy(lambda x: x[1]).take(M)
    result_df = spark.createDataFrame(data = result, schema = ["user", "distance"])

    # Ghi kết quả vào HDFS
    result_df.write.mode("overwrite").options(header='False', delimiter='\t').csv(output_file)

    spark.stop()
