from pyspark.sql import SparkSession

def calculateAvgRating(table_name, mysql_url, mysql_properties, output_file):
    spark = SparkSession.builder \
        .appName("AvgRating") \
        .getOrCreate()
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    
    # Lấy dữ liệu từ DataFrame và chuyển đổi thành định dạng key-value
    ratings = df.rdd.map(lambda row: (row.user_id, row.rating))

    user_totals = ratings.aggregateByKey((0, 0), lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))

    # Calculate average and output the result
    avg_ratings = user_totals.map(lambda x: (x[0], x[1][0] / float(x[1][1])))

    # with open(output_file, "w") as output_file:
    #     for user, avg_rating in avg_ratings.collect():
    #         output_file.write(f"{user}\t{avg_rating}\n")

    result_data = avg_ratings.toDF(["User", "Average Rating"])
    result_data = result_data.toPandas()

    # Stop the Spark session
    spark.stop()
    return result_data

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("CalculateAVGRating") \
        .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
        .getOrCreate()

    spark.stop()

