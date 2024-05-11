from pyspark.sql import SparkSession

def calculateAvgRating(spark, mysql_url, mysql_properties, output_file):
    table_name = 'MovieLen100k_training'
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)

    # Lấy dữ liệu từ DataFrame và chuyển đổi thành định dạng key-value
    ratings = df.rdd.map(lambda row: (row.user_id, row.rating))

    user_totals = ratings.aggregateByKey((0, 0), lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))

    # Calculate average and output the result
    avg_ratings = user_totals.map(lambda x: (x[0], x[1][0] / float(x[1][1])))
    
    result_data = avg_ratings.toDF(["User", "Average Rating"])

    result_data.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file)

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("CalculateAVGRating") \
        .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
        .getOrCreate()

    mysql_url = "jdbc:mysql://localhost:3306/ML100?useSSL=false"
    mysql_properties = {
        "user": "root",
        "password": "Password@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    output_file = "hdfs:///Clustering_mysql/AverageRating"

    calculateAvgRating(spark, mysql_url, mysql_properties, output_file)
    spark.stop()
