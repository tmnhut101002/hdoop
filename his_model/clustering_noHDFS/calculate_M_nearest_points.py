from pyspark.sql import SparkSession

def calculateNearestPoints(users_df, distance_df):
    spark = SparkSession.builder.appName("MNearestPoints").getOrCreate()
    # Load data from D.txt
    distance_data = spark.createDataFrame(distance_df).rdd
    parsed_data = distance_data#.map(lambda line: tuple(map(float, line.split('\t'))))

    # Calculate number of discarded points
    users_data= spark.createDataFrame(users_df).rdd
    number_of_users = users_data.count()
    
    # Cal M nearest
    M = int(number_of_users / 4 / 1.5) + 1

    # Sort by distance and take the top M
    result = parsed_data.sortBy(lambda x: x[1]).take(M)
    result_df = spark.createDataFrame(data = result, schema = ["User", "Distance"])
    result_df = result_df.toPandas()
    spark.stop()
    return result_df


if __name__ == '__main__':
    spark = SparkSession.builder.appName("MNearestPoints").getOrCreate()
    distance_file = "hdfs://localhost:9000/Clustering/Distance.csv"
    users_file = "../users.txt"
    output_file = "hdfs://localhost:9000/Clustering/M_NearestPoints.csv"

    calculateNearestPoints(spark, users_file, distance_file, output_file)

    spark.stop()
