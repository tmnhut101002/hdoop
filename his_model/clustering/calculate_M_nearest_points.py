from pyspark.sql import SparkSession

def calculateNearestPoints(users_file, distance_file, output_file):
    spark = SparkSession.builder.appName("MNearestPoints").getOrCreate()
    # Load data from D.txt
    distance_data = spark.sparkContext.textFile(distance_file)
    parsed_data = distance_data.map(lambda line: tuple(map(float, line.split('\t'))))

    # Calculate number of discarded points
    users_data= spark.sparkContext.textFile(users_file)
    number_of_users = users_data.count()
    
    # Cal M nearest
    M = int(number_of_users / 4 / 1.5) + 1

    # Sort by distance and take the top M
    result = parsed_data.sortBy(lambda x: x[1]).take(M)
    result_df = spark.createDataFrame(data = result, schema = ["user", "distance"])

    result_df.write.mode("overwrite").options(header='False', delimiter='\t').csv(output_file)

    spark.stop()


if __name__ == '__main__':
    distance_file = "hdfs://localhost:9000/HM_clustering/Distance"
    users_file = "hdfs://localhost:9000/HM_clustering/User"
    output_file = "hdfs://localhost:9000/HM_clustering/M_NearestPoints"
    calculateNearestPoints(users_file, distance_file, output_file)
