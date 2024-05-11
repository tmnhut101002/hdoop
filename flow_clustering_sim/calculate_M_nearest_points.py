from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

def calculateNearestPoints(spark0, users_file, distance_file, output_file):
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

    # Write the result
    # with open(output_file,"w") as out_file:
    #     for user, distance in result:
    #         out_file.write(f"{user}\t{distance}\n")
    # out_file.close()
    spark.stop()


if __name__ == '__main__':
    spark = SparkSession.builder.appName("MNearestPoints").getOrCreate()
    distance_file = "hdfs://localhost:9000/Clustering/Distance.csv"
    users_file = "../users.txt"
    output_file = "hdfs://localhost:9000/Clustering/M_NearestPoints.csv"

    calculateNearestPoints(spark, users_file, distance_file, output_file)

# conf = SparkConf().setAppName("MNearestPoints")
# sc = SparkContext(conf=conf)

# # Load data from D.txt
# data = sc.textFile("./output/distance.txt")
# parsed_data = data.map(lambda line: tuple(map(float, line.split('\t'))))

# # Calculate number of discarded points
# users_file = sc.textFile("../users.txt")
# number_of_users = users_file.count()

# M = int(number_of_users / 4 / 1.5) + 1

# # Calculate M nearest points
# def calculate_M_nearest_points(line):
#     user, distance = line
#     return user, distance

# # Sort by distance and take the top M
# result = parsed_data.sortBy(lambda x: x[1]).take(M)

# # Print the result
# with open("./output/M_nearest_points.txt","w") as out_file:
#     for user, distance in result:
#         out_file.write(f"{user}\t{distance}\n")

# # Stop the SparkContext
    spark.stop()
