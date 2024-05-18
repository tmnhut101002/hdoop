from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
import os
import timeit
from calculate_M_nearest_points import calculateNearestPoints
from calculate_scaling import calculateScaling
from calculate_sumFD import sumFD
from create_centroid import createCentroid
from create_importance import createImportance
from create_item_list import createItemList
from create_user_item_matrix import createUserItemMatrix
from discard_nearest_points import discard_nearest_points
from discard_nearest_points_F import discard_nearest_points as discard_nearest_points_F
from get_max import getMax
from calculate_distance import calDistance
from calculate_distance_for_noncluster_user import devideNonClusterUser
from label import simpleLabel
from create_user_list import createUserList
from calculate_avg_rating import calculateAvgRating

num_cluster = 5
USER_FILE = "hdfs:///HM_clustering/User"
i = 1

if __name__ == '__main__':
    start = timeit.default_timer()
    spark = SparkSession.builder.appName("clusteringForRS").getOrCreate()

    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()

    # Create empty schema
    columns = StructType([
            StructField('col1', StringType(), True),
            StructField('col2', StringType(), True)])


    # Create an empty RDD with empty schema
    empty = spark.createDataFrame(data = emp_RDD,
                                schema = columns)

    empty.write.mode("overwrite").options(header='False', delimiter = '\t').csv("hdfs:///HM_clustering/UserItemMatrixLabel")
    empty.write.mode("overwrite").options(header='False', delimiter = '\t').csv("hdfs:///HM_clustering/Centroids")

    spark.stop()

    mysql_url = "jdbc:mysql://localhost:3306/ecommerce?useSSL=false"
    mysql_properties = {
        "user": "root",
        "password": "Password@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    #Tao User List
    spark = SparkSession.builder \
        .appName("UserList") \
        .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
        .getOrCreate()
    output_file = "hdfs:///HM_clustering/User"
    createUserList(spark, mysql_url, mysql_properties, output_file)
    spark.stop()

    #Tao item list
    spark = SparkSession.builder \
        .appName("ItemList") \
        .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
        .getOrCreate()
    output_file = "hdfs:///HM_clustering/Item"
    createItemList(spark, mysql_url, mysql_properties, output_file)
    spark.stop()

    #Tinh avg rating
    spark = SparkSession.builder \
        .appName("AvgRating") \
        .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
        .getOrCreate()
    output_file = "hdfs:///HM_clustering/AverageRating"
    calculateAvgRating(spark, mysql_url, mysql_properties, output_file)
    spark.stop()


    #Tao ma tran item user
    spark = SparkSession.builder \
        .appName("UserItemMatrix") \
        .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
        .getOrCreate()
    avg_file = "hdfs:///HM_clustering/AverageRating"
    items_file = "hdfs:///HM_clustering/Item"
    output_file = "hdfs:///HM_clustering/UserItemMatrix"
    createUserItemMatrix(spark, mysql_url, mysql_properties, items_file, avg_file, output_file)
    spark.stop()

    #Tinh do quan trong F cua user
    spark = SparkSession.builder \
        .appName("CalImportance") \
        .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
        .getOrCreate()
    output_file = "hdfs:///HM_clustering/Importance"
    createImportance(spark,mysql_url, mysql_properties,output_file)
    spark.stop()
    spark = 0

    #Tinh F_max user quan trong nhat (dau tien)
    input_path = "hdfs:///HM_clustering/Importance"
    output_path = "hdfs:///HM_clustering/MaxImportance"
    getMax(spark,input_path,output_path)

    #Tao centroid
    user_item_matrix_path = "hdfs:///HM_clustering/UserItemMatrix"
    most_important_user_path = "hdfs:///HM_clustering/MaxImportance"
    output_path = "hdfs:///HM_clustering/Centroids"
    output_path_new = "hdfs:///HM_clustering/NewCentroids"
    createCentroid(spark, user_item_matrix_path, most_important_user_path, output_path, output_path_new)

    #Tinh khoang cach cac diem toi first centroids
    userItemMatrixFile = "hdfs:///HM_clustering/UserItemMatrix"
    centroidsFile = "hdfs:///HM_clustering/NewCentroids"
    outputFile = "hdfs:///HM_clustering/Distance"
    calDistance (spark, userItemMatrixFile, centroidsFile, outputFile)

    #Get M nearest points (M = int(num_users /4/1.5) + 1)
    distance_file = "hdfs:///HM_clustering/Distance"
    users_file = "hdfs:///HM_clustering/User"
    output_file = "hdfs:///HM_clustering/M_NearestPoints"
    calculateNearestPoints(spark, users_file, distance_file, output_file)

    #Discard nearest points in user_item_matrix.txt and add label
    input_path_matrix = "hdfs:///HM_clustering/UserItemMatrix"
    input_path_nearest_points = "hdfs:///HM_clustering/M_NearestPoints"
    output_path = "hdfs:///HM_clustering/UserItemMatrix"
    user_in_cluster_path = "hdfs:///HM_clustering/UserItemMatrixLabel"
    discard_nearest_points(spark, input_path_matrix, input_path_nearest_points, output_path, user_in_cluster_path)

    #Discard nearest points in importance file
    input_path_matrix = "hdfs:///HM_clustering/Importance"
    input_path_nearest_points = "hdfs:///HM_clustering/M_NearestPoints"
    output_path = "hdfs:///HM_clustering/Importance"
    discard_nearest_points_F(spark, input_path_matrix, input_path_nearest_points, output_path)

    #Loop cho cac cluster con lai
    for i in range(num_cluster-1):
        #Tinh khoang cach cac diem toi first centroids
        userItemMatrixFile = "hdfs:///HM_clustering/UserItemMatrix"
        centroidsFile = "hdfs:///HM_clustering/NewCentroids"
        outputFile = "hdfs:///HM_clustering/Distance"
        calDistance (spark, userItemMatrixFile, centroidsFile, outputFile)
        
        #Find F max for scaling
        input_path = "hdfs:///HM_clustering/Importance"
        output_path = "hdfs:///HM_clustering/MaxDistance"
        getMax(spark,input_path, output_path)

        #Scaling F
        input_file_1 = "hdfs:///HM_clustering/Importance"
        input_file_2 = "hdfs:///HM_clustering/MaxImportance"
        output_file = "hdfs:///HM_clustering/NewImportance"
        calculateScaling(spark, input_file_1, input_file_2, output_file)

        #Find Dmin max for scaling
        input_path_d = "hdfs:///HM_clustering/Distance"
        output_path_d = "hdfs:///HM_clustering/MaxDistance"
        getMax(spark,input_path_d, output_path_d)

        #Scaling D
        input_file_1d = "hdfs:///HM_clustering/Distance"
        input_file_2d = "hdfs:///HM_clustering/MaxDistance"
        output_file_d = "hdfs:///HM_clustering/NewDistance"
        calculateScaling(spark, input_file_1d, input_file_2d, output_file_d)

        #Sum F and D
        input_file_F = "hdfs:///HM_clustering/NewImportance"
        input_file_D = "hdfs:///HM_clustering/NewDistance"
        output_file= "hdfs:///HM_clustering/SumFD"
        sumFD(spark,input_file_F,input_file_D, output_file)

        #Find user has sumFD being max
        input_path = "hdfs:///HM_clustering/SumFD"
        output_path = "hdfs:///HM_clustering/MaxSumFD"
        getMax(spark,input_path, output_path)

        #Create centroid
        user_item_matrix_path = "hdfs:///HM_clustering/UserItemMatrix"
        most_important_user_path = "hdfs:///HM_clustering/MaxSumFD"
        output_path = "hdfs:///HM_clustering/Centroids"
        output_path_new = "hdfs:///HM_clustering/NewCentroids"
        createCentroid(spark, user_item_matrix_path, most_important_user_path, output_path, output_path_new)

        #cal distance from user to new centroid
        userItemMatrixFile = "hdfs:///HM_clustering/UserItemMatrix"
        centroidsFile = "hdfs:///HM_clustering/NewCentroids"
        outputFile = "hdfs:///HM_clustering/Distance"
        calDistance (spark, userItemMatrixFile, centroidsFile, outputFile)

        #cal M nearest points
        distance_file = "hdfs:///HM_clustering/Distance"
        users_file = "hdfs:///HM_clustering/User"
        output_file = "hdfs:///HM_clustering/M_NearestPoints"
        calculateNearestPoints(spark, users_file, distance_file, output_file)

        #Discard nearest point in user_item_matrix and add label for nearest point
        input_path_matrix = "hdfs:///HM_clustering/UserItemMatrix"
        input_path_nearest_points = "hdfs:///HM_clustering/M_NearestPoints"
        output_path = "hdfs:///HM_clustering/UserItemMatrix"
        user_in_cluster_path = "hdfs:///HM_clustering/UserItemMatrixLabel"
        discard_nearest_points(spark, input_path_matrix, input_path_nearest_points, output_path, user_in_cluster_path)

        #Discard nearest point in F
        input_path_matrix = "hdfs:///HM_clustering/Importance"
        input_path_nearest_points = "hdfs:///HM_clustering/M_NearestPoints"
        output_path = "hdfs:///HM_clustering/Importance"
        discard_nearest_points_F(spark, input_path_matrix, input_path_nearest_points, output_path)
        i+=1

    #Xu ly user chua co cum => dua user vao cum co centroid gan minh nhat
    userItemMatrixFile = "hdfs:///HM_clustering/UserItemMatrix"
    centroidsFile = "hdfs:///HM_clustering/Centroids"
    outputFile = "hdfs:///HM_clustering/ToClusterUser"
    user_in_cluster_path = "hdfs:///HM_clustering/UserItemMatrixLabel"
    devideNonClusterUser (spark, userItemMatrixFile, centroidsFile, outputFile, user_in_cluster_path)

    #Lam gon label
    user_in_cluster_path = "hdfs:///HM_clustering/UserItemMatrixLabel"
    user_label = "hdfs:///HM_clustering/Label"
    simpleLabel(spark, user_in_cluster_path, user_label)

    stop = timeit.default_timer()
    print('Time: ', stop - start)