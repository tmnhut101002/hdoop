from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
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
from split_files_by_label import insertDataToDB, splitAVG, splitInput, clear_directory
from prepare_data import updateNewData

num_cluster = 5

if __name__ == '__main__':
    spark = SparkSession.builder.appName("clusteringForRS").getOrCreate()

    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()

    # Create empty schema
    columns = StructType([
            StructField('col1', StringType(), True),
            StructField('col2', StringType(), True)])

    # Create an empty RDD with empty schema
    empty = spark.createDataFrame(data = emp_RDD, schema = columns)

    empty.write.mode("overwrite").options(header='False', delimiter = '\t').csv("hdfs://localhost:9000/HM_clustering/UserItemMatrixLabel")
    empty.write.mode("overwrite").options(header='False', delimiter = '\t').csv("hdfs://localhost:9000/HM_clustering/Centroids")

    spark.stop()

    # MySQL properties
    mysql_url = "jdbc:mysql://localhost:3306/ecommerce?useSSL=false"
    mysql_properties = {
        "user": "root",
        "password": "Password@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    table_name = 'TrainingData'
    
    #Update new preview
    updateNewData()
    
    #Create User List
    spark = SparkSession.builder \
        .appName("UserList") \
        .getOrCreate()
    output_file = "hdfs://localhost:9000/HM_clustering/User"
    createUserList(spark, mysql_url, mysql_properties, output_file, table_name)

    #Create item list
    spark = SparkSession.builder \
        .appName("ItemList") \
        .getOrCreate()
    output_file = "hdfs://localhost:9000/HM_clustering/Item"
    createItemList(spark, mysql_url, mysql_properties, output_file, table_name)

    #Tinh avg rating
    spark = SparkSession.builder \
        .appName("AvgRating") \
        .getOrCreate()
    output_file = "hdfs://localhost:9000/HM_clustering/AverageRating"
    calculateAvgRating(spark, mysql_url, mysql_properties, output_file, table_name)

    #Tao ma tran item user
    avg_file = "hdfs://localhost:9000/HM_clustering/AverageRating"
    items_file = "hdfs://localhost:9000/HM_clustering/Item"
    output_file = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
    createUserItemMatrix(mysql_url, mysql_properties, items_file, avg_file, output_file, table_name)

    #Tinh do quan trong F cua user
    spark = SparkSession.builder \
        .appName("CalImportance") \
        .getOrCreate()
    output_file = "hdfs://localhost:9000/HM_clustering/Importance"
    createImportance(spark,mysql_url, mysql_properties,output_file, table_name)

    #Tinh F_max user quan trong nhat (dau tien)
    input_path = "hdfs://localhost:9000/HM_clustering/Importance"
    output_path = "hdfs://localhost:9000/HM_clustering/MaxImportance"
    getMax(input_path,output_path)

    #Tao centroid
    user_item_matrix_path = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
    most_important_user_path = "hdfs://localhost:9000/HM_clustering/MaxImportance"
    output_path = "hdfs://localhost:9000/HM_clustering/Centroids"
    output_path_new = "hdfs://localhost:9000/HM_clustering/NewCentroids"
    createCentroid(user_item_matrix_path, most_important_user_path, output_path, output_path_new)

    #Tinh khoang cach cac diem toi first centroids
    userItemMatrixFile = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
    centroidsFile = "hdfs://localhost:9000/HM_clustering/NewCentroids"
    outputFile = "hdfs://localhost:9000/HM_clustering/Distance"
    calDistance(userItemMatrixFile, centroidsFile, outputFile)

    #Get M nearest points (M = int(num_users /4/1.5) + 1)
    distance_file = "hdfs://localhost:9000/HM_clustering/Distance"
    users_file = "hdfs://localhost:9000/HM_clustering/User"
    output_file = "hdfs://localhost:9000/HM_clustering/M_NearestPoints"
    calculateNearestPoints(users_file, distance_file, output_file)

    #Discard nearest points in user_item_matrix.txt and add label
    input_path_matrix = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
    input_path_nearest_points = "hdfs://localhost:9000/HM_clustering/M_NearestPoints"
    output_path = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
    user_in_cluster_path = "hdfs://localhost:9000/HM_clustering/UserItemMatrixLabel"
    discard_nearest_points(input_path_matrix, input_path_nearest_points, output_path, user_in_cluster_path)

    #Discard nearest points in importance file
    input_path_matrix = "hdfs://localhost:9000/HM_clustering/Importance"
    input_path_nearest_points = "hdfs://localhost:9000/HM_clustering/M_NearestPoints"
    output_path = "hdfs://localhost:9000/HM_clustering/Importance"
    discard_nearest_points_F(input_path_matrix, input_path_nearest_points, output_path)

    #Loop cho cac cluster con lai
    for i in range(num_cluster-1):
        #Tinh khoang cach cac diem toi first centroids
        userItemMatrixFile = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
        centroidsFile = "hdfs://localhost:9000/HM_clustering/NewCentroids"
        outputFile = "hdfs://localhost:9000/HM_clustering/Distance"
        calDistance (userItemMatrixFile, centroidsFile, outputFile)
        
        #Find F max for scaling
        input_path = "hdfs://localhost:9000/HM_clustering/Importance"
        output_path = "hdfs://localhost:9000/HM_clustering/MaxDistance"
        getMax(input_path, output_path)

        #Scaling F
        input_file_1 = "hdfs://localhost:9000/HM_clustering/Importance"
        input_file_2 = "hdfs://localhost:9000/HM_clustering/MaxImportance"
        output_file = "hdfs://localhost:9000/HM_clustering/NewImportance"
        calculateScaling(input_file_1, input_file_2, output_file)

        #Find Dmin max for scaling
        input_path_d = "hdfs://localhost:9000/HM_clustering/Distance"
        output_path_d = "hdfs://localhost:9000/HM_clustering/MaxDistance"
        getMax(input_path_d, output_path_d)

        #Scaling D
        input_file_1d = "hdfs://localhost:9000/HM_clustering/Distance"
        input_file_2d = "hdfs://localhost:9000/HM_clustering/MaxDistance"
        output_file_d = "hdfs://localhost:9000/HM_clustering/NewDistance"
        calculateScaling(input_file_1d, input_file_2d, output_file_d)

        #Sum F and D
        input_file_F = "hdfs://localhost:9000/HM_clustering/NewImportance"
        input_file_D = "hdfs://localhost:9000/HM_clustering/NewDistance"
        output_file= "hdfs://localhost:9000/HM_clustering/SumFD"
        sumFD(input_file_F, input_file_D, output_file)

        #Find user has sumFD being max
        input_path = "hdfs://localhost:9000/HM_clustering/SumFD"
        output_path = "hdfs://localhost:9000/HM_clustering/MaxSumFD"
        getMax(input_path, output_path)

        #Create centroid
        user_item_matrix_path = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
        most_important_user_path = "hdfs://localhost:9000/HM_clustering/MaxSumFD"
        output_path = "hdfs://localhost:9000/HM_clustering/Centroids"
        output_path_new = "hdfs://localhost:9000/HM_clustering/NewCentroids"
        createCentroid(user_item_matrix_path, most_important_user_path, output_path, output_path_new)

        #cal distance from user to new centroid
        userItemMatrixFile = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
        centroidsFile = "hdfs://localhost:9000/HM_clustering/NewCentroids"
        outputFile = "hdfs://localhost:9000/HM_clustering/Distance"
        calDistance(userItemMatrixFile, centroidsFile, outputFile)

        #cal M nearest points
        distance_file = "hdfs://localhost:9000/HM_clustering/Distance"
        users_file = "hdfs://localhost:9000/HM_clustering/User"
        output_file = "hdfs://localhost:9000/HM_clustering/M_NearestPoints"
        calculateNearestPoints(users_file, distance_file, output_file)

        #Discard nearest point in user_item_matrix and add label for nearest point
        input_path_matrix = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
        input_path_nearest_points = "hdfs://localhost:9000/HM_clustering/M_NearestPoints"
        output_path = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
        user_in_cluster_path = "hdfs://localhost:9000/HM_clustering/UserItemMatrixLabel"
        discard_nearest_points(input_path_matrix, input_path_nearest_points, output_path, user_in_cluster_path)

        #Discard nearest point in F
        input_path_matrix = "hdfs://localhost:9000/HM_clustering/Importance"
        input_path_nearest_points = "hdfs://localhost:9000/HM_clustering/M_NearestPoints"
        output_path = "hdfs://localhost:9000/HM_clustering/Importance"
        discard_nearest_points_F(input_path_matrix, input_path_nearest_points, output_path)

    #Xu ly user chua co cum => dua user vao cum co centroid gan minh nhat
    userItemMatrixFile = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
    centroidsFile = "hdfs://localhost:9000/HM_clustering/Centroids"
    outputFile = "hdfs://localhost:9000/HM_clustering/ToClusterUser"
    user_in_cluster_path = "hdfs://localhost:9000/HM_clustering/UserItemMatrixLabel"
    devideNonClusterUser (userItemMatrixFile, centroidsFile, outputFile, user_in_cluster_path)

    #Lam gon label
    user_in_cluster_path = "hdfs://localhost:9000/HM_clustering/UserItemMatrixLabel"
    user_label = "hdfs://localhost:9000/HM_clustering/Label"
    simpleLabel(user_in_cluster_path, user_label)

    spark = SparkSession.builder \
        .appName("SplitFile") \
        .getOrCreate()
    insertDataToDB(spark, mysql_url, mysql_properties, table_name)
    output_directory = "../mfps_v2/temp_preSIM"
    clear_directory(output_directory)
    splitInput(mysql_url, mysql_properties)
    splitAVG(mysql_url, mysql_properties)
