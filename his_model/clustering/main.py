from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, FloatType
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
from split_files_by_label import splitClusterInfo
from prepare_data import updateNewData
from update_centroids import updateCentroid
import datetime
import numpy as np

HDFS_HOST = 'localhost:9000'
NUM_CLUSTER = 5

if __name__ == '__main__':
    start = datetime.datetime.now()
    # Tạo Spark Session
    spark = SparkSession.builder.appName("clusteringForRS").getOrCreate()
    
    # Khởi tạo một cấu trúc RDD
    emp_RDD = spark.sparkContext.emptyRDD()
    columns = StructType([StructField('col1', StringType(), True), StructField('col2', StringType(), True)])
    empty = spark.createDataFrame(data = emp_RDD, schema = columns)
    empty.write.mode("overwrite").options(header='False', delimiter = '\t').csv(f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrixLabel")
    empty.write.mode("overwrite").options(header='False', delimiter = '\t').csv(f"hdfs://{HDFS_HOST}/HM_clustering/Centroids")
    
    columns = StructType([StructField('col1', StringType(), True), StructField('col2', FloatType(), True)])
    empty.write.mode("overwrite").options(header='False', delimiter = '\t').csv(f"hdfs://{HDFS_HOST}/HM_clustering/SumFD")
    spark.stop()
    
    host = 'localhost'
    port = '3306'
    user = 'root'
    password = '1234'
    
    mysql_url = f"jdbc:mysql://{host}:{port}/ecommerce?useSSL=false"
    mysql_properties = {
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    table_name = 'TrainingData'
    
    print("Num cluster:", NUM_CLUSTER)
    # Hàm cập nhật đánh giá mới
    print('Update new rating data...')
    updateNewData()
    
    # Tạo List User
    print('Create User List...')
    spark = SparkSession.builder \
        .appName("UserList") \
        .getOrCreate()
    output_file = f"hdfs://{HDFS_HOST}/HM_clustering/User"
    createUserList(spark, mysql_url, mysql_properties, output_file, table_name)

    # Tạo list Item
    print("Create Item list...")
    spark = SparkSession.builder \
        .appName("ItemList") \
        .getOrCreate()
    output_file = f"hdfs://{HDFS_HOST}/HM_clustering/Item"
    createItemList(spark, mysql_url, mysql_properties, output_file, table_name)

    print("Calculate AVG Rating foreach User...")
    # Tính Rating trung bình
    spark = SparkSession.builder \
        .appName("AvgRating") \
        .getOrCreate()
    output_file = f"hdfs://{HDFS_HOST}/HM_clustering/AverageRating"
    calculateAvgRating(spark, mysql_url, mysql_properties, output_file, table_name)

    # Tạo Ma trận User x Item
    print('Create User x Item matrix...')
    avg_file = f"hdfs://{HDFS_HOST}/HM_clustering/AverageRating"
    items_file = f"hdfs://{HDFS_HOST}/HM_clustering/Item"
    output_file = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrix"
    output_file_cluster = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrixCluster"
    createUserItemMatrix(mysql_url, mysql_properties, items_file, avg_file, output_file, table_name, output_file_cluster)

    # Tính độ quan trọng (F) của từng User
    print('Calculate Importance (F)...')
    spark = SparkSession.builder \
        .appName("CalImportance") \
        .getOrCreate()
    output_file = f"hdfs://{HDFS_HOST}/HM_clustering/Importance"
    createImportance(spark,mysql_url, mysql_properties,output_file, table_name)

    # Tính F_max user quan trọng nhất (đầu tiên)
    print('Get F max...')
    input_path = f"hdfs://{HDFS_HOST}/HM_clustering/Importance"
    output_path = f"hdfs://{HDFS_HOST}/HM_clustering/MaxImportance"
    getMax(input_path,output_path)

    # Tạo centroid (trọng tâm)
    print('Create Centroid...')
    user_item_matrix_path = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrix"
    most_important_user_path = f"hdfs://{HDFS_HOST}/HM_clustering/MaxImportance"
    output_path = f"hdfs://{HDFS_HOST}/HM_clustering/Centroids"
    output_path_new = f"hdfs://{HDFS_HOST}/HM_clustering/NewCentroids"
    createCentroid(user_item_matrix_path, most_important_user_path, output_path, output_path_new)

    # Tính khoảng cách đến trọng tâm đầu tiên
    print('Calculate Distance (D)...')
    userItemMatrixFile = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrix"
    centroidsFile = f"hdfs://{HDFS_HOST}/HM_clustering/NewCentroids"
    outputFile = f"hdfs://{HDFS_HOST}/HM_clustering/Distance"
    calDistance(userItemMatrixFile, centroidsFile, outputFile)

    # Lấy M điểm gần nhất với trọng tâm (M = int(num_users / NUM_CLUSTER / 1.5) + 1)
    print('Calculate Nearest Points...')
    distance_file = f"hdfs://{HDFS_HOST}/HM_clustering/Distance"
    users_file = f"hdfs://{HDFS_HOST}/HM_clustering/User"
    output_file = f"hdfs://{HDFS_HOST}/HM_clustering/M_NearestPoints"
    calculateNearestPoints(users_file, distance_file, NUM_CLUSTER, output_file)

    # Loại bỏ các điểm gần nhất trong User x Item và gán nhãn (label)
    print('Discard Nearest Points...')
    input_path_matrix = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrix"
    input_path_nearest_points = f"hdfs://{HDFS_HOST}/HM_clustering/M_NearestPoints"
    output_path = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrix"
    user_in_cluster_path = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrixLabel"
    discard_nearest_points(input_path_matrix, input_path_nearest_points, output_path, user_in_cluster_path)

    # Loại bỏ các điểm gần nhất trong F
    print('Discard Nearest Points in F...')
    input_path_matrix = f"hdfs://{HDFS_HOST}/HM_clustering/Importance"
    input_path_nearest_points = f"hdfs://{HDFS_HOST}/HM_clustering/M_NearestPoints"
    output_path = f"hdfs://{HDFS_HOST}/HM_clustering/Importance"
    discard_nearest_points_F(input_path_matrix, input_path_nearest_points, output_path)

    # Tính khoảng cách tới trọng tâm đối với các user còn lại reong matrix UserItem
    print('Calculate distance after discard UI matrix...')
    userItemMatrixFile = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrix"
    centroidsFile = f"hdfs://{HDFS_HOST}/HM_clustering/NewCentroids"
    outputFile = f"hdfs://{HDFS_HOST}/HM_clustering/Distance"
    calDistance (userItemMatrixFile, centroidsFile, outputFile)
    
    # Loop 
    for i in range(NUM_CLUSTER - 1):
        print('\nLoop', i)
        
        # Tìm F_max cho scaling
        print('Get F max...')
        input_path = f"hdfs://{HDFS_HOST}/HM_clustering/Importance"
        output_path = f"hdfs://{HDFS_HOST}/HM_clustering/MaxDistance"
        getMax(input_path, output_path)

        # Scaling F
        print('Scaling F...')
        input_file_1 = f"hdfs://{HDFS_HOST}/HM_clustering/Importance"
        input_file_2 = f"hdfs://{HDFS_HOST}/HM_clustering/MaxImportance"
        output_file = f"hdfs://{HDFS_HOST}/HM_clustering/NewImportance"
        calculateScaling(input_file_1, input_file_2, output_file)

        # Tìm D_min max cho scaling
        print('Get D max...')
        input_path_d = f"hdfs://{HDFS_HOST}/HM_clustering/Distance"
        output_path_d = f"hdfs://{HDFS_HOST}/HM_clustering/MaxDistance"
        getMax(input_path_d, output_path_d)

        # Scaling D
        print('Scaling D...')
        input_file_1d = f"hdfs://{HDFS_HOST}/HM_clustering/Distance"
        input_file_2d = f"hdfs://{HDFS_HOST}/HM_clustering/MaxDistance"
        output_file_d = f"hdfs://{HDFS_HOST}/HM_clustering/NewDistance"
        calculateScaling(input_file_1d, input_file_2d, output_file_d)

        # Tổng F và D
        print('Sum (F,D)')
        input_file_F = f"hdfs://{HDFS_HOST}/HM_clustering/NewImportance"
        input_file_D = f"hdfs://{HDFS_HOST}/HM_clustering/NewDistance"
        output_file= f"hdfs://{HDFS_HOST}/HM_clustering/SumFD"
        sumFD(input_file_F, input_file_D, output_file)

        # Tìm người dùng có sumFD max
        print('Get SumFD max)')
        input_path = f"hdfs://{HDFS_HOST}/HM_clustering/SumFD"
        output_path = f"hdfs://{HDFS_HOST}/HM_clustering/MaxSumFD"
        getMax(input_path, output_path)

        # Tạo centroid
        print('Create Centroid...')
        user_item_matrix_path = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrix"
        most_important_user_path = f"hdfs://{HDFS_HOST}/HM_clustering/MaxSumFD"
        output_path = f"hdfs://{HDFS_HOST}/HM_clustering/Centroids"
        output_path_new = f"hdfs://{HDFS_HOST}/HM_clustering/NewCentroids"
        createCentroid(user_item_matrix_path, most_important_user_path, output_path, output_path_new)

        # Khoảng cách cal từ người dùng tới trọng tâm mới
        print('Calculate Distance...')
        userItemMatrixFile = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrix"
        centroidsFile = f"hdfs://{HDFS_HOST}/HM_clustering/NewCentroids"
        outputFile = f"hdfs://{HDFS_HOST}/HM_clustering/Distance"
        calDistance(userItemMatrixFile, centroidsFile, outputFile)

        # Tính M nearest points
        print('Calculate Nearest Points...')
        distance_file = f"hdfs://{HDFS_HOST}/HM_clustering/Distance"
        users_file = f"hdfs://{HDFS_HOST}/HM_clustering/User"
        output_file = f"hdfs://{HDFS_HOST}/HM_clustering/M_NearestPoints"
        calculateNearestPoints(users_file, distance_file, NUM_CLUSTER, output_file)

        # Loại bỏ các điểm gần nhất trong User x Item
        print('Discard Nearest Points...')
        input_path_matrix = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrix"
        input_path_nearest_points = f"hdfs://{HDFS_HOST}/HM_clustering/M_NearestPoints"
        output_path = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrix"
        user_in_cluster_path = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrixLabel"
        discard_nearest_points(input_path_matrix, input_path_nearest_points, output_path, user_in_cluster_path)

        # Loại bỏ các điểm gần nhất trong F
        print('Discard Nearest Points in F...')
        input_path_matrix = f"hdfs://{HDFS_HOST}/HM_clustering/Importance"
        input_path_nearest_points = f"hdfs://{HDFS_HOST}/HM_clustering/M_NearestPoints"
        output_path = f"hdfs://{HDFS_HOST}/HM_clustering/Importance"
        discard_nearest_points_F(input_path_matrix, input_path_nearest_points, output_path)
        
        # Loại bỏ các điểm trong sumFD
        print('Discard Nearest Points in sumFD...')
        input_path_matrix = f"hdfs://{HDFS_HOST}/HM_clustering/SumFD"
        input_path_nearest_points = f"hdfs://{HDFS_HOST}/HM_clustering/M_NearestPoints"
        output_path = f"hdfs://{HDFS_HOST}/HM_clustering/SumFD"
        discard_nearest_points_F(input_path_matrix, input_path_nearest_points, output_path)
        
    # Xử lý các user => Đưa vào cụm gần nhất
    print("\nDevide User into Cluster...")
    userItemMatrixFile = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrixCluster"
    centroidsFile = f"hdfs://{HDFS_HOST}/HM_clustering/Centroids"
    user_in_cluster_path = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrixLabel"
    labelItemRating = devideNonClusterUser (userItemMatrixFile, centroidsFile, user_in_cluster_path)
    
    # Cập nhật trọng tâm cụm
    print("\nUpdate Centroids...")
    updateCheck = np.array([])
    while True:
        labKey, labVal, updateCent_1 = updateCentroid(labelItemRating)
        # print (labVal)
        # print(updateCheck)
        labelItemRating = devideNonClusterUser(userItemMatrixFile ,None, user_in_cluster_path, updateCent_1)
        if (np.array_equal(labVal, updateCheck)):
            break
        else:
            updateCheck = labVal

    # Làm gọn label
    print("Simple Label...")
    user_in_cluster_path = f"hdfs://{HDFS_HOST}/HM_clustering/UserItemMatrixLabel"
    user_label = f"hdfs://{HDFS_HOST}/HM_clustering/Label"
    simpleLabel(user_in_cluster_path, user_label)

    # Tách dữ liệu, chuẩn bị file từng cụm làm đầu vào cho tính Sim
    print('Write clustering result...')
    spark = SparkSession.builder \
        .appName("SplitFile") \
        .getOrCreate()
    splitClusterInfo(spark, mysql_url, mysql_properties, table_name)
    end = datetime.datetime.now()
    print('>>> Total time for clustering:', (end-start))
