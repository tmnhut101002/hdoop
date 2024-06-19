from pyspark.sql import SparkSession
from calculate_avg_rating import calculateAvgRating
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
from pyspark.sql.types import StructType,StructField, StringType
import timeit
import pandas as pd

num_cluster = 5
LABEL_FILE = "./output/label.csv"

if __name__ ==  '__main__':
    start = timeit.default_timer()
    spark = SparkSession.builder.appName("clusteringForRS").getOrCreate()

    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()
    
    # Create empty schema
    columns = StructType([
            StructField('col1', StringType(), True),
            StructField('col2', StringType(), True)])

    # Create an empty RDD with empty schema
    empty = spark.createDataFrame(data = emp_RDD, schema = columns)
    spark.stop()
    
    mysql_url = "jdbc:mysql://localhost:3306/ecommerce?useSSL=false"
    mysql_properties = {
        "user": "root",
        "password": "Password@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    table_name = 'TrainingData'
    
    #Tao user list
    uList = createUserList(table_name, mysql_url, mysql_properties)

    #Tao item list
    iList = createItemList(table_name, mysql_url, mysql_properties)

    #Tinh avg rating
    output_file_path = "../mfps_v2/output/avg.txt"
    avgRating = calculateAvgRating(table_name, mysql_url, mysql_properties, output_file_path)

    #Tao ma tran item user
    uiMatrix = createUserItemMatrix(iList, table_name, mysql_url, mysql_properties, avgRating)

    #Tinh do quan trong F cua user
    importance_df = createImportance(table_name, mysql_url, mysql_properties)

    #Tinh F_max user quan trong nhat (dau tien)
    maxF_df = getMax(importance_df)

    #Tao centroid
    centroids_df = pd.DataFrame(columns=['User', 'UIRateData'])
    newCentroid_df= createCentroid(uiMatrix, maxF_df)
    centroids_df = pd.concat([centroids_df, newCentroid_df], ignore_index=True, sort=False)

    #Tinh khoang cach cac diem toi first centroids
    distance_df = calDistance (uiMatrix, newCentroid_df)

    #Get M nearest points (M = int(num_users /4/1.5) + 1)
    mNearestPoints_df = calculateNearestPoints(uList, distance_df)

    #Discard nearest points in user_item_matrix.txt and add label
    uLabel = pd.DataFrame(columns=['user','label'])

    labelValues_df, uiMatrix = discard_nearest_points(uiMatrix, mNearestPoints_df)
    uLabel = pd.concat([uLabel, labelValues_df], ignore_index=True, sort=False)

    #Discard nearest points in importance file
    importance_df = discard_nearest_points_F(importance_df, mNearestPoints_df)

    #Loop cho cac cluster con lai
    for i in range(num_cluster-1):
        #Cal distance from users to centroid
        distance_df = calDistance (uiMatrix, newCentroid_df)

        #Find F max for scaling
        maxF_df = getMax(importance_df)

        #Scaling F
        newF_df = calculateScaling(importance_df, maxF_df)

        #Find Dmin max for scaling
        maxD_df = getMax(distance_df)

        #Scaling D
        newD_df = calculateScaling(distance_df, maxD_df)

        #Sum F and D
        sumFD_df = sumFD(newF_df, newD_df)

        #Find user has sumFD being max
        maxSumFD_df = getMax(sumFD_df)

        #Create centroid
        newCentroid_df = createCentroid(uiMatrix, maxSumFD_df)
        centroids_df = pd.concat([centroids_df, newCentroid_df], ignore_index=True, sort=False)

        #cal distance from user to new centroid
        distance_df = calDistance (uiMatrix, newCentroid_df)

        #cal M nearest points
        mNearestPoints_df = calculateNearestPoints(uList, distance_df)

        #Discard nearest point in user_item_matrix and add label for nearest point
        labelValues_df, uiMatrix = discard_nearest_points(uiMatrix, mNearestPoints_df)
        uLabel = pd.concat([uLabel, labelValues_df], ignore_index=True, sort=False)
        
        #Discard nearest point in F
        importance_df = discard_nearest_points_F(importance_df, mNearestPoints_df)

    #Xu ly user chua co cum => dua user vao cum co centroid gan minh nhat
    labelValues_df, clusterUser_df = devideNonClusterUser (uiMatrix, centroids_df)
    uLabel = pd.concat([uLabel, labelValues_df], ignore_index=True, sort=False)
    
    #Lam gon label
    user_label = LABEL_FILE
    simpleLabel(uLabel, user_label)

    stop = timeit.default_timer()
    print('Time: ', stop - start)

