from pyspark.sql import SparkSession
import os

#Su dung cho ca F va D
def calculateScaling (input_df, max_input_df):

    spark = SparkSession.builder.appName('caculateScaling').getOrCreate()
    #Read input file and convert to RDD
    lines_input_file_1= spark.createDataFrame(input_df,['user','values'])
    user_importance_rdd = lines_input_file_1.rdd # RDD of tuples: (user,F or D)

    lines_input_file_2 = spark.createDataFrame(max_input_df, ['user','max_value'])
    user_max_importance_rdd = lines_input_file_2.rdd # RDD of tuples: (user,max_F or max_minD)
    
    #Scaling F
    user_importance_rdd = user_importance_rdd.map(lambda x: (1, x))
    user_max_importance_rdd = user_max_importance_rdd.map(lambda x: (1 ,x[1])) #file maxF/max_minD chi co mot gia tri duy nhat

    temp1_rdd = user_importance_rdd.join(user_max_importance_rdd) #tuple (1, ((user, F), maxF))
    temp2_rdd = temp1_rdd.map(lambda x: x[1]) #((user, F), maxF)
    resultF_rdd = temp2_rdd.map(lambda x :(x[0][0], float(x[0][1])/float(x[1]))) #(user, scaling_F)

    result = resultF_rdd.toDF(["User", "scaling_F"])
    result = result.toPandas()

    spark.stop()
    return result
    

if __name__ ==  "__main__":
    spark = SparkSession.builder.appName('caculateScaling').getOrCreate()

    #Input: user \t F
    input_file_1 = "hdfs://localhost:9000/Clustering/Importance.csv"
    input_file_2 = "hdfs://localhost:9000/Clustering/MaxImportance.csv"
    output_file = "hdfs://localhost:9000/Clustering/NewImportance.csv"

    input_file_1d = "hdfs://localhost:9000/Clustering/Distance.csv"
    input_file_2d = "hdfs://localhost:9000/Clustering/MaxDistance.csv"
    output_file_d = "hdfs://localhost:9000/Clustering/NewDistance.csv"
    
    calculateScaling(spark, input_file_1, input_file_2, output_file)
    calculateScaling(spark, input_file_1d, input_file_2d, output_file_d)

    spark.stop()


