from pyspark.sql import SparkSession
import os

def sumFD(newF_df, newD_df):

    spark = SparkSession.builder.appName('caculateSumFD').getOrCreate()

    lines_input_file_F = spark.createDataFrame(newF_df)
    F_rdd =  lines_input_file_F.rdd

    lines_input_file_D = spark.createDataFrame(newD_df)
    D_rdd = lines_input_file_D.rdd

    F_join_D_rdd = F_rdd.join(D_rdd) #(user,(F,D))
    sum_FD = F_join_D_rdd.map(lambda x: (x[0],float(x[1][0])+float(x[1][1]))) #(user, sumFD)

    sum_FD = sum_FD.toDF(['user','sumFD'])
    sum_FD = sum_FD.toPandas()

    spark.stop()
    return sum_FD

if  __name__ == "__main__":
    spark = SparkSession.builder.appName('caculateSumFD').getOrCreate()

    input_file_F = "hdfs://localhost:9000/Clustering/NewImportance.csv"
    input_file_D = "hdfs://localhost:9000/Clustering/NewDistance.csv"
    output_file= "hdfs://localhost:9000/Clustering/SumFD.csv"

    sumFD(spark,input_file_F,input_file_D, output_file)

    spark.stop()