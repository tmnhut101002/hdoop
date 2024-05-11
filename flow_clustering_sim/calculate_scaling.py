
from pyspark.sql import SparkSession
import os

def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)
def extract_line(line):
            fields = line.split('\t')
            user, importance = fields[0], fields[1]
            return (user, float(importance))

#Su dung cho ca F va D
def calculateScaling (spark0, input_file, max_input_file, output_file):

    spark = SparkSession.builder.appName('caculateScaling').getOrCreate()
    #Read input file and convert to RDD
    lines_input_file_1= spark.sparkContext.textFile(input_file)
    user_importance_rdd = lines_input_file_1.map(extract_line) # RDD of tuples: (user,F or D)

    lines_input_file_2 = spark.sparkContext.textFile(max_input_file)
    user_max_importance_rdd = lines_input_file_2.map(extract_line) # RDD of tuples: (user,max_F or max_minD)
    
    #Scaling F
    user_importance_rdd = user_importance_rdd.map(lambda x: (1, x))
    user_max_importance_rdd = user_max_importance_rdd.map(lambda x: (1 ,x[1])) #file maxF/max_minD chi co mot gia tri duy nhat

    temp1_rdd = user_importance_rdd.join(user_max_importance_rdd) #tuple (1, ((user, F), maxF))
    temp2_rdd = temp1_rdd.map(lambda x: x[1]) #((user, F), maxF)
    resultF_rdd = temp2_rdd.map(lambda x :(x[0][0], x[0][1]/x[1])) #(user, scaling_F)

    resultF_rdd.toDF(["User", "scaling_F"]).write.mode("overwrite").options(header='False', delimiter = '\t').csv(output_file)

    #Ghi vao output_file
    # with open(output_file,'w') as output_file:
    #     for user, new_F in resultF_rdd.collect():
    #            output_file.write(f'{user}\t{new_F}\n')
    # output_file.close()`
    spark.stop()
      

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

    # # Read importance.txt into a RDD of tuples
    # lines_input_file_1 = spark.sparkContext.textFile(input_file_1)
    # user_importance_rdd = lines_input_file_1.map(extract_line) # RDD of tuples: (user,F)

    # lines_input_file_1d = spark.sparkContext.textFile(input_file_1d)
    # user_distance_rdd = lines_input_file_1d.map(extract_line) # RDD of tuples: (user,D)

    # # Read max_distance.txt into a RDD of tuples
    # lines_input_file_2 = spark.sparkContext.textFile(input_file_2)
    # user_max_importance_rdd = lines_input_file_2.map(extract_line) # RDD of tuples: (user,max_F)

    # lines_input_file_2d = spark.sparkContext.textFile(input_file_2d)
    # user_max_distance_rdd= lines_input_file_2d.map(extract_line) # RDD of tuples: (user,max_minD)

    # #Scaling F
    # user_importance_rdd = user_importance_rdd.map(lambda x: (1, x))
    # user_max_importance_rdd = user_max_importance_rdd.map(lambda x: (1 ,x[1])) #file maxF chi co mot gia tri duy nhat

    # temp1_rdd = user_importance_rdd.join(user_max_importance_rdd) #tuple (1, ((user, F), maxF))
    # temp2_rdd = temp1_rdd.map(lambda x: x[1]) #((user, F), maxF)
    # resultF_rdd = temp2_rdd.map(lambda x :(x[0][0], x[0][1]/x[1])) #(user, scaling_F)

    # #Ghi vao output_file
    # with open(output_file,'w') as output_file:
    #     for user, new_F in resultF_rdd.collect():
    #            output_file.write(f'{user}\t{new_F}\n')
    # output_file.close()

    # #Scaling D
    # user_distance_rdd = user_distance_rdd.map(lambda x: (1, x))
    # user_max_distance_rdd = user_max_distance_rdd.map(lambda x: (1 ,x[1])) #file max_minD chi co mot gia tri duy nhat

    # temp3_rdd = user_distance_rdd.join(user_max_distance_rdd) #tuple (1, ((user, D), max_minD))
    # temp4_rdd = temp3_rdd.map(lambda x: x[1]) #((user, D), max_minD)
    # resultD_rdd = temp4_rdd.map(lambda x :(x[0][0], x[0][1]/x[1])) #(user, scaling_D)

    # #Ghi vao output_file
    # with open(output_file_d,'w') as output_file:
    #     for user, new_D in resultD_rdd.collect():
    #            output_file.write(f'{user}\t{new_D}\n')
    # output_file.close()
    # spark.stop()


