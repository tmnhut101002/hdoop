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

    spark.stop()

if __name__ ==  "__main__":
    spark = SparkSession.builder.appName('caculateScaling').getOrCreate()

    #Input: user \t F
    input_file_1 = "hdfs:///Clustering_mysql/Importance"
    input_file_2 = "hdfs:///Clustering_mysql/MaxImportance"
    output_file = "hdfs:///Clustering_mysql/NewImportance"

    input_file_1d = "hdfs:///Clustering_mysql/Distance"
    input_file_2d = "hdfs:///Clustering_mysql/MaxDistance"
    output_file_d = "hdfs:///Clustering_mysql/NewDistance"
    
    calculateScaling(spark, input_file_1, input_file_2, output_file)
    calculateScaling(spark, input_file_1d, input_file_2d, output_file_d)

    spark.stop()

