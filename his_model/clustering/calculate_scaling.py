from pyspark.sql import SparkSession
import os

def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)
def extract_line(line):
            fields = line.split('\t')
            user, importance = fields[0], fields[1]
            return (user, float(importance))

def calculateScaling (input_file, max_input_file, output_file):

    spark = SparkSession.builder.appName('caculateScaling').getOrCreate()
    
    # Đọc input và chuyển thành RDD
    lines_input_file_1= spark.sparkContext.textFile(input_file)
    user_importance_rdd = lines_input_file_1.map(extract_line) # RDD of tuples: (user,F or D)

    lines_input_file_2 = spark.sparkContext.textFile(max_input_file)
    user_max_importance_rdd = lines_input_file_2.map(extract_line) # RDD of tuples: (user,max_F or max_minD)

    # Scaling F
    user_importance_rdd = user_importance_rdd.map(lambda x: (1, x))
    user_max_importance_rdd = user_max_importance_rdd.map(lambda x: (1 ,x[1])) #file maxF/max_minD chi co mot gia tri duy nhat

    temp1_rdd = user_importance_rdd.join(user_max_importance_rdd) #tuple (1, ((user, F), maxF))
    temp2_rdd = temp1_rdd.map(lambda x: x[1]) #((user, F), maxF)
    resultF_rdd = temp2_rdd.map(lambda x :(x[0][0], x[0][1]/x[1])) #(user, scaling_F)

    # Ghi kết quả vào HDFS
    resultF_rdd.toDF(["User", "scaling_F"]).write.mode("overwrite").options(header='False', delimiter = '\t').csv(output_file)
    spark.stop()

