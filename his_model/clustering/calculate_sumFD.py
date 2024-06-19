from pyspark.sql import SparkSession
import os

def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)

def extract(line):
            fields = line.split('\t')
            user, info = fields[0], fields[1]
            return (user, float(info))

def sumFD(input_file_F,input_file_D, output_file):
    spark = SparkSession.builder.appName('caculateSumFD').getOrCreate()

    lines_input_file_F = spark.sparkContext.textFile(input_file_F)
    F_rdd =  lines_input_file_F.map(extract)

    lines_input_file_D = spark.sparkContext.textFile(input_file_D)
    D_rdd = lines_input_file_D.map(extract)

    F_join_D_rdd = F_rdd.join(D_rdd) #(user,(F,D))
    sum_FD_rdd = F_join_D_rdd.map(lambda x: (x[0],x[1][0]+x[1][1])) #(user, sumFD)

    sum_FD_rdd.toDF(['user','sumFD']).write.mode("overwrite").options(header='False', delimiter = '\t').csv(output_file)

    spark.stop()

if  __name__ == "__main__":
    input_file_F = "hdfs://localhost:9000/HM_clustering/NewImportance"
    input_file_D = "hdfs://localhost:9000/HM_clustering/NewDistance"
    output_file= "hdfs://localhost:9000/HM_clustering/SumFD"
    sumFD(input_file_F, input_file_D, output_file)
