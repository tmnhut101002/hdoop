from pyspark.sql import SparkSession
import numpy as np

def get_max_map(row):
    user, value = row.strip().split('\t')
    return None, f'{user};{value}'

def get_max_reduce(_, values):
    values_list = [line.strip().split(';') for line in values]
    values_array = np.array(values_list)

    index = np.argmax(values_array[:, 1])
    max_value = values_array[index]
    max_key, max_value = max_value

    return f'{max_key}', f'{max_value}'

def getMax(input_path, output_path):
    spark = SparkSession.builder.appName("GetMax").getOrCreate()
    input_data = spark.sparkContext.textFile(input_path)
    mapped_data = input_data.map(get_max_map)
    grouped_data = mapped_data.groupByKey()
    result = grouped_data.map(lambda x: get_max_reduce(x[0], x[1]))
    output = result.toDF(["MaxKey", "MaxValue"])
    output.write.mode("overwrite").options(header='False', delimiter='\t').csv(output_path)
    spark.stop()

if __name__ == "__main__":
    
    input_path = "hdfs://localhost:9000/HM_clustering/Importance"
    output_path = "hdfs://localhost:9000/HM_clustering/MaxImportance"
    getMax(input_path,output_path)
