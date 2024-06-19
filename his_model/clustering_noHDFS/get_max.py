from pyspark.sql import SparkSession
import numpy as np

def get_max_map(col):
    return None, f'{col[0]};{col[1]}'

def get_max_reduce(_, values):
    values_list = [line.strip().split(';') for line in values]
    values_array = np.array(values_list)

    # Lấy chỉ số của giá trị lớn nhất trong cột thứ hai
    index = np.argmax(values_array[:, 1])

    # Lấy giá trị lớn nhất
    max_value = values_array[index]
    max_key, max_value = max_value

    return f'{max_key}', f'{max_value}'

def getMax(importance_df):

    spark = SparkSession.builder.appName("GetMax").getOrCreate()
    
    # Đọc dữ liệu đầu vào thành RDD
    input_data = spark.createDataFrame(importance_df).rdd

    # Sử dụng map để thực hiện chức năng của mapper
    mapped_data = input_data.map(get_max_map)
    
    # Sử dụng groupByKey va map để thực hiện chức năng của reducer
    grouped_data = mapped_data.groupByKey()
    result = grouped_data.map(lambda x: get_max_reduce(x[0], x[1]))
    
    result = result.toDF(["MaxKey", "MaxValue"])
    result = result.toPandas()

    spark.stop()
    return result
    

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GetMax").getOrCreate()

    input_path = "hdfs://localhost:9000/Clustering/Importance.csv"  # Tệp đầu vào
    output_path = "hdfs://localhost:9000/Clustering/MaxImportance.csv"  # Tệp đầu ra

    input_path_d = "hdfs://localhost:9000/Clustering/Distance.csv"  # Tệp đầu vào
    output_path_d = "hdfs://localhost:9000/Clustering/MaxDistance.csv"  # Tệp đầu ra

    getMax(spark,input_path,output_path)
    getMax(spark,input_path_d, output_path_d)

    spark.stop()
    
