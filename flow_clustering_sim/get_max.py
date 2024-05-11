from pyspark.sql import SparkSession
import numpy as np

def get_max_map(row):
    user, value = row.strip().split('\t')
    return None, f'{user};{value}'

def get_max_reduce(_, values):
    values_list = [line.strip().split(';') for line in values]
    values_array = np.array(values_list)

    # Lấy chỉ số của giá trị lớn nhất trong cột thứ hai
    index = np.argmax(values_array[:, 1])

    # Lấy giá trị lớn nhất
    max_value = values_array[index]
    max_key, max_value = max_value

    return f'{max_key}', f'{max_value}'

def getMax(spark1, input_path, output_path):

    spark = SparkSession.builder.appName("GetMax").getOrCreate()
     # Đọc dữ liệu đầu vào thành RDD
    input_data = spark.sparkContext.textFile(input_path)
     # Sử dụng map để thực hiện chức năng của mapper
    mapped_data = input_data.map(get_max_map)
    # Sử dụng groupByKey va map để thực hiện chức năng của reducer
    grouped_data = mapped_data.groupByKey()
    result = grouped_data.map(lambda x: get_max_reduce(x[0], x[1]))
    # Ghi file
    output = result.toDF(["MaxKey", "MaxValue"])
    output.write.mode("overwrite").options(header='False', delimiter='\t').csv(output_path)

    # with open(output_path, 'w') as out:
    #     for max_key, max_value in output:
    #         out.write(f"{max_key}\t{max_value}")
    # out.close()
    spark.stop()
    

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GetMax").getOrCreate()

    input_path = "hdfs://localhost:9000/Clustering/Importance.csv"  # Tệp đầu vào
    output_path = "hdfs://localhost:9000/Clustering/MaxImportance.csv"  # Tệp đầu ra

    input_path_d = "hdfs://localhost:9000/Clustering/Distance.csv"  # Tệp đầu vào
    output_path_d = "hdfs://localhost:9000/Clustering/MaxDistance.csv"  # Tệp đầu ra

    getMax(spark,input_path,output_path)
    getMax(spark,input_path_d, output_path_d)

    spark.stop()
    # # Đọc dữ liệu đầu vào thành RDD
    # input_data = spark.sparkContext.textFile(input_path)
    # input_data_d = spark.sparkContext.textFile(input_path_d)

    # # Sử dụng map để thực hiện chức năng của mapper
    # mapped_data = input_data.map(get_max_map)
    # mapped_data_d = input_data_d.map(get_max_map)

    # # Sử dụng groupByKey để thực hiện chức năng của reducer
    # grouped_data = mapped_data.groupByKey()
    # grouped_data_d = mapped_data_d.groupByKey()

    # # Sử dụng map để thực hiện chức năng của reducer
    # result = grouped_data.map(lambda x: get_max_reduce(x[0], x[1]))
    # result_d = grouped_data_d.map(lambda x: get_max_reduce(x[0], x[1]))

    # # Hiển thị kết quả
    # output = result.collect()
    # with open(output_path, 'w') as out:
    #     for max_key, max_value in output:
    #         out.write(f"{max_key}\t{max_value}")
    # out.close()

    # output_d = result_d.collect()
    # with open(output_path_d, 'w') as out_d:
    #     for max_key, max_value in output_d:
    #         out_d.write(f"{max_key}\t{max_value}")
    # out_d.close()
    
