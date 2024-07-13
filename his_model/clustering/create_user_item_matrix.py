from pyspark.sql import SparkSession
import datetime

def extract_user_item_rating(df):
    user = df[0]
    item = df[1]
    rating = df[2]
    return (user, item, float(rating))

def create_item_list(filename):
    spark = SparkSession.builder.appName("ItemListCreator").getOrCreate()
    data = spark.sparkContext.textFile(filename).map(lambda x : x.strip()).collect()
    item = data
    spark.stop()
    return item
    
def extract_avg_rating(line):
    fields = line.split('\t')
    user, avg_rating = fields[0], fields[1]
    return (user, float(avg_rating))

def mapValueResult(x):
    user_avg = x[0]
    item_rating = x[1]
    result_list, real_rating = item_rating[0], item_rating[1]

    for i in range(len(real_rating)):
        for j in range(len(result_list)):
            if str(result_list[j]) == str(real_rating[i]):
                result_list[j] = str(result_list[j])  + ";" + str(real_rating[i+1])

    for i in range(len(result_list)):
        a = str(result_list[i]).split(';')
        if len(a) == 1:
            result_list[i]= str(result_list[i]) +';'+ str(user_avg[1])

    return  ((user_avg[0]), '|'.join(result_list))

def createUserItemMatrix(mysql_url, mysql_properties, items_file, avg_file, output_file, table_name, output_file_cluster):
    # Lấy list item
    items_path = items_file
    items = create_item_list(items_path)

    spark = SparkSession.builder.appName('UserItemMatrix').getOrCreate()
    
    # Lấy dữ liệu rating
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    lines_input_file = df.rdd.map(lambda row: (row.user_id, row.item_id, row.rating))
    user_item_rating_rdd = lines_input_file.map(extract_user_item_rating)
    
    # rating trung bình
    avg_ratings = spark.sparkContext.textFile(avg_file)
    user_avg_rating_rdd = avg_ratings.map(extract_avg_rating)

    # Tạo user_rdd
    user_rdd = user_avg_rating_rdd.keys()
    
    oneuser_moreitem_rdd = user_avg_rating_rdd. join(user_rdd.map(lambda x: (x,list(items)))).map(lambda x: ((x[0],x[1][0]),x[1][1])) # [0] (user,avg) [(item,rate(-1)),...]
    
    user_avg_item_rdd_0 = user_avg_rating_rdd.join(user_item_rating_rdd)#user (avg, item) -- user trung
    user_avg_item_rdd_0 = user_avg_item_rdd_0.map(lambda x: ((x[0],x[1][1]),x[1][0])) #(user,item),(avg)
    user_avg_item_rdd_0 = user_avg_item_rdd_0.join(user_item_rating_rdd.map(lambda x: ((x[0],x[1]),x[2])))# (u,i),(a,r)
    user_avg_item_rdd_0 = user_avg_item_rdd_0.map (lambda x : ((x[0][0],x[1][0]),(x[0][1],x[1][1])))#[0](u,a),(i,r); [,](u,a),(i,r);...
    user_avg_item_rdd_0 = user_avg_item_rdd_0.sortByKey().reduceByKey(lambda x,y : x + y).map(lambda x: list(x))

    # Ghi kết quả vào HDFS
    result_rdd = oneuser_moreitem_rdd.join(user_avg_item_rdd_0)
    result = result_rdd.map(mapValueResult).toDF(["user","ItemRating"])
    # result = result.coalesce(1)
    result.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file)
    result.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file_cluster)
    # result = result.coalesce(1)
    # result.write.mode('overwrite').parquet(output_file)
    # result.write.mode('overwrite').parquet(output_file_cluster)
    spark.stop()

if __name__ == "__main__":
    s = datetime.datetime.now()
    host = 'localhost'
    port = '3306'
    user = 'root'
    password = '1234'
    
    mysql_url = f"jdbc:mysql://{host}:{port}/ecommerce?useSSL=false"
    mysql_properties = {
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    table_name = 'TrainingData'
    
    avg_file = "hdfs://localhost:9000/HM_clustering/AverageRating"
    items_file = "hdfs://localhost:9000/HM_clustering/Item"
    output_file = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
    output_file_cluster = "hdfs://localhost:9000/HM_clustering/UserItemMatrixCluster"
    createUserItemMatrix(mysql_url, mysql_properties, items_file, avg_file, output_file, table_name, output_file_cluster)
    e = datetime.datetime.now()
    print('>>>', (e - s))

