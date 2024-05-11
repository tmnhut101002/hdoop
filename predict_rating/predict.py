from pyspark.sql import SparkSession
from pyspark import SparkConf
import numpy as np
import pandas as pd
from pyspark.sql.functions import col

def extractSimFile(line):
    user, sim = line.strip().split('\t')
    user_u, user_v = user.strip().split(';')
    return (user_v, float(sim))

def extractUserAvgFile(line):
    user, avg = line.strip().split('\t')
    return (user, avg)

def extractInputFileWithRating(line):
    userItem,  ratingTime = line.strip().split('\t')
    user, item = userItem.strip().split(';')
    rate, time = ratingTime.strip().split(';')
    return ((user, item), float(rate))

def recommend_list(user, N_item, training_df, testing_df, user_avg_df, sim_path, real_pre_df):
    user_col = []
    user_col_rmse=[] 
    item_col=[]
    rating_real_col=[]
    rating_predict_col=[]
    
    # Sim User
    user_sim_rdd = spark.sparkContext.textFile(f'{sim_path}/{user}.csv').map(extractSimFile)
    user_sim_df = user_sim_rdd.toDF(['user_v', 'sim'])
    predict_list = testing_df.filter(testing_df['user_id'] == str(user)).select('item_id').rdd.flatMap(lambda x: x).collect()

    
    # R_u_tb = user_avg_df.avg_rating[user_avg_df.user == user].values[0]
    R_u_tb = user_avg_df.filter(user_avg_df['user'] == str(user)).select('avg_rating').collect()

    for j in predict_list:
        user_j = training_df.filter(training_df['item_id'] == j).select('user_id').rdd.flatMap(lambda x: x).collect()
        # user_j = training_df.user_id[training_df.item_id == j].to_list()
        
        # user_j_top_k = user_sim_df.user_v[user_sim_df.user_v.isin(user_j) == True][:50].to_list()
        filtered_df = user_sim_df.filter(col("user_v").isin(user_j))
        user_j_top_k_df = filtered_df.limit(50)
        user_j_top_k = user_j_top_k_df.select("user_v").rdd.flatMap(lambda x: x).collect()

        if (len(user_j_top_k)==0): #không có láng giềng => sum_sim = -1
            predict_sim = -1
        else:
            numerator = (
                user_sim_df
                .join(training_df, user_sim_df.user_v == training_df.user_id, "inner")
                .join(user_avg_df, user_sim_df.user_v == user_avg_df.user, "inner")
                .filter(training_df.item_id == j)
                .filter(col("user_id").isin(user_j_top_k))
                .select(
                    (user_sim_df.sim * (training_df.rating - user_avg_df.avg_rating)).alias("numerator")
                )
                .rdd.flatMap(lambda x: x)
                .sum()
            )

            denominator = (
                user_sim_df
                .filter(col("user_v").isin(user_j_top_k))
                .select(user_sim_df.sim)
                .rdd.flatMap(lambda x: x)
                .map(lambda x: abs(x))
                .sum()
            )
            
            predict_sim = R_u_tb[0]['avg_rating'] + numerator / denominator
            '''
            predict_sim = R_u_tb + sum(
                np.array([(user_sim_df.sim[user_sim_df.user_v == i].values[0])*(training_df.rating[training_df.item_id == j][training_df.user_id == i].values[0] - user_avg_df.rating[user_avg_df.user == i].values[0])
                for i in user_j_top_k])) / sum(np.array([abs(user_sim_df.sim[user_sim_df.user_v == i].values[0]) for i in user_j_top_k]))
            '''
            if predict_sim > 5:
              predict_sim = 5

        #Tìm rating trong test
        # rate_real =  testing_df.rating[testing_df.user_id == user][testing_df.item_id == j].values[0]
        rate_real = (
            testing_df
            .filter((col("user_id") == user) & (col("item_id") == j))
            .select("rating")
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        #thêm id item j vào cột item
        user_col.append(user)
        item_col.append(j)

        #Thêm sim vào cột đánh giá
        rating_real_col.append(rate_real[0])
        rating_predict_col.append(predict_sim)
        user_col_rmse.append(pow(predict_sim - rate_real[0],2))

    df = pd.DataFrame(data={'User':user_col,'Item':item_col,'Rating_real':rating_real_col,'Rating_predict':rating_predict_col, 'RMSE': user_col_rmse})
    df = df.sort_values(by=['Rating_predict'], axis=0, ascending=False, ignore_index = True)
    
    real_pre_df = pd.concat([real_pre_df, df])

    #Lấy danh sách item từ 1 tới N
    df =  df.iloc[:N_item]

    #Trả về danh sách N item
    return df.Item.to_list(), real_pre_df

def revaluation(N, sim_path, testing_df, training_df, user_avg_df, first, last):
    #Khai bao trước những thước đo đánh giá thành các cột
    real_pre_df = pd.DataFrame()
    User_col = []
    # danh sách item được đề xuất cho người dùng
    I_N_col = []
    # Danh sách item trong tập testing mà người dùng thật sự đánh giá cao
    I_te_col = []
    intersection_col = []
    I_N_len_col = []
    I_te_len_col = []
    intersection_len_col = []
    # danh sách lưu trữ giá trị Precision (độ chính xác) cho mỗi người dùng, giao giữa I_N_col và I_te_col.
    Prec_u_N = []
    # danh sách lưu trữ giá trị Recall (độ phủ) cho mỗi người dùng. Recall được tính dựa trên số lượng mục trong phần giao của I_N_col và I_te_col
    Rec_u_N = []

    F1_u_N = []

    testing_user = testing_df.select('user_id').rdd.flatMap(lambda x: x).collect()
    for i in range(first, last + 1):
        if str(i) in testing_user:
            #Danh sách item system recommend cho người dùng
            I_N, real_pre_df = recommend_list(i, N, training_df, testing_df, user_avg_df, sim_path, real_pre_df)
            
            sparkDF_1 = spark.createDataFrame(real_pre_df)
            sparkDF_1.write.mode("append") \
                .format("jdbc") \
                .option("driver","com.mysql.cj.jdbc.Driver") \
                .option("url", "jdbc:mysql://localhost:3306/Result_rating?useSSL=false") \
                .option("dbtable", "Result_real_vs_predict_rating") \
                .option("user", "root") \
                .option("password", "Password@123") \
                .save()
            #Danh sách item trong file test của người dùng
            I_te = testing_df.filter((col("user_id") == i) & (col("rating") >= user_avg_df.filter(col("user") == i).select("avg_rating").collect()[0][0])).select("item_id").rdd.flatMap(lambda x: x).collect()
            # I_te = testing_df.Item[testing_df.User == i][testing_df.Rating >= user_avg_df[user_avg_df['user'] == i]['avg_rating'].values[0]].to_list()
            if (len(I_te)) == 0:
                continue
            #Phần giao giữa danh sách item
            intersection = [value for value in I_N if value in I_te]
            print (I_N)
            print(I_te)
            print(intersection)
            try:
                Prec =  abs(len(intersection)) / N
                Rec = abs(len(intersection)) / abs(len(I_te))
                F1 = 2 * (Prec * Rec) / (Prec + Rec)
            except:
                if intersection == [] or I_te == [] or I_N == []:
                    Prec = 0
                    Rec = 0
                    F1 = 0
        else:
            continue
        #Thêm id người dùng, các độ đo vào các cột
        User_col.append(i)
        I_N_col.append(' '.join(str(e)for e in I_N))
        I_te_col.append(' '.join(str(e)for e in I_te))
        intersection_col.append(' '.join(str(e)for e in intersection))
        I_N_len_col.append(len(I_N))
        I_te_len_col.append(len(I_te))
        intersection_len_col.append(len(intersection))
        Prec_u_N.append(Prec)
        Rec_u_N.append(Rec)
        F1_u_N.append(F1)

        print("User:", i," ", "Prec:", Prec," ", "Rec:", Rec, " ", "F1:", F1)

    #Tạo dataframe
    df = pd.DataFrame(data={'User':User_col, 'I_N':I_N_col, 'I_te':I_te_col, 'intersection':intersection_col, 'I_N_len':I_N_len_col, 'I_te_len':I_te_len_col, 'intersection_len':intersection_len_col, 'Precision':Prec_u_N, 'Recursion': Rec_u_N, 'F1': F1_u_N})
    '''
    sparkDF = spark.createDataFrame(df)

    sparkDF.write.mode("append") \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/Result_rating?useSSL=false") \
        .option("dbtable", "Result_detail") \
        .option("user", "root") \
        .option("password", "Password@123") \
        .save()
    '''
    
    

if  __name__ == "__main__":
    spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "mysql-connector-java-8.0.13.jar")\
           .getOrCreate()

    user_avg_path = 'hdfs:///new_data/clustering/avg_ratings_full.txt'
    user_avg_rdd = spark.sparkContext.textFile(f'{user_avg_path}').map(lambda x: x.strip().split('\t')).map(lambda x: (str(x[0]),x[1]))
    user_avg_df = user_avg_rdd.toDF(['user', 'avg_rating'])
    user_avg_df = user_avg_df.withColumn("avg_rating", col("avg_rating").cast("double"))

    real_pre_df = pd.DataFrame()

    mysql_url = "jdbc:mysql://localhost:3306/ML100?useSSL=false"
    mysql_properties = {
        "user": "root",
        "password": "Password@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    table_name_training = 'MovieLen100k_training'
    training_df = spark.read.jdbc(mysql_url, table_name_training, properties=mysql_properties)

    table_name_testing = 'MovieLen100k_testing'
    testing_df = spark.read.jdbc(mysql_url, table_name_testing, properties=mysql_properties)


    sim_path = 'hdfs:///MFPS/full_sim'
    
    N = 5
    first = 2
    last = 2

    revaluation(N, sim_path, testing_df, training_df, user_avg_df, first, last)

    spark.stop()
