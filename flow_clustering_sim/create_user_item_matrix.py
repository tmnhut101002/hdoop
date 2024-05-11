import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import numpy as np
import os
import pyspark.sql.functions as f

def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)


    
def extract_user_item_rating(line):
            fields = line.split('\t')
            user, item = fields[0].split(';')
            rating, timestamp = fields[1].split(';')
            return (user, item, float(rating))

def create_item_list(filename):
        spark = SparkSession.builder.appName("ItemListCreator").getOrCreate()
        data = spark.sparkContext.textFile(filename).map(lambda x : x.strip()).collect()
        item = data
        spark.stop()
        return item
        

        # items = []
        # with open(filename, 'r') as file:
        #     for line in file:
        #         # item = line.strip(' \t\n')
        #         item = line.strip()  # Remove leading/trailing whitespaces and newlines
        #         items.append([item, -1.0])
        # return items
    
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
    
def createUserItemMatrix(spark1, items_file, input_file, avg_file, output_file):
    
        
    #item_list
    items_path = items_file
    items = create_item_list(items_path)
    #items_rdd = spark.sparkContext.parallelize(items,2)

    spark = SparkSession.builder.appName('UserItemMatrix').getOrCreate()
    #map lines user-item-rating
    lines_input_file = spark.sparkContext.textFile(input_file)
    user_item_rating_rdd = lines_input_file.map(extract_user_item_rating)
   

    #avergrate user rating
    avg_ratings = spark.sparkContext.textFile(avg_file)
    user_avg_rating_rdd = avg_ratings.map(extract_avg_rating)

    #user_rdd
    user_rdd = user_avg_rating_rdd.keys()
    #user /t item1; item2;...
    oneuser_moreitem_rdd = user_avg_rating_rdd. join(user_rdd.map(lambda x: (x,list(items)))).map(lambda x: ((x[0],x[1][0]),x[1][1])) # [0] (user,avg) [(item,rate(-1)),...]
    #oneuser_moreitem_rating_list = oneuser_moreitem_rdd.sortByKey().collect() #Ma tran ket qua du kien

    # user (avg(item,rating))
    user_avg_item_rdd_0 = user_avg_rating_rdd.join(user_item_rating_rdd)#user (avg, item) -- user trung
    
    
    user_avg_item_rdd_2 = user_avg_item_rdd_0.map(lambda x: ((x[0],x[1][1]),x[1][0])) #(user,item),(avg)

    # user_avg_item_rdd_1= user_avg_item_rdd_0.map(lambda x: ((x[0],x[1][0]),x[1][1])).mapValues(lambda x : tuple((x,-1))).reduceByKey(lambda x,y: x+y)
    # user_avg_item_rdd_1 = user_avg_item_rdd_1.mapValues(lambda x : remove_value_from_tuple(x, -1))

    user_avg_item_rdd_3 = user_avg_item_rdd_2.join(user_item_rating_rdd.map(lambda x: ((x[0],x[1]),x[2])))# (u,i),(a,r)
    user_avg_item_rdd_4 = user_avg_item_rdd_3.map (lambda x : ((x[0][0],x[1][0]),(x[0][1],x[1][1])))#[0](u,a),(i,r); [,](u,a),(i,r);...
    user_avg_item_rdd_5 = user_avg_item_rdd_4.sortByKey().reduceByKey(lambda x,y : x + y).map(lambda x: list(x))

    result_rdd = oneuser_moreitem_rdd.join(user_avg_item_rdd_5)
    result = result_rdd.map(mapValueResult).toDF(["user","ItemRating"])

    result.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file)

    #print(user_avg_item_rating_list)
    #user+item is key
    #user_avg_item_rating_rdd_change_key = user_avg_item_rating_rdd.map(lambda x :((x[0],x[1][1][0]),x[1])).sortByKey()
    
    # rating cua user tren mot san pham da duoc tinh trung binh => user,item, rating la duy nhat cho moi dong, rdd = ((user,item),rating) (su dung rdd.map(lambda x: (x[0], (x[1],1))).reduceByKey(lambda x,y : ((x[0]+y[0]),(x[1]+y[1]))).map(lambda x : (x[0], x[1][0]/x[1][1])))
    # final_result = []
    # with open(output_file, "w") as output_file:
    #       result = []
    #       for u,i in oneuser_moreitem_rating_list:
    #             result = []
    #             output_file.write(f"{u[0]}\t")
    #             item_inf = list(i)
    #             for U,I in user_avg_item_rdd_4_list:
    #                   if (u == U):
    #                         for item in item_inf:
    #                             if(item[0] == I[0]):
    #                                 item[1] = I[1]
    #                         for item in item_inf:
    #                             if(item[0] != I[0] and item[1] == -1):
    #                                 item[1] = u[1]
                        
    #             for re in item_inf:
    #                   result.append(f"{re[0]};{re[1]}")
    #             result = '|'.join(result)
    #             final_result = final_result.append([u[0], result ]) 
    #             output_file.write(f"{result}\n")
                            
    spark.stop()





if __name__ == '__main__':
    spark = SparkSession.builder.appName('UserItemMatrix').getOrCreate()
    input_file = '../input_file.txt'
    avg_file = "hdfs://localhost:9000/Clustering/AverageRating.csv"
    items_file = "hdfs://localhost:9000/Clustering/Item.csv"
    output_file = "hdfs://localhost:9000/Clustering/UserItemMatrix.csv"
    createUserItemMatrix(spark, items_file, input_file, avg_file, output_file)
    spark.stop()
    # #item_list
    # items_path = items_file
    # items = create_item_list(items_path)
    # items_rdd = spark.sparkContext.parallelize(items,2)
        
    # #map lines user-item-rating
    # lines_input_file = spark.sparkContext.textFile(input_file)
    # user_item_rating_rdd = lines_input_file.map(extract_user_item_rating)
   

    # #avergrate user rating
    # avg_ratings = spark.sparkContext.textFile(avg_file)
    # user_avg_rating_rdd = avg_ratings.map(extract_avg_rating)
    # user_avg_list = user_avg_rating_rdd.collect() #[(user, avg),...]

    # #user_rdd
    # user_rdd = user_avg_rating_rdd.keys()
    # #user /t item1; item2;...
    # oneuser_moreitem_rdd = user_avg_rating_rdd. join(user_rdd.map(lambda x: (x,list(items)))).map(lambda x: ((x[0],x[1][0]),x[1][1])) # [0] (user,avg) [(item,rate(-1)),...]
    # oneuser_moreitem_rating_list = oneuser_moreitem_rdd.sortByKey().collect() #Ma tran ket qua du kien

    # # user (avg(item,rating))
    # user_avg_item_rdd_0 = user_avg_rating_rdd.join(user_item_rating_rdd)#user (avg, item) -- user trung
    
    
    # user_avg_item_rdd_2 = user_avg_item_rdd_0.map(lambda x: ((x[0],x[1][1]),x[1][0])) #(user,item),(avg)

    # # user_avg_item_rdd_1= user_avg_item_rdd_0.map(lambda x: ((x[0],x[1][0]),x[1][1])).mapValues(lambda x : tuple((x,-1))).reduceByKey(lambda x,y: x+y)
    # # user_avg_item_rdd_1 = user_avg_item_rdd_1.mapValues(lambda x : remove_value_from_tuple(x, -1))

    # user_avg_item_rdd_3 = user_avg_item_rdd_2.join(user_item_rating_rdd.map(lambda x: ((x[0],x[1]),x[2])))# (u,i),(a,r)
    # user_avg_item_rdd_4 = user_avg_item_rdd_3.map (lambda x : ((x[0][0],x[1][0]),(x[0][1],x[1][1])))#[0](u,a),(i,r); [,](u,a),(i,r);...
    # user_avg_item_rdd_4_list = user_avg_item_rdd_4.sortByKey().collect()

    # #print(user_avg_item_rating_list)
    # #user+item is key
    # #user_avg_item_rating_rdd_change_key = user_avg_item_rating_rdd.map(lambda x :((x[0],x[1][1][0]),x[1])).sortByKey()
    
    # # rating cua user tren mot san pham da duoc tinh trung binh => user,item, rating la duy nhat cho moi dong, rdd = ((user,item),rating) (su dung rdd.map(lambda x: (x[0], (x[1],1))).reduceByKey(lambda x,y : ((x[0]+y[0]),(x[1]+y[1]))).map(lambda x : (x[0], x[1][0]/x[1][1])))
    # with open(output_file, "w") as output_file:
    #       result = []
    #       for u,i in oneuser_moreitem_rating_list:
    #             result = []
    #             output_file.write(f"{u[0]}\t")
    #             rate_inf = []
    #             item_inf = list(i)
    #             for U,I in user_avg_item_rdd_4_list:
    #                   if (u == U):
    #                         for item in item_inf:
    #                             if(item[0] == I[0]):
    #                                 item[1] = I[1]
    #                         for item in item_inf:
    #                             if(item[0] != I[0] and item[1] == -1):
    #                                 item[1] = u[1]
                     
                      
    #             for re in item_inf:
    #                   result.append(f"{re[0]};{re[1]}")
    #             result = '|'.join(result)
    #             output_file.write(f"{result}\n")
                            
              
    # # result_data = oneuser_moreitem_rating_list
    # # with open(output_file, "w") as output_file:
    # #         for user, rating in result_data:
    # #             output_file.write(f"{user}\t{rating}\n")

    # # user_item_matrix = UserItemMatrix(input_file, items_file, output_file, avg_file,spark)
    # # user_item_matrix.run_spark_job()
    #spark.stop()


