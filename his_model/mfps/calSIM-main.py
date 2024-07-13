from ratingCommodity import RatingCommodity
from ratingDetails import RatingDetails
from ratingUsefulness import RatingUsefulness
from ratingTime import RatingTime
from mfps import mfps, load_sim_to_mysql
import mysql.connector
import os
import datetime
if  __name__ == "__main__":
    # Kết nối database
    # host = 'mysql-ecommerce-nhut0789541410-f8ba.e.aivencloud.com'
    # port = '27163'
    # user = 'avnadmin'
    # password = 'AVNS_SQHY8Ivz7J5kp9ElUF2'
    
    host = 'localhost'
    port = '3306'
    user = 'root'
    password = '1234'
    
    # Kết nối Database
    db_config = {
        'user': user,
        'password': password,
        'host':  host,
        'port': port,
        'database': 'ecommerce',
        'raise_on_warnings': True
    }
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    
    input_directory = '/home/hdoop/his_model/mfps/temp_preSIM/'
    file_list = os.listdir(input_directory)
    ListLabel = [file.split('_')[1].split('.')[0] for file in file_list if file.startswith('input_')]

    # Xóa bảng độ tương đồng cũ
    query_truncate = 'Truncate Sim;'
    cursor.execute(query_truncate)
    cnx.commit()
    cursor.close()
    
    i = 0
    start = datetime.datetime.now()
    # Tính độ tương đồng cho từng cụm
    for label in ListLabel:
        print(f'\nLoop {i} - Calculate SIM for cluster {label}')
        INPUT = f'file://{input_directory}input_{label}.txt'
        AVG = f'file://{input_directory}avg_{label}.txt'
        
        print('Calculate RC...')
        rc_df = RatingCommodity(INPUT)
        
        print('Calculate RU...')
        ru_df = RatingUsefulness(INPUT)
        
        print('Calculate RD...')
        rd_df = RatingDetails(INPUT, AVG)
        
        print('Calculate RT...')
        rt_df = RatingTime(INPUT)
        
        print('Calculate SIM...')
        sim_df = mfps(rc_df,ru_df,rd_df,rt_df)
        
        print('Storage Similarity...')
        load_sim_to_mysql(sim_df, db_config)
        i+=1
        
    end = datetime.datetime.now()
    print('>>> Total time:', end-start)