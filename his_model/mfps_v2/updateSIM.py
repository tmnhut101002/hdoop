from ratingCommodity import RatingCommodity
from ratingDetails import RatingDetails
from ratingUsefullness import RatingUsefullness
from ratingTime import RatingTime
from mfps1h import mfps1h
from mfps import load_sim_to_mysql
import mysql.connector
if  __name__ == "__main__":
    
    db_config = {
        'user': 'root',
        'password': 'Password@123',
        'host': 'localhost',
        'database': 'ecommerce',
        'raise_on_warnings': True
    }
    
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    
    query_user_rate_1h = '''
        SELECT DISTINCT customerID
        FROM ProductReview
        WHERE unix_timestamp(createdAt) > (SELECT max(timestamp) FROM TrainingData)
    '''
    cursor.execute(query_user_rate_1h)
    ListUserNewRate = [str(row[0]) for row in cursor.fetchall()]
    
    query_label = f'''
        SELECT DISTINCT label
        FROM TrainingData_Cluster
        WHERE user_id IN ({query_user_rate_1h});
    '''
    cursor.execute(query_label)
    ListLabel = [row[0] for row in cursor.fetchall()]
    
    query_del_sim = f'''
        delete from Sim 
        where user_v in ({query_user_rate_1h}) or
            user_u in ({query_user_rate_1h})
    '''
    cursor.execute(query_del_sim)
    cnx.commit()
    time = 0
    for index in range(len(ListLabel)):
        user_list = []
        INPUT = f'file:///home/hdoop/his_model/mfps_v2/temp_preSIM/input_{ListLabel[index]}.txt'
        AVG = f'file:///home/hdoop/his_model/mfps_v2/temp_preSIM/avg_{ListLabel[index]}.txt'
        rc_df = RatingCommodity(INPUT)
        ru_df = RatingUsefullness(INPUT)
        rd_df = RatingDetails(INPUT, AVG)
        rt_df = RatingTime(INPUT)

        sim_df = mfps1h(rc_df,ru_df,rd_df,rt_df, ListUserNewRate)
        load_sim_to_mysql(sim_df)
