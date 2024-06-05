from ratingCommodity import RatingCommodity
from ratingDetails import RatingDetails
from ratingUsefullness import RatingUsefullness
from ratingTime import RatingTime
from mfps1h import mfps1h
from mfps import load_sim_to_redis
import timeit
import redis
import mysql.connector


if  __name__ == "__main__":
    redis_client = redis.from_url('redis://localhost:6379/?decode_responses=True')
    MODEL_INDEX_NAME = "idx:sim"
    MODEL_KEY_BASE = "ecommerce:sim"
    
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
        WHERE unix_timestamp(createdAt) >= (SELECT max(timestamp) FROM TrainingData)
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
    # time = 0
    for index in range(len(ListLabel)):
        user_list = []
        INPUT = f'file:///home/hdoop/his_model/mfps_v2/temp_preSIM/input_{ListLabel[index]}.txt'
        AVG = f'file:///home/hdoop/his_model/mfps_v2/temp_preSIM/avg_{ListLabel[index]}.txt'
        start =  timeit.default_timer()
        rc_df = RatingCommodity(INPUT)
        rcTime = timeit.default_timer()
        ru_df = RatingUsefullness(INPUT)
        ruTime = timeit.default_timer()
        rd_df = RatingDetails(INPUT, AVG)
        rdTime = timeit.default_timer()
        rt_df = RatingTime(INPUT)
        rtTime = timeit.default_timer()

        sim_df = mfps1h(rc_df,ru_df,rd_df,rt_df, ListUserNewRate)
        load_sim_to_redis(sim_df, redis_client, MODEL_KEY_BASE)
        
        stop = timeit.default_timer()

        # stop = timeit.default_timer()
        # with  open('./output/time.txt','a') as f:
        #     f.write(f'Start: {start} ... End: {stop}\n')
        #     f.write(f'mfps: {stop-start}s \n')
        #     f.write(f'Rating Commodities: {rcTime-start}s\n')
        #     f.write(f'Rating Usefulness: {ruTime-rcTime}s\n')
        #     f.write(f'Rating Details: {rdTime-ruTime}s\n')
        #     f.write(f'Rating Time: {rtTime-rdTime}s\n')
