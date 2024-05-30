from ratingCommodity import RatingCommodity
from ratingDetails import RatingDetails
from ratingUsefullness import RatingUsefullness
from ratingTime import RatingTime
from mfps import mfps, load_sim_to_redis, initialize_redis_index
import timeit
import mysql.connector
import redis

if  __name__ == "__main__":
    redis_client = redis.from_url('redis://localhost:6379/?decode_responses=True')
    MODEL_INDEX_NAME = "idx:sim"
    MODEL_KEY_BASE = "ecommerce:sim"
    try:
        redis_client.ft(MODEL_INDEX_NAME).info()
        print('Index already exists!')
    except:
        initialize_redis_index(redis_client, MODEL_INDEX_NAME, MODEL_KEY_BASE)
    
    db_config = {
        'user': 'root',
        'password': 'Password@123',
        'host': 'localhost',
        'database': 'ecommerce',
        'raise_on_warnings': True
    }
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    # Query to fetch distinct labels from the label column
    query = "SELECT DISTINCT label FROM TrainingData_Cluster"
    cursor.execute(query)
    ListLabel = [row[0] for row in cursor.fetchall()]
    time = 0
    for index in range(len(ListLabel)):
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

        sim_df = mfps(rc_df,ru_df,rd_df,rt_df)
        load_sim_to_redis(sim_df, redis_client, MODEL_KEY_BASE)
        
        stop = timeit.default_timer()
        with  open('./output/time.txt','a') as f:
            f.write(f'Start: {start} ... End: {stop}\n')
            f.write(f'mfps: {stop-start}s \n')
            f.write(f'Rating Commodities: {rcTime-start}s\n')
            f.write(f'Rating Usefulness: {ruTime-rcTime}s\n')
            f.write(f'Rating Details: {rdTime-ruTime}s\n')
            f.write(f'Rating Time: {rtTime-rdTime}s\n')
            time += (stop-start)
            f.write(f'Total: {time}')