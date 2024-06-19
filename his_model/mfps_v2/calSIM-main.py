from ratingCommodity import RatingCommodity
from ratingDetails import RatingDetails
from ratingUsefullness import RatingUsefullness
from ratingTime import RatingTime
from mfps import mfps, load_sim_to_mysql
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
    query = "SELECT DISTINCT label FROM TrainingData_Cluster"
    cursor.execute(query)
    ListLabel = [row[0] for row in cursor.fetchall()]
    
    print("Truncate Table Sim...")
    query_truncate = 'Truncate Sim;'
    cursor.execute(query_truncate)
    cnx.commit()
    cursor.close()
    
    for index in range(len(ListLabel)):
        INPUT = f'file:///home/hdoop/his_model/mfps_v2/temp_preSIM/input_{ListLabel[index]}.txt'
        AVG = f'file:///home/hdoop/his_model/mfps_v2/temp_preSIM/avg_{ListLabel[index]}.txt'
        
        rc_df = RatingCommodity(INPUT)
        ru_df = RatingUsefullness(INPUT)
        rd_df = RatingDetails(INPUT, AVG)
        rt_df = RatingTime(INPUT)
        sim_df = mfps(rc_df,ru_df,rd_df,rt_df)

        load_sim_to_mysql(sim_df)