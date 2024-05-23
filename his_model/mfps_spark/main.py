from ratingCommodity import RatingCommodity
from ratingDetails import RatingDetails
from ratingUsefullness import RatingUsefullness
from ratingTime import RatingTime
from mfps import mfps
import mysql.connector
from mysql.connector import errorcode

if  __name__ == "__main__":
    db_config = {
        'user': 'root',
        'password': 'Password@123',
        'host': 'localhost',
        'database': 'ecommerce',
        'raise_on_warnings': True
    }
    NUMBER_OF_CLUSTERS = 4
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    # Query to fetch distinct labels from the label column
    query = "SELECT DISTINCT label FROM TrainingData_1"
    cursor.execute(query)

    # Fetching the results
    # ListLabel = [row[0] for row in cursor.fetchall()]
    # for index in range(len(ListLabel)):
    #     INPUT = f'file:///home/hdoop/his_model/mfps_spark/temp_preSIM/input_{ListLabel[index]}.txt'
    #     AVG = f'file:///home/hdoop/his_model/mfps_spark/temp_preSIM/avg_{ListLabel[index]}.txt'

    #     RatingCommodity(INPUT, db_config)
    #     RatingUsefullness(INPUT, db_config)
    #     RatingDetails(INPUT, db_config, AVG)
    #     RatingTime(INPUT, db_config)
    #     mfps(db_config, ListLabel[index])
    
    INPUT = f'file:///home/hdoop/his_model/mfps_spark/temp_preSIM/input_21558077.txt'
    AVG = f'file:///home/hdoop/his_model/mfps_spark/temp_preSIM/avg_21558077.txt'

    # RatingCommodity(INPUT, db_config)
    # RatingUsefullness(INPUT, db_config)
    RatingDetails(INPUT, db_config, AVG)
    RatingTime(INPUT, db_config)
    # mfps(db_config, '21558077')
