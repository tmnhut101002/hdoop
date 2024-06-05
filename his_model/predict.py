import mysql.connector
import json
import requests
import timeit

def get_unrated_products_and_predict(user_id, redis_host='localhost', redis_port=6379, mysql_config={}):
    # Connect to MySQL
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor(dictionary=True)

    # Get average ratings of all users
    query_avg_ratings = "SELECT user_id, avg_rating FROM AVGRating"
    cursor.execute(query_avg_ratings)
    avg_ratings = cursor.fetchall()
    avg_ratings_dict = {row['user_id']: row['avg_rating'] for row in avg_ratings}

    # Get training data
    query_training_data = "SELECT user_id, item_id, rating, timestamp FROM TrainingData"
    cursor.execute(query_training_data)
    training_data = cursor.fetchall()
    training_data_dict = {(row['user_id'], row['item_id']): row['rating'] for row in training_data}
    
    # Connect to Redis
    # r = redis.Redis(host=redis_host, port=redis_port, db=0)

    # # Get top 10 similar users from Redis
    # s = timeit.default_timer()
    # sim_key_pattern = f'ecommerce:sim:{user_id}:*'
    # similar_users = []
    # for key in r.scan_iter(sim_key_pattern):
    #     key_str = key.decode() if isinstance(key, bytes) else key
    #     user_v = key_str.split(':')[-1]
    #     sim_value = r.execute_command('JSON.GET', key_str)
    #     if sim_value:
    #         sim_data = json.loads(sim_value)
    #         sim = sim_data.get('mfps')
    #         if sim is not None:
    #             similar_users.append((int(user_v), sim))
    # t = timeit.default_timer()
    # print("Load sim REDIS", t - s)
    # similar_users = sorted(similar_users, key=lambda x: -float(x[1]))
    
    query_get_sim = f'''
        select user_v, sim
        from Sim
        where user_u = {user_id};
    '''
    cursor.execute(query_get_sim)
    similar_users = cursor.fetchall()
    similar_users_tuples = [(int(user['user_v']), user['sim']) for user in similar_users]
    similar_users_tuples = sorted(similar_users_tuples, key=lambda x: x[1], reverse=True)
    
    # Get products rated by similar users but not rated by the current user
    query_unrated_products = f"""
        SELECT DISTINCT p.id as product_id
        FROM Product p
        WHERE p.id NOT IN (
            SELECT productID
            FROM ProductReview
            WHERE customerID = {user_id});
    """
    cursor.execute(query_unrated_products)
    unrated_products = cursor.fetchall()

    cursor.close()
    cnx.close()
    
    

    def calculate_predict_rating(product_id):
        numerator = 0
        denominator = 0
        k = 0
        for user_v, sim in similar_users_tuples:
            if k == 50:
                break
            if (str(user_v), str(product_id)) in training_data_dict:
                rv_i = training_data_dict[(str(user_v), str(product_id))]
                mu_v = avg_ratings_dict.get(str(user_v), 0)
                numerator += float(sim) * (rv_i - mu_v)
                denominator += abs(float(sim))
                k+=1

        user_avg_rating = avg_ratings_dict.get(str(user_id), 0)
        predict_sim = user_avg_rating + numerator / denominator if denominator != 0 else user_avg_rating
        predict_sim = max(1, min(predict_sim, 5))
        return predict_sim

    for product in unrated_products:
        product_id = product['product_id']
        product['product_id'] = str(product_id)
        product['predict_rating'] = str(calculate_predict_rating(product_id))

        # Sort the list in descending order by 'predict_rating'
    sorted_result = sorted(unrated_products, key=lambda x: float(x['predict_rating']), reverse=True)
    print(sorted_result[:20])
    # Convert the sorted list to a JSON string
    json_result = json.dumps(sorted_result[:50], ensure_ascii=False, indent=4)
    res ={'customer_id': user_id, 'list': json_result}
    res_json = json.dumps(res, ensure_ascii=False, indent=4)
    return res_json

    
if __name__ == '__main__':

    mysql_config = {
        'user': 'root',
        'password': 'Password@123',
        'host': 'localhost',
        'database': 'ecommerce',
    }

    user_id = '2415'
    # start = timeit.default_timer()
    result = get_unrated_products_and_predict(user_id, mysql_config=mysql_config)
    # stop = timeit.default_timer()
    # print('time:', stop-start)
    a = {'data': result}
    res = requests.post('http://127.0.0.1:8080/api/recommend', json=a)
    print(result)