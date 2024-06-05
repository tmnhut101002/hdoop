from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import scipy.stats
import redis
from redis.commands.search.query import Query
from sentence_transformers import SentenceTransformer
import mysql.connector
import json
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.field import TagField, TextField, NumericField, VectorField
from redis.commands.search.query import Query
import timeit
import requests

INDEX_NAME = 'idx:product-name1'
DOC_PREFIX = 'ecommerce:product:'
    

def create_query_table(query, queries, encoded_queries, extra_params = {}):
    results_list = []
    itemRealSearch = []
        
    for i, encoded_query in enumerate(encoded_queries):
        result_docs = client.ft(INDEX_NAME).search(query, {'query_vector': np.array(encoded_query, dtype=np.float32).tobytes()} | extra_params).docs
        for doc in result_docs:
            vector_score = round(1 - float(doc.vector_score), 2)
            # print(doc.name)
            results_list.append({
                'query': queries[i], 
                'score': vector_score, 
                'id': doc.id,
                'name': doc.name,
            })
            description_embeddings = client.json().get(f'{doc.id}', '$.name_embeddings')
            itemRealSearch.append((description_embeddings[0], (doc.id).split(":")[-1]))

    return itemRealSearch

def ranking_items(itemSessionVector, itemRealSearch):
    scoreList = []
    itemScoreList = []
    for i in itemRealSearch:
        for j in itemSessionVector:
            score = cosine_similarity([i[0]],[j[0]])*j[1]
            scoreList.append(score)
            itemScoreList.append(i[1])

    rank = scipy.stats.rankdata(scoreList)

    # List of index of rank => find item id in itemScoreList (top 10)
    rankScoreIndex = []
    for i in range(1, len(rank) + 1):
        a = np.where(rank == i)
        if len(a) > 0:
            rankScoreIndex = np.append(rankScoreIndex,a)
        else:
            break

    # List of item id in itemScoreList ranked base-on score (de-duplicated)
    rankItemId = []
    for i in rankScoreIndex:
        if int(itemScoreList[int(i)]) not in rankItemId:
            rankItemId.append(int(itemScoreList[int(i)]))
            if len (rankItemId) == 10:
                break
        else:
            continue

    return rankItemId

def getInfo4Session(cusID, redis_client, mysql_config={}):
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor(dictionary=True)
    query_info_4Session = f'''
        SELECT sa.type, sa.productID
        FROM SessionActivity sa
        JOIN ecommerce.Session s ON sa.sessionID = s.id
        WHERE s.customerID = {cusID}
        AND s.createdAt IN (
            SELECT DISTINCT createdAt
            FROM (
                SELECT createdAt
                FROM ecommerce.Session
                WHERE customerID = {cusID}
                ORDER BY createdAt DESC
                LIMIT 4
            ) AS RecentTimestamps
        )
        ORDER BY s.createdAt DESC, sa.productID, sa.type;
    '''
    cursor.execute(query_info_4Session)
    res4Session = cursor.fetchall()
    # itemSessionVector = [(item['productID'], type_to_weight[item['type']]) for item in res4Session]
    itemSessionVector = [(redis_client.json().get(f'ecommerce:product:{item["productID"]}')['description_embeddings'], type_to_weight[item['type']]) for item in res4Session]
    return itemSessionVector
    
def getSearhContent(cusID, mysql_config={}):
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor(dictionary=True)
    query_get_search = f'''
        SELECT content
        FROM SearchSession ss JOIN ecommerce.Session s ON ss.sessionID = s.id
        WHERE customerID = {cusID} and (sessionID in (select distinct id
                                                    from Session 
                                                    where customerID = {cusID}
                                                        and createdAt = (select max(createdAt) 
                                                                        from Session 
                                                                        where customerID = {cusID})))
        ORDER BY s.createdAt desc, searchTime desc
        LIMIT 10;
    '''
    cursor.execute(query_get_search)
    searchContent = cursor.fetchall()
    res = []
    for i in range(len(searchContent)):
        res.append(searchContent[i]['content'])
    return res
    
def checkIndex(client):
    VECTOR_DIMENSION = 768
    try:
        client.ft(INDEX_NAME).info()
        print('Index already exists!')
    except:
        schema = (
            NumericField("$.id", as_name = "id"),
            TextField("$.name", as_name = "name", no_stem=True),
            TextField("$.summary", as_name = "summary", no_stem=True),
            NumericField("$.shop_id", as_name = "shop_id"),
            VectorField('$.name_embeddings', 'FLAT', {
                'TYPE': 'FLOAT32',
                'DIM': VECTOR_DIMENSION,
                'DISTANCE_METRIC': 'COSINE',
                },  as_name='vector_name'),
        )

        # index Definition
        definition = IndexDefinition(prefix=[DOC_PREFIX], index_type=IndexType.JSON)

        # create Index
        client.ft(INDEX_NAME).create_index(fields=schema, definition=definition)

def get_predicted_ratings(user_id, item_ids, redis_host='localhost', redis_port=6379, mysql_config={}):
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
    
    
    # # Connect to Redis
    # r = redis.Redis(host=redis_host, port=redis_port, db=0)

    # Get top 10 similar users from Redis
    # a = timeit.default_timer()
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
    # b =  timeit.default_timer()
    # print('Load sim:', b-a)
    # similar_users = sorted(similar_users, key=lambda x: -float(x[1])) # Sort and get top 10
    
    query_get_sim = f'''
        select user_v, sim
        from Sim
        where user_u = {user_id};
    '''
    cursor.execute(query_get_sim)
    similar_users = cursor.fetchall()
    similar_users_tuples = [(int(user['user_v']), user['sim']) for user in similar_users]
    similar_users_tuples = sorted(similar_users_tuples, key=lambda x: x[1], reverse=True)
    
    cursor.close()
    cnx.close()
    # Function to calculate predicted rating
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

    # Calculate predicted ratings for the provided item_ids
    unrated_products = []
    for product_id in item_ids:
        predicted_rating = calculate_predict_rating(product_id)
        unrated_products.append({'product_id': str(product_id), 'predict_rating': str(predicted_rating)})

    sorted_result = sorted(unrated_products, key=lambda x: float(x['predict_rating']), reverse=True)
    print(sorted_result[:10])
    # Convert the sorted list to a JSON string
    json_result = json.dumps(sorted_result[:10], ensure_ascii=False, indent=4)
    res ={'customer_id': user_id, 'list': json_result}
    res_json = json.dumps(res, ensure_ascii=False, indent=4)
    return res_json


if __name__ == "__main__":
    a = timeit.default_timer()
    client = redis.Redis(host = 'localhost', port=6379, decode_responses=True)
    start = timeit.default_timer()
    print("Load model...")
    embedder = SentenceTransformer('keepitreal/vietnamese-sbert')
    stop = timeit.default_timer()
    print("Load model:", stop - start)
    
    mysql_config = {
        'user': 'root',
        'password': 'Password@123',
        'host': 'localhost',
        'database': 'ecommerce',
    }
    weight = {
        'w_click' : 0.2,
        'w_favorite': 0.3,
        'w_buy': 0.5
    }
    type_to_weight = {
        0: weight['w_click'],    # type 0 -> w_click
        1: weight['w_buy'],      # type 1 -> w_buy
        2: weight['w_favorite']  # type 2 -> w_favorite
    }
    
    query = (
        Query('(*)=>[KNN 20 @vector_name $query_vector AS vector_score]')
            .sort_by('vector_score')
            .return_fields('vector_score', 'id', 'name')
            .paging(0, 20)
            .dialect(2)
    )

    customerID = '10577013'
    
    # checkIndex(client)
    print("Get items 4 SS...")
    itemSessionVector = getInfo4Session(customerID, client, mysql_config)
    print("Get search content...")
    search_content = getSearhContent(customerID, mysql_config)
    print(search_content)
    
    
    while(True):
        print("Encode search...")
        encoded_queries = embedder.encode(search_content)
        print("Search 20 items...")
        itemRealSearch = create_query_table(query, search_content, encoded_queries)
        print("Ranking...")
        rankingItems = ranking_items(itemSessionVector, itemRealSearch)
        
        if (len(rankingItems)) > 0:
            break
    
    print(tuple(rankingItems))
    
    print("Check items...")
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor(dictionary=True)
    query_rated_item = f'''
        select distinct productID
        from ProductReview
        where customerID = {customerID} and productID in {tuple(rankingItems)};
    '''
    cursor.execute(query_rated_item)
    ListRated = cursor.fetchall()
    print(ListRated)
    
    ListPredict = [index for index in rankingItems if index not in ListRated]
    print(ListPredict)
    
    print("Predict rating top 10...")
    start_1 = timeit.default_timer()
    result = get_predicted_ratings(customerID, ListPredict, mysql_config = mysql_config)
    stop_1 = timeit.default_timer()
    print("Predict:", stop_1 - start_1)
    b = timeit.default_timer()
    print('Total time:', b - a)
    # print(result)
    # stop = timeit.default_timer()
    
    json_result = json.dumps(result, ensure_ascii=False, indent=4)
    res ={'customer_id': customerID, 'list': json_result}
    
    data = {'data': res}
    res = requests.post('http://127.0.0.1:8080/api/recommend', json=data)