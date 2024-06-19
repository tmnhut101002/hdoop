import redis
import numpy as np
from scipy.spatial.distance import cosine
from sentence_transformers import SentenceTransformer
from redis.commands.search.query import Query
import mysql.connector
import re

def get_all_keys(pattern, redis_client):
    cursor = '0'
    keys = []
    while cursor != 0:
        cursor, partial_keys = redis_client.scan(cursor=cursor, match=pattern)
        keys.extend([key.decode('utf-8') for key in partial_keys])
    return keys

def extract_ids(keys):
    pattern = re.compile(r'ecommerce:product:(\d+)')
    ids = [pattern.search(str(key)).group(1) for key in keys if pattern.search(key)]
    return ids

def update_product(redis_client, cursor, embedder):
    pattern = 'ecommerce:product:*'
    all_keys = get_all_keys(pattern, redis_client)

    redis_product_ids = extract_ids(all_keys)
    
    query_get_product_mysql = '''
        select distinct id, name from Product
    '''
    cursor.execute(query_get_product_mysql)
    product_mysql_dict = [data for data in cursor.fetchall()]
    new_products = [product for product in product_mysql_dict if str(product['id']) not in redis_product_ids]

    pipeline = redis_client.pipeline(transaction=False)
    i = 0
    for product in new_products:
        name_embedding = embedder.encode(product['name']).astype(np.float32).tolist()  
        product_key = f"ecommerce:product:{product['id']}"
        product_data = {
            'id': product['id'],
            'name': product['name'],
            'name_embeddings': name_embedding
        }
        print(f"{i}. New product added - ID: {product['id']}, Name: {product['name']}")
        pipeline.json().set(product_key, '$', product_data)
        pipeline.execute()
        i += 1

def get_name_embedding(redis_client, key):
    data = redis_client.json().get(key)
    if data:
        return np.array(data.get('name_embeddings'), dtype=np.float32)
    return None

def extract_product_id(key):
    return int(key.decode('utf-8').split(':')[-1])

def cosine_similarity(vec1, vec2):
    return 1 - cosine(vec1, vec2)

def create_query_table(client, query, encoded_name_product, extra_params = {}, INDEX_NAME = 'idx:product-name'):
    itemSimilarity = []
    results_list = []
    result_docs = client.ft(INDEX_NAME).search(query, {'query_vector': np.array(encoded_name_product, dtype=np.float32).tobytes()} | extra_params).docs
    for doc in result_docs:
        vector_score = round(1 - float(doc.vector_score), 2)
        results_list.append({
                'score': vector_score,
                'id': doc.id,
                # 'name': doc.name,
            })
        if vector_score != 1.0:
            itemSimilarity.append(((doc.id).split(":")[-1], vector_score))
        
    return itemSimilarity

if __name__ == '__main__':
    redis_host = 'localhost'
    redis_port = 6379
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
    mysql_config = {
        'user': 'root',
        'password': 'Password@123',
        'host': 'localhost',
        'database': 'ecommerce',
    }
    
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor(dictionary=True)
    
    embedder = SentenceTransformer('keepitreal/vietnamese-sbert')
    print("Updating products...")
    update_product(redis_client, cursor, embedder)
    query = (
            Query('(*)=>[KNN 21 @vector_name $query_vector AS vector_score]')
                .sort_by('vector_score')
                .return_fields('vector_score', 'id', 'name')
                .paging(0, 21)
                .dialect(2)
        )

    query_list_item = ''' Select id from Product'''
    cursor.execute(query_list_item)
    ListItem = cursor.fetchall()
    
    query_truncate_sim = '''TRUNCATE SimItem'''
    cursor.execute(query_truncate_sim)
    i = 1
    for id in ListItem:
        print(id)
        query_get_name = f'''
            SELECT name
            FROM Product
            where id = {id['id']}
        '''
        cursor.execute(query_get_name)
        item_name = cursor.fetchall()
        encoded_name_product = embedder.encode(item_name[0]['name'])
        print('Get top 20 items...')
        while(True):
            itemSim = create_query_table(redis_client, query, encoded_name_product)
            if (len(itemSim)) > 0:
                    break
        print('Insert item', i)
        for data in itemSim:
            item_id = int(id['id'])
            item_rec = int(data[0])
            score = data[1]
            cursor.execute('insert into SimItem (item_id, item_rec, score) values (%s, %s, %s)',(item_id, item_rec, score))
        cnx.commit()
        i += 1
