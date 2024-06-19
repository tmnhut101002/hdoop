import redis
import re
import mysql.connector
from sentence_transformers import SentenceTransformer
import numpy as np 

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
    print(f"Connecting to Redis.")
    
    pattern = 'ecommerce:product:*'
    all_keys = get_all_keys(pattern, redis_client)

    redis_product_ids = extract_ids(all_keys)
    
    query_get_product_mysql = '''
        select distinct id, name from Product
    '''
    cursor.execute(query_get_product_mysql)
    product_mysql = [data for data in cursor.fetchall()]
    product_mysql_dict = [{'id': str(row[0]), 'name': row[1]} for row in product_mysql]

    new_products = [product for product in product_mysql_dict if product['id'] not in redis_product_ids]

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
    
if __name__ == "__main__":
    embedder = SentenceTransformer('keepitreal/vietnamese-sbert')
    print(f"Connecting to Redis.")
    redis_client = redis.from_url('redis://localhost:6379/?decode_responses=True')
    
    pattern = 'ecommerce:product:*'
    all_keys = get_all_keys(pattern, redis_client)

    redis_product_ids = extract_ids(all_keys)

    db_config = {
        'user': 'root',
        'password': 'Password@123',
        'host': 'localhost',
        'database': 'ecommerce',
        'raise_on_warnings': True
    }
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    
    query_get_product_mysql = '''
        select distinct id, name from Product
    '''
    cursor.execute(query_get_product_mysql)
    product_mysql = [data for data in cursor.fetchall()]
    product_mysql_dict = [{'id': str(row[0]), 'name': row[1]} for row in product_mysql]
    mysql_product_ids = [product['id'] for product in product_mysql_dict]

    new_products = [product for product in product_mysql_dict if product['id'] not in redis_product_ids]

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