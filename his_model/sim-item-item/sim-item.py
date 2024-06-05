import redis
import numpy as np
from scipy.spatial.distance import cosine
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.field import TagField, NumericField
from collections import defaultdict
import heapq

# Hàm để lấy name_embeddings từ Redis
def get_name_embedding(key):
    data = redis_client.json().get(key)
    if data:
        return np.array(data.get('name_embeddings'), dtype=np.float32)
    return None

def extract_product_id(key):
    return int(key.decode('utf-8').split(':')[-1])

# Hàm tính cosine similarity
def cosine_similarity(vec1, vec2):
    return 1 - cosine(vec1, vec2)

def initialize_redis_index(redis_client, model_index_name, model_key_base):
    # Tạo một chỉ mục mới
    redis_client.ft(model_index_name).create_index(
        [
            TagField("$.item_u", as_name="item_id_u"),
            TagField("$.item_v", as_name="item_id_v"),
            NumericField("$.sim", as_name="sim")
        ],
        definition=IndexDefinition(
            index_type=IndexType.JSON,
            prefix=[f"{model_key_base}:"]
        )
    )

if __name__ == "__main__":
    print("Connect to Redis...")
    redis_host='localhost'
    redis_port=6379
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
    MODEL_INDEX_NAME = "idx:sim_item"
    MODEL_KEY_BASE = "ecommerce:sim_item"
    
    try:
        redis_client.ft(MODEL_INDEX_NAME).info()
        print('Index already exists!')
    except:
        print('Create Index...')
        initialize_redis_index(redis_client, MODEL_INDEX_NAME, MODEL_KEY_BASE)
    
    print("Get keys....")
    keys = sorted(redis_client.keys('ecommerce:product:*'))
    
    # Lấy name_embeddings cho tất cả các sản phẩm
    print("Get name embedding keys....")
    embeddings = {key: get_name_embedding(key) for key in keys}
    
    # Trích xuất ID sản phẩm cho tất cả các khóa
    print("Get product_id....")
    product_ids = {key: extract_product_id(key) for key in embeddings.keys()}
    
    # Tính cosine similarity giữa từng cặp sản phẩm
    print("Cal Cosine...")
    keys_with_embeddings = list(embeddings.keys())
    similarities = defaultdict(list)
    
    for i, key1 in enumerate(keys_with_embeddings):
        for j, key2 in enumerate(keys_with_embeddings):
            if i < j:  # Để tránh tính lại các cặp đã tính
                sim = cosine_similarity(embeddings[key1], embeddings[key2])
                item_u = product_ids[key1]
                item_v = product_ids[key2]

                # Sử dụng heapq để giữ top 10 giá trị similarity cao nhất cho mỗi item_u
                if len(similarities[item_u]) < 10:
                    heapq.heappush(similarities[item_u], (sim, item_v))
                else:
                    heapq.heappushpop(similarities[item_u], (sim, item_v))
        if i == 20:
            break

    pipeline = redis_client.pipeline(transaction=False)
    for item_u, top_similarities in similarities.items():
        for sim, item_v in top_similarities:
            data = {
                "item_u": str(item_u),
                "item_v": str(item_v),
                "sim": sim
            }
            key = f"{MODEL_KEY_BASE}:{item_u}:{item_v}"
            pipeline.json().set(key, "$", data)
            print(f'Add key: {key} with similarity {sim}')
    pipeline.execute()
    print("Cosine similarities have been stored in Redis.")