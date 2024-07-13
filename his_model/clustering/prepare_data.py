import mysql.connector

def updateNewData():
    # host = 'mysql-ecommerce-nhut0789541410-f8ba.e.aivencloud.com'
    # port = '27163'
    # user = 'avnadmin'
    # password = 'AVNS_SQHY8Ivz7J5kp9ElUF2'
    
    host = 'localhost'
    port = '3306'
    user = 'root'
    password = '1234'
    
    # Kết nối Database
    db_config = {
        'user': user,
        'password': password,
        'host':  host,
        'port': port,
        'database': 'ecommerce',
        'raise_on_warnings': True
    }
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    
    # Lấy rating mới
    query_get_new_data = '''
        select customerID, productID, rating, unix_timestamp(createdAt) 
        from ProductReview
        where unix_timestamp(createdAt) > (select max(timestamp) from TrainingData)
    '''
    # Cập nhật rating mới vào bảng huấn luyện 
    cursor.execute(query_get_new_data)
    list_new_data = cursor.fetchall()
    if len(list_new_data) != 0:
        for data in list_new_data:
            query_insert_data = f'''
                insert into TrainingData (user_id, item_id, rating, timestamp) values ({str(data[0])}, {str(data[1])}, {int(data[2])}, {int(data[3])})
            '''
            cursor.execute(query_insert_data)
    
    cnx.commit()
    cursor.close()
    cnx.close()