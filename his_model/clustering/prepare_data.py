import mysql.connector

def updateNewData():
    db_config = {
        'user': 'root',
        'password': 'Password@123',
        'host': 'localhost',
        'database': 'ecommerce',
        'raise_on_warnings': True
    }
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    
    query_get_new_data = '''
        select customerID, productID, rating, unix_timestamp(createdAt) 
        from ProductReview
        where unix_timestamp(createdAt) > (select max(timestamp) from TrainingData) 
    '''
    
    cursor.execute(query_get_new_data)
    list_new_data = cursor.fetchall()
    if len(list_new_data) != 0:
        for data in list_new_data:
            query_insert_data = f'''
                insert into TrainingData values ({str(data[0])}, {str(data[1])}, {int(data[2])}, {int(data[3])})
            '''
            cursor.execute(query_insert_data)
    
    cnx.commit()
    cursor.close()
    cnx.close()

if __name__ == "__main__":
    db_config = {
        'user': 'root',
        'password': 'Password@123',
        'host': 'localhost',
        'database': 'ecommerce',
        'raise_on_warnings': True
    }
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    
    query_get_new_data = '''
        select customerID, productID, rating, unix_timestamp(createdAt) 
        from ProductReview
        where unix_timestamp(createdAt) > (select max(timestamp) from TrainingData) 
    '''
    
    cursor.execute(query_get_new_data)
    list_new_data = cursor.fetchall()
    if len(list_new_data) != 0:
        for data in list_new_data:
            query_insert_data = f'''
                insert into TrainingData values ({str(data[0])}, {str(data[1])}, {int(data[2])}, {int(data[3])})
            '''
            cursor.execute(query_insert_data)
    
    cnx.commit()
    cursor.close()
    cnx.close()