import os
import pandas as pd
import mysql.connector

# Thiết lập thông tin kết nối MySQL
host = 'mysql-ecommerce-nhut0789541410-f8ba.e.aivencloud.com'
port = '27163'
user = 'avnadmin'
password = 'AVNS_SQHY8Ivz7J5kp9ElUF2'
database = 'ecommerce'  # Thay bằng tên database của bạn

# Kết nối MySQL
connection = mysql.connector.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    database=database
)

cursor = connection.cursor()

print('Save AVGRating')
cursor.execute("TRUNCATE TABLE AVGRating")
# Tạo bảng AVGRating
cursor.execute("""
    INSERT INTO AVGRating (user_id, avg_rating)
    SELECT customerID AS user_id, AVG(rating) AS avg_rating 
    FROM ProductReview 
    GROUP BY customerID
""")

cursor.execute("TRUNCATE TABLE TrainingData_Cluster")
directory = '../mfps/temp_preSIM'

# Đọc và xử lý từng tệp
print('Save label')
for filename in os.listdir(directory):
    if filename.startswith('avg_') and filename.endswith('.txt'):
        # Lấy giá trị centroid từ tên tệp
        centroid = filename.split('_')[1].split('.')[0]
        
        # Đọc tệp vào DataFrame của pandas
        filepath = os.path.join(directory, filename)
        df = pd.read_csv(filepath, sep='\t', header=None, names=['user_id', 'value'])
        
        # Thêm cột label với giá trị centroid
        df['label'] = centroid
        
        # Ghi dữ liệu vào bảng MySQL
        for index, row in df.iterrows():
            print(f'{i} Add {index}')
            cursor.execute("""
                INSERT INTO TrainingData_Cluster (user_id, label)
                VALUES (%s, %s)
                """, (row['user_id'], row['label'])
            )

# Lưu các thay đổi và đóng kết nối
connection.commit()
cursor.close()
connection.close()
