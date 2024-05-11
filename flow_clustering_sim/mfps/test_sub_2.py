import subprocess
import logging
import time

# Cấu hình logging
logging.basicConfig(level=logging.INFO)

# Lệnh để xóa thư mục
delete_command = 'hdfs dfs -rm -r /new_data/mfps_output/group_{0}'

# Danh sách lệnh cần chạy
commands = [
    'python3 create_combinations.py -r hadoop hdfs:///new_data/clustering/input_file_{0}.txt --output hdfs:///new_data/mfps_output/group_{0}/create_combinations_group_{0}',
    'python3 rating_commodity.py -r hadoop hdfs:///new_data/clustering/input_file_{0}.txt --users-path hdfs:///new_data/clustering/avg_ratings_{0}.txt --output hdfs:///new_data/mfps_output/group_{0}/rating_commodity_group_{0}',
    'python3 rating_usefulness.py -r hadoop hdfs:///new_data/clustering/input_file_{0}.txt --rating-commodity-path hdfs:///new_data/mfps_output/group_{0}/rating_commodity_group_{0}/part-00000 --output hdfs:///new_data/mfps_output/group_{0}/rating_usefulness_group_{0}',
    'python3 rating_details.py -r hadoop hdfs:///new_data/mfps_output/group_{0}/create_combinations_group_{0}/part-00000 --avg-rating-path hdfs:///new_data/clustering/avg_ratings_{0}.txt --output hdfs:///new_data/mfps_output/group_{0}/rating_details_group_{0}',
    'python3 rating_time.py -r hadoop hdfs:///new_data/mfps_output/group_{0}/create_combinations_group_{0} --output hdfs:///new_data/mfps_output/group_{0}/rating_time_group_{0}',
    'python3 calculate_mfps.py -r hadoop hdfs:///new_data/mfps_output/group_{0}/rating_commodity_group_{0} hdfs:///new_data/mfps_output/group_{0}/rating_usefulness_group_{0} hdfs:///new_data/mfps_output/group_{0}/rating_details_group_{0} hdfs:///new_data/mfps_output/group_{0}/rating_time_group_{0} --output hdfs:///new_data/mfps_output/group_{0}/mfps_group_{0}'
]

# Số lần lặp
num_iterations = 5

# Vòng lặp chạy 5 lần
for i in range(num_iterations):
    group_number = i
    logging.info(f"Running iteration {i} with group_{group_number}")
    
    # Xóa thư mục của group cũ nếu tồn tại
    subprocess.run(delete_command.format(group_number), shell=True, check=False)
    
    # Vòng lặp qua danh sách lệnh và thực thi từng lệnh
    for command in commands:
        try:
            command = command.format(group_number)
            logging.info(f"Running command: {command}")
            subprocess.run(command, shell=True, check=True)
            logging.info(f"Command executed successfully: {command}")

            # Đợi 30 giây trước khi chạy lệnh tiếp theo
            logging.info("Waiting for 30 seconds...")
            time.sleep(30)

        except subprocess.CalledProcessError as e:
            logging.error(f"Error running command: {command}. Error: {e}")

