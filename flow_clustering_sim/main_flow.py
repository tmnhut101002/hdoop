import subprocess
import logging
import time
import os
import sys
import clustering.main

# Cấu hình logging
logging.basicConfig(level=logging.INFO)

# Lệnh để xóa thư mục
check_directory_command = 'hdfs dfs -test -d /new_data/clustering'
delete_cluster_command = 'hdfs dfs -rm -r /new_data/clustering'
delete_sim_command = 'hdfs dfs -rm -r /new_data/mfps_output/group_{0}'

command_clustering = 'spark-submit clustering/main.py'

command_split_file = 'python3 clustering/split_files_by_lable.py'

# Danh sách lệnh cần chạy
commands_mfps = [
    'python3 mfps/create_combinations.py -r hadoop hdfs:///new_data/clustering/input_file_{0}.txt --output hdfs:///new_data/mfps_output/group_{0}/create_combinations_group_{0}',
    'python3 mfps/rating_commodity.py -r hadoop hdfs:///new_data/clustering/input_file_{0}.txt --users-path hdfs:///new_data/clustering/avg_ratings_{0}.txt --output hdfs:///new_data/mfps_output/group_{0}/rating_commodity_group_{0}',
    'python3 mfps/rating_usefulness.py -r hadoop hdfs:///new_data/clustering/input_file_{0}.txt --rating-commodity-path hdfs:///new_data/mfps_output/group_{0}/rating_commodity_group_{0}/part-00000 --output hdfs:///new_data/mfps_output/group_{0}/rating_usefulness_group_{0}',
    'python3 mfps/rating_details.py -r hadoop hdfs:///new_data/mfps_output/group_{0}/create_combinations_group_{0}/part-00000 --avg-rating-path hdfs:///new_data/clustering/avg_ratings_{0}.txt --output hdfs:///new_data/mfps_output/group_{0}/rating_details_group_{0}',
    'python3 mfps/rating_time.py -r hadoop hdfs:///new_data/mfps_output/group_{0}/create_combinations_group_{0} --output hdfs:///new_data/mfps_output/group_{0}/rating_time_group_{0}',
    'python3 mfps/calculate_mfps.py -r hadoop hdfs:///new_data/mfps_output/group_{0}/rating_commodity_group_{0} hdfs:///new_data/mfps_output/group_{0}/rating_usefulness_group_{0} hdfs:///new_data/mfps_output/group_{0}/rating_details_group_{0} hdfs:///new_data/mfps_output/group_{0}/rating_time_group_{0} --output hdfs:///new_data/mfps_output/group_{0}/mfps_group_{0}'
]

# Run Clustering
'''
try:
    logging.info(f"Running command: {command_clustering}")
    subprocess.run(command_clustering, shell=True, check=True)
    logging.info(f"Command executed successfully: {command_clustering}")

    logging.info("Waiting for 30 seconds...")
    time.sleep(30)

except subprocess.CalledProcessError as e:
    logging.error(f"Error running command: {command_clustering}. Error: {e}")

# Run split file
try:
    logging.info(f"Running command: {command_split_file}")
    check_result = subprocess.run(check_directory_command, shell=True)
    if check_result.returncode == 0:
        # If the directory exists, proceed with deletion
    	subprocess.run(delete_cluster_command, shell=True, check=True)
    	print("Directory deleted successfully.")
    #subprocess.run(delete_cluster_command, shell=True, check=True)
    subprocess.run(command_split_file, shell=True, check=True)
    logging.info(f"Command executed successfully: {command_split_file}")

    logging.info("Waiting for 30 seconds...")
    time.sleep(30)

except subprocess.CalledProcessError as e:
    logging.error(f"Error running command: {command_split_file}. Error: {e}")
'''

num_iterations = 3

for i in range(num_iterations):
    group_number = i
    logging.info(f"Running iteration {i} with group_{group_number}")

    subprocess.run(delete_sim_command.format(group_number), shell=True, check=False)

    for command in commands_mfps:
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

