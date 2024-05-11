import os
import sys
from hdfs import InsecureClient
import pandas as pd
from mrjob.emr import EMRJobRunner
import boto3

sys.path.append(os.path.abspath("./util"))

#from custom_util import run_mr_job, write_data_to_file
from create_combinations import create_combinations
from rating_commodity import rating_commodity
from rating_usefulness import rating_usefulness
from rating_details import rating_details
from rating_time import rating_time
from calculate_mfps import CalculateMFPS

"""
def run_mr_job(mr_job_class, input_args):
    mr_job = mr_job_class(args=input_args)
    with mr_job.make_runner() as runner:
        runner.run()
        data = []
        for key, value in mr_job.parse_output(runner.cat_output()):
            data.append(f"{key}\t{value}")
        return data
"""

def run_mr_job(mr_job_class, input_args):
    mr_job = mr_job_class(args=input_args)
    # Sử dụng HadoopRunner thay vì InlineMRJobRunner
    runner = mr_job.make_runner()
    runner.run()
    data = []
    for key, value in mr_job.parse_output(runner.cat_output()):
        data.append(f"{key}\t{value}")
    return data

def run_mfps(input_path, avg_ratings_path, output_path):
    # Create combinations
    result_data = run_mr_job(create_combinations, [input_path])
    with client.write(f"/output_mfps/create_combinations_1.txt", encoding="utf-8") as writer:
            result_data.to_csv(writer, index=False, header=False)
    # write_data_to_file(("./mfps/output/create_combinations.txt"), result_data)

    # # Calculate rating commodity
    # result_data = run_mr_job(
    #     rating_commodity,
    #     [input_path, "--users-path", avg_ratings_path],
    # )
    # write_data_to_file(("./mfps/output/rating_commodity.txt"), result_data)

    # # Calculate rating usefulness
    # result_data = run_mr_job(
    #     rating_usefulness,
    #     [
    #         input_path,
    #         "--rating-commodity-path",
    #         "./mfps/output/rating_commodity.txt",
    #     ],
    # )
    # write_data_to_file("./mfps/output/rating_usefulness.txt", result_data)

    # # Calculate rating details
    # result_data = run_mr_job(
    #     rating_details,
    #     [
    #         "./mfps/output/create_combinations.txt",
    #         "--avg-rating-path",
    #         avg_ratings_path,
    #     ],
    # )
    # write_data_to_file("./mfps/output/rating_details.txt", result_data)

    # # Calculate rating time
    # result_data = run_mr_job(rating_time, ["./mfps/output/create_combinations.txt"])
    # write_data_to_file("./mfps/output/rating_time.txt", result_data)

    # # Calculate MFPS
    # result_data = run_mr_job(
    #     CalculateMFPS,
    #     [
    #         "./mfps/output/rating_commodity.txt",
    #         "./mfps/output/rating_usefulness.txt",
    #         "./mfps/output/rating_details.txt",
    #         "./mfps/output/rating_time.txt",
    #     ],
    # )
    # write_data_to_file(output_path, result_data)
    return result_data
if __name__ == "__main__": 
    client = InsecureClient("http://localhost:9870", user="hdoop")
    
    NUMBER_OF_CLUSTERS = 1
    mfps_result = []
    for index in range(NUMBER_OF_CLUSTERS):
        input_path = f"data_mrjob/new_data/input_file_copy.txt"
        avg_ratings_path = f"/user/hdoop/new_data/avg_ratings_{index}.txt"
        output_path = f"user/hdoop/new_data/mfps_{index}.txt"

        # with client.read(input_path, encoding="utf-8") as reader:
        #     input_file = pd.read_csv(reader, sep="\t", dtype="str", names=["key", "value"])
        # print(input_file)

        result_data = run_mfps(
            input_path=input_path,
            avg_ratings_path=avg_ratings_path,
            output_path=output_path
        )
        mfps_result.append(result_data)
