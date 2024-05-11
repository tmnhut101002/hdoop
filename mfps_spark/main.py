from ratingCommodity import RatingCommodity
from ratingDetails import RatingDetails
from ratingUsefullness import RatingUsefullness
from ratingTime import RatingTime
from mfps import mfps

if  __name__ == "__main__":
    NUMBER_OF_CLUSTERS = 5
    for index in range(NUMBER_OF_CLUSTERS):
        INPUT = f"hdfs:///new_data/clustering/input_file_{index}.txt"
        AVG =  f"hdfs:///new_data/clustering/avg_ratings_{index}.txt"
        rc = f"hdfs:///MFPS/group_{index}/rc"
        rd = f"hdfs:///MFPS/group_{index}/rd"
        ru = f"hdfs:///MFPS/group_{index}/ru"
        rt = f"hdfs:///MFPS/group_{index}/rt"
        #RatingCommodity(INPUT, rc)
        #RatingUsefullness(INPUT, ru)
        #RatingDetails(INPUT, rd, AVG)
        #RatingTime(INPUT, rt)
        MFPS = f"hdfs:///MFPS/full_sim"
        mfps(rc, ru, rd, rt, MFPS)
