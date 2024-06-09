from ratingCommodity import RatingCommodity
from ratingDetails import RatingDetails
from ratingUsefullness import RatingUsefullness
from ratingTime import RatingTime
from mfps import mfps
import timeit

INPUT = '../input_file.txt'
MFPS =  'hdfs://localhost:9000/MFPS/mfps.csv'
AVG =  'hdfs://localhost:9000/Clustering/AverageRating.csv'
if  __name__ == "__main__":
    start =  timeit.default_timer()
    RatingCommodity(INPUT)
    rcTime = timeit.default_timer()
    RatingUsefullness(INPUT)
    ruTime = timeit.default_timer()
    RatingDetails(INPUT, AVG)
    rdTime = timeit.default_timer()
    RatingTime(INPUT)
    rtTime = timeit.default_timer()

    mfps(rc,ru,rd,rt,MFPS)

    stop = timeit.default_timer()
    with  open('./output/time.txt','a') as f:
        f.write(f'Start: {start} ... End: {stop}\n')
        f.write(f'mfps: {stop-start}s \n')
        f.write(f'Rating Commodities: {rcTime-start}s\n')
        f.write(f'Rating Usefulness: {ruTime-rcTime}s\n')
        f.write(f'Rating Details: {rdTime-ruTime}s\n')
        f.write(f'Rating Time: {rtTime-rdTime}s\n')