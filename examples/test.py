from mapreduce import *
from time import time


def mapper_1(item):
    return -item


def mapper_2(item):
    return (item +5)*23 - 1


def reducer_2(accumulated, item):
    return accumulated + item


def filter_1(item):
    return item % 2 == 0


def timer(start):
    print('  Time       : %8.6fsec' % (time() - start))

def worker_assign(workers, current_worker_id, item):
    return item % workers == current_worker_id

if __name__ == "__main__":
    N = 150000
    ''' Example '''
    print('* map ')
    mr = MapReducer().mapper(mapper_1)
    start = time()
    result = mr(range(N))
    print('  List:', len(list(result)))
    ''' approximate running time '''
    timer(start)

    print('* map & reduce ')
    mr = MapReducer().mapper(mapper_2).reducer(reducer_2,0)
    start = time()
    result=mr(range(N))
    print('  MR Result  :', result)
    timer(start)
    n=sum([(n +5)*23 - 1 for n in range(N)])
    print('  Validation:',n)

    print('* pre-filtering, map, reduce ')
    mr = MapReducer().mapper(mapper_2).reducer(reducer_2,0).prefilter(filter_1)
    start = time()
    result = mr(range(N))
    print('  MR Result  :', result)
    timer(start)
    print('  Validation :', sum([(n +5)*23 - 1 for n in range(N) if n % 2 == 0]))

    print('* pre-filtering, map, reduce with generator input ')
    mr = MapReducer().mapper(mapper_2).reducer(reducer_2,0)\
        .prefilter(filter_1).worker_assigner(worker_assign)
    start = time()
    result = mr(range(N))
    print('  MR Result  :', result)
    timer(start)
    print('  Validation :', sum([(n +5)*23 - 1 for n in range(N) if n % 2 == 0]))
