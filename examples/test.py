from mapreduce import *


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

def is_prime(n):
    for i in range(2,ceil(sqrt(n))):
        if n%i == 0:
            return False
    return True

if __name__ == "__main__":
    workers = 4
    N = 150000
    ''' Example '''
    start = time()
    mr = MapReducer(range(N), workers=workers, mapper=mapper_1)
    print('* map ')
    print('  List:', len(list(mr.run())))
    ''' approximate running time '''
    timer(mr.started)
    mr = MapReducer(range(N), workers=workers, mapper=mapper_2, reducer=reducer_2, initializer=0)
    print('* map & reduce ')
    print('  MR Result  :', mr.run())
    timer(mr.started)
    n=sum([(n +5)*23 - 1 for n in range(N)])
    print('  Validation:',n)

    mr = MapReducer(range(N), workers=workers, mapper=mapper_2, reducer=reducer_2, prefiltering=filter_1, initializer=0)
    print('* pre-filtering, map, reduce ')
    print('  MR Result  :', mr.run())
    timer(mr.started)
    print('  Validation :', sum([(n +5)*23 - 1 for n in range(N) if n % 2 == 0]))
    mr = MapReducer(
        range(N),
        workers=workers,
        mapper=mapper_2,
        reducer=reducer_2,
        initializer=0,
        prefiltering=filter_1,
        worker_assign=worker_assign
    )
    print('* pre-filtering, map, reduce with generator input ')
    print('  MR Result  :', mr.run())
    timer(mr.started)
    print('  Validation :', sum([(n +5)*23 - 1 for n in range(N) if n % 2 == 0]))
