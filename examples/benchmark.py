from mapreduce import *


def mapper_2(item):
    return (item +5)*23 - 1


def reducer_2(accumulated, item):
    return accumulated + item


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
    workers = 1
    N = 1500000

    mr = MapReducer(range(N), workers=workers, prefiltering=is_prime, mapper=mapper_2, reducer=reducer_2, initializer=0)
    print('* map & reduce ')
    print('  MR Result  :', mr.run())
    timer(mr.started)

    mr = MapReducer(range(N), workers=workers, prefiltering=is_prime, mapper=mapper_2, reducer=reducer_2, initializer=0,
                    worker_assign = worker_assign)
    print('* map & reduce with work assign function ')
    print('  MR Result  :', mr.run())
    timer(mr.started)

    lst=list(range(N))
    start = time()
    n=sum([(n +5)*23 - 1 for n in range(N) if is_prime(n)])
    print('* Validation using list comprehension:',n)
    timer(start)
    start = time()
    n=0
    for i in range(N):
        if is_prime(i):
            n+=(i +5)*23 - 1
    print('* Validation using for loop:',n)
    timer(start)

    r=range(N)
    r=filter(is_prime,r)
    r=map(mapper_2,r)
    n=reduce(reducer_2,r)
    print('* Validation using for map filter reduce:', n)
    timer(start)



