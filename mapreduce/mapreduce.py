import collections
import os
import itertools
import multiprocessing as mp
from math import ceil,sqrt
from types import GeneratorType as generator
from functools import partial,reduce
from time import time
from typing import Iterable,Callable

_cpu_cores = mp.cpu_count()


class MapReducer(object):
    def __init__(self, iterable:Iterable, workers:int = 0, mapper:Callable = None,
                 prefiltering:Callable = None, postfiltering:Callable = None,
                 reducer:Callable = None, merger:Callable = None, initializer = None,
                 worker_assign:Callable = None):
        """

        :param iterable:
        :param workers: Number of processes to be spawned, 0 means all cpu cores
        :param mapper: function to apply to items
        :param prefiltering: function to filter items before entering map phase
        :param postfiltering: function to filter items after map phase
        :param reducer: function passed to reduce for the reduce phases (also the final phase)
        :param merger: function passed to merge final results
        :param initializer: initializer value passed only the final reduce phase
        :param worker_assign: function to assign items to workers (used for generators only)
        """
        if workers < 0:
            raise ValueError("Workers can't be negative!")
        self.started = time()
        self.iterable = iterable
        self.MR_Manager = None
        self.workers = workers
        if workers == 0:
            self.real_workers = mp.cpu_count()
        else:
            self.real_workers = workers
        self.mapper = mapper
        self.prefiltering = prefiltering
        self.postfiltering = postfiltering
        self.reducer = reducer
        self.merger = merger
        self.initializer = initializer
        self.worker_assign = worker_assign

    def mapper(self, item):
        ''' add any extra handling for the map phase here '''
        return self.mapper(item)

    def process(self, items, manager):
        itemlist = items
        if self.prefiltering is not None:
            itemlist = filter(self.prefiltering, itemlist)
        itemlist = map(self.mapper, itemlist)
        if self.postfiltering is not None:
            itemlist = filter(self.postfiltering, itemlist)
        if self.reducer is not None:
            ''' chain will resolve the issue of non-uniform return values '''
            manager.append(reduce(self.reducer, itemlist, self.initializer))
        else:
            manager.extend(list(itemlist))

    def run(self):
        m = mp.Manager()
        ''' manager object to store the result from all processes '''
        self.MR_Manager = m.list()
        processes = []
        step = None
        if isinstance(self.iterable, collections.abc.Sequence):
            l = len(self.iterable)
            step = int(ceil(l / self.real_workers))
        ''' setup processes '''
        for worker_id in range(self.real_workers):
            ''' if the length of the iterable is known slice the iterable and assign to each worker '''
            if l is not None:
                P = mp.Process(target=self.process,
                               args=(itertools.islice(self.iterable, worker_id * step, (worker_id + 1) * step), self.MR_Manager))
            else:
                ''' 
                create a partial since we need to pass the worker count and 
                current worker id to the assignment filter 
                '''
                assignment = partial(self.worker_assign, self.real_workers, worker_id)
                ''' otherwise filter elements with worker_assign '''
                P = mp.Process(target=self.process,
                               args=(filter(assignment, self.iterable, ), self.MR_Manager))
            processes.append(P)
        ''' start & run '''
        start = time()
        [p.start() for p in processes]
        [p.join() for p in processes]
        print(timer(start))
        ''' final reduce phase '''
        itemlist = itertools.chain(self.MR_Manager)
        if self.merger is not None:
            ''' reduce cannot handle None initializer, but works as expected if we omit the parameter altogether '''
            if self.initializer is not None:
                return reduce(self.merger, itemlist, self.initializer)
            else:
                return reduce(self.merger, itemlist)
        else:
            if self.initializer is not None:
                return reduce(self.reducer, itemlist, self.initializer)
            else:
                return reduce(self.reducer, itemlist)


''' Helper Functions for the examples '''


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
    item % workers == current_worker_id

def is_prime(n):
    for i in range(2,ceil(sqrt(n))):
        if n%i == 0:
            return False
    return True

if __name__ == "__main__":
    workers = 4
    N = 10000000
    ''' Example '''
    start = time()
    mr = MapReducer(list(range(N)), workers=workers, mapper=mapper_1)
    print('* map ')
    # print('  List:', list(mr.run()))
    ''' approximate running time '''
    timer(mr.started)
    mr = MapReducer(list(range(N)), workers=workers, prefiltering=is_prime,mapper=mapper_2, reducer=reducer_2, initializer=0)
    print('* map & reduce ')
    print('  MR Result  :', mr.run())
    timer(mr.started)
    print('  Validation :', sum([(n +5)*23 - 1 for n in range(N)]))
    lst=list(range(N))
    start = time()
    n=sum([(n +5)*23 - 1 for n in range(N) if is_prime(n)])
    timer(start)
    print(n)
    start = time()
    n=0
    for i in range(N):
        if is_prime(i):
            n+=(i +5)*23 - 1
    timer(start)
    print(n)
    mr = MapReducer(list(range(N)), workers=workers, mapper=mapper_2, reducer=reducer_2, prefiltering=filter_1, initializer=0)
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
