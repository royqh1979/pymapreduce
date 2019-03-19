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
        result = self.initializer
        if self.prefiltering is not None and self.postfiltering is not None:
            for item in items:
                if not self.prefiltering(item):
                    continue
                new_item = self.mapper(item)
                if not self.postfiltering(item):
                    continue
                result = self.reducer(result, new_item)
        elif self.prefiltering is None and self.postfiltering is not None:
            for item in items:
                new_item = self.mapper(item)
                if not self.postfiltering(item):
                    continue
                result = self.reducer(result, new_item)
        elif self.prefiltering is not None and self.postfiltering is None:
            for item in items:
                if not self.prefiltering(item):
                    continue
                new_item = self.mapper(item)
                result = self.reducer(result, new_item)
        else:
            for item in items:
                new_item = self.mapper(item)
                result = self.reducer(result, new_item)
        manager.append(result)




    def run(self):
        if self.reducer is None:
            return mp.Pool().map(self.mapper, self.iterable)
        m = mp.Manager()
        ''' manager object to store the result from all processes '''
        self.MR_Manager = m.list()
        processes = []
        ''' setup processes '''
        for worker_id in range(self.real_workers):
            ''' if the length of the iterable is known slice the iterable and assign to each worker '''
            if self.worker_assign is None:
                P = mp.Process(target=self.process,
                               args=(itertools.islice(self.iterable, worker_id,None,self.real_workers), self.MR_Manager))
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
        [p.start() for p in processes]
        [p.join() for p in processes]
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



