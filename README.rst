A Multi-processing based single-host MapReduce Framework
========================================================

This is a simple mapreduce framework.

Sample program
----------------------
.. code-block:: python

    from mapreduce import *
    from math import ceil, sqrt


    def mapper_2(item):
        return (item + 5) * 23 - 1


    def reducer_2(accumulated, item):
        return accumulated + item


    def is_prime(n):
        for i in range(2, ceil(sqrt(n))):
            if n % i == 0:
                return False
        return True


    if __name__ == "__main__":
        N = 150000
        print('* map & reduce ')
        mr = MapReducer().prefilter(is_prime).mapper(mapper_2).reducer(reducer_2, 0)
        result = mr(range(N))
        print('  MR Result  :', result)
        n = sum([(n + 5) * 23 - 1 for n in range(N) if is_prime(n)])
        print('  Validation:', n)


