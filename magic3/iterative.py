# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import random
from _operator import mul as _mul, itemgetter as _itemgetter
from _collections import deque
from itertools import islice, count, chain, starmap, repeat, cycle, filterfalse, groupby, tee, combinations, zip_longest


def tail(n, iterable):
    " return an iterator over the last n items: tail(3, 'ABCDEFG') --> E F G "
    return iter(deque(iterable, maxlen=n))

def advance(iterator, n):
    " advance the iterator n-steps ahead. If n is none, consume entirely "
    if n is None:
        deque(iterator, maxlen=0)
    else:
        next(islice(iterator, n, n), None)

def pad_none(iterable):
    """ returns the sequence elements and then returns None indefinitely """
    return chain(iterable, repeat(None))

def repeat_call(func, times=None, *args):
    """ repeat calls to func with specified arguments """
    if times is None:
        return starmap(func, repeat(args))
    return starmap(func, repeat(args, times))

def round_robin(*iterables):
    """ roundrobin('ABC', 'D', 'EF') --> A D E B F C """
    pending = len(iterables)
    nexts = cycle(iter(it).__next__ for it in iterables)
    while pending:
        try:
            for next_ in nexts:
                yield next_()
        except StopIteration:
            pending -= 1
            nexts = cycle(islice(nexts, pending))

def partition(pred, iterable):
    """ use a predicate to partition entries into false entries and true entries 
        partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9 """
    t1, t2 = tee(iterable)
    return filterfalse(pred, t1), filter(pred, t2)

def powerset(iterable):
    " powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3) "
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s) + 1))

def iter_except(func, exception, first=None):
    """ call a function repeatedly until an exception is raised. Converts 
        a call-until-exception interface to an iterator interface. Like 
        builtins.iter(func, sentinel) but uses an exception instead of a 
        sentinel to end the loop. Examples:
        iter_except(functools.partial(heappop, h), IndexError)   # priority queue iterator
        iter_except(d.popitem, KeyError)                         # non-blocking dict iterator
        iter_except(d.popleft, IndexError)                       # non-blocking deque iterator
        iter_except(q.get_nowait, Queue.Empty)                   # loop over a producer Queue
        iter_except(s.pop, KeyError)                             # non-blocking set iterator
    """
    try:
        if first is not None:
            yield first()  # For database APIs needing an initial cast to db.first()
        while 1:
            yield func()
    except exception:
        pass

def first_true(iterable, default=False, pred=None):
    """ returns the first true value in the iterable. If no true value is found, 
        returns *default* If *pred* is not None, returns the first item for which
        pred(item) is true. 
        first_true([a,b,c], x) --> a or b or c or x
        first_true([a,b], x, f) --> a if f(a) else b if f(b) else x """
    return next(filter(pred, iterable), default)

def random_select(iterable, n):
    """ select n random elements from iterable """
    pool = tuple(iterable)
    indices = sorted(random.sample(range(len(pool)), n))
    return tuple(pool[i] for i in indices)

def random_product(*args, repeat=1):
    " random selection from itertools.product(*args, **kwds) "
    pools = [tuple(pool) for pool in args] * repeat
    return tuple(random.choice(pool) for pool in pools)

def random_permutation(iterable, r=None):
    " random selection from itertools.permutations(iterable, r) "
    pool = tuple(iterable)
    r = len(pool) if r is None else r
    return tuple(random.sample(pool, r))

def randomCombination(iterable, r):
    " random selection from itertools.combinations(iterable, r) "
    pool = tuple(iterable)
    n = len(pool)
    indices = sorted(random.sample(range(n), r))
    return tuple(pool[i] for i in indices)

def random_combination(iterable, r):
    "random selection from itertools.combinations_with_replacement(iterable, r)"
    pool = tuple(iterable)
    n = len(pool)
    indices = sorted(random.randrange(n) for _ in range(r))
    return tuple(pool[i] for i in indices)

def dot_product(vec1, vec2):
    """ dot product of vectors, len(vec1) should be equalto len(vec2) """
    return sum(map(_mul, vec1, vec2))

def ntake(iterable, start, n):
    """ take n elements from start pos from iterable """
    return islice(iterable, start, start + n, 1)

def foreach2(func1, func2, iterable):
    """ for each element in iterable, call fun2(fun1(i)) """
    for each in iterable:
        yield func2(func1(each))

def until_fail(fun, exception, iterable, *args):
    """ call fun with element in iterable and args until exception specified raised """
    n = 0
    try:
        for i in iterable:
            fun(i, *args)
            n += 1
    except exception:
        pass
    return n

def nth(iterable, n, default=None):
    """ get the n-th element from iterable """
    return next(islice(iterable, n, None), default)

def quantify(iterable, pred=bool):
    """ sum of pred(iterable) """
    return sum(map(pred, iterable))

def chain_cycles(iterable, n):
    """ cycle iterators of iterable, n times """
    return chain.from_iterable(repeat(tuple(iterable), n))

def flatten(nested):
    """ flat nested containers, return chained iterator """
    return chain.from_iterable(nested)

def pairwise(iterable):
    """ make pair of iterable using tee """
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

def grouper(iterable, n, fillvalue=None):
    """ put elements in n groups, aligned with fillvalue """
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)

def ever_seen(iterable, key=None):
    """ yield only seen before element from iterable """
    seen = set()
    seen_add = seen.add
    if key is None:
        for element in filterfalse(seen.__contains__, iterable):
            seen_add(element)
            yield element
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element

def just_seen(iterable, key=None):
    """ yield just seen element from iterable """
    return map(next, map(_itemgetter(1), groupby(iterable, key)))


import unittest
class TestIteralgos(unittest.TestCase):
    """ unit tester for iteralgos """
    def setUp(self):
        self.p = [1, 3, 5, 7, 9]
        self.q = [0, 2, 4, 6, 8]
        self.a = [1, 2, 3, 4, 5]
        self.s = ['aa', 'bb', 'cc', 'dd', 'ee']
    
    def tearDown(self):
        pass
    
    def test_padNone(self):
        r = pad_none([1, 2, 3])
        self.assertEqual(next(r), 1)
        self.assertEqual(next(r), 2)
        self.assertEqual(next(r), 3)
        self.assertEqual(next(r), None)
    
    def test_tail(self):
        self.assertEqual(list(tail(4, 'ABCDEFGHIJK')), ['H', 'I', 'J', 'K'])
        
    def test_powerset(self):
        self.assertEqual(list(powerset([1, 2, 3])), [(), (1,), (2,), (3,), (1, 2), (1, 3), (2, 3), (1, 2, 3)])
    
    def test_dotProduct(self):
        self.assertEqual(dot_product(self.p, self.q), 140)
    
    def test_advance(self):
        i = iter(self.a)
        advance(i, 3)
        self.assertEqual(next(i), 4)
    
    def test_partition(self):
        i0, i1 = partition(lambda x: x & 1, range(10))
        self.assertEqual(list(i0), [0, 2, 4, 6, 8])
        self.assertEqual(list(i1), [1, 3, 5, 7, 9])
    
    def test_ntake(self):
        self.assertEqual(['aa', 'bb'], list(ntake(self.s, 0, 2)))
    
    def test_untilFail(self):
        def _fun(x):
            return x * 2
        self.assertEqual(until_fail(_fun, StopIteration, self.q), 5)
    
    def test_nth(self):
        self.assertEqual(nth(self.s, 3, None), 'dd')
        self.assertEqual(nth(self.s, 8, None), None)
    
    def test_quantify(self):
        self.assertEqual(quantify(self.q, lambda x : x > 5), 2)
        self.assertEqual(quantify(self.s, lambda s : s == 'ee'), 1)
    
    def test_chainCycles(self):
        a = (3, 2, 1)
        self.assertEqual(list(chain_cycles(a, 2)), [3, 2, 1, 3, 2, 1])
    
    def test_flatten(self):
        ll = [{1, 2}, [3, 4], (5, 6)]
        self.assertEqual(list(flatten(ll)), [1, 2, 3, 4, 5, 6])
    
    def test_repeatCall(self):
        n = 0
        for i in repeat_call(random.random, 3):
            n += 1
            self.assertTrue(i >= 0 and i <= 1.0)
        self.assertEqual(n, 3)
    
    def test_pairwise(self):
        self.assertEqual(list(pairwise(range(6))), [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)])
    
    def test_grouper(self):
        self.assertEqual(list(grouper(self.p, 2, None)), [(1, 3), (5, 7), (9, None)])
    
    def test_roundRobin(self):
        a = ['aa', 0, 1, 'bb', 2, 3, 'cc', 4, 5, 'dd', 6, 7, 'ee', 8, 9]
        self.assertEqual(list(round_robin(self.s, self.q, self.p)), a)
    
    def test_everSeen(self):
        self.assertEqual(list(ever_seen("AAABCCDDDDEFABC")), ['A', 'B', 'C', 'D', 'E', 'F'])
    
    def test_justSeen(self):
        self.assertEqual(list(just_seen("AAABBCCDDDDEFABC")),
                         ['A', 'B', 'C', 'D', 'E', 'F', 'A', 'B', 'C'])
    
    def test_firstTrue(self):
        self.assertEqual(first_true([{}, 0, None, False, 'bingo', 1, 2, 3, 0], False), 'bingo')
    
    def test_randomSelect(self):
        self.assertEqual(len(random_select(self.a, 3)), 3)
    
    def test_randomProduct(self):
        m, n = random_product(self.p, self.q)
        self.assertIn(m, self.p)
        self.assertIn(n, self.q)
        
    def test_randomPermutation(self):
        r = random_permutation(self.p)
        for i in r:
            self.assertIn(i, r)
        self.assertEqual(len(r), len(self.p))

    def test_randomCombination(self):
        r = random_combination(self.s, 4)
        for i in r:
            self.assertIn(i, r)
        self.assertEqual(len(r), 4)


if __name__ == '__main__':
    unittest.main()


