# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import sys
import random
import array 
from _collections import defaultdict
from _operator import itemgetter
import heapq

class BiMap(object):
    """ like bimap, can search v from k and search k from v,
        ensure k and v must be unique, using this as dict
    """
    __slots__ = ('_kv', '_vk')
    def __init__(self, hasher = None):
        super().__init__()
        self._kv = {}
        self._vk = {}
    
    def __str__(self):
        return '%s\n%s' % (self._kv, self._vk)

    def __len__(self):
        assert len(self._kv) == len(self._vk)
        return len(self._kv)
    
    def size(self):
        return (self.__len__(), sys.getsizeof(self))
    
    def clear(self):
        self._kv.clear()
        self._kv = {}
        self._vk.clear()
        self._vk = {}
    
    def iter_key_value(self):
        return iter(self._kv.items())
    
    def iter_value_key(self):
        return iter(self._vk.items())
    
    def zip(self):
        return zip(self._kv, self._vk)
    
    def has_key(self, key):
        return key in self._kv
    
    def has_value(self, val):
        return val in self._vk
    
    def __contains__(self, obj):
        if obj in self._kv:
            return True
        if obj in self._vk:
            return True
        return False
    
    def get_value(self, key):
        return self._kv[key]
    
    def get_key(self, val):
        return self._vk[val]
    
    def __getitem__(self, obj):
        try:
            return self._kv[obj]
        except KeyError:
            try:
                return self._vk[obj]
            except KeyError:
                raise ValueError(obj)
    
    def __setitem__(self, key, val):
        self._kv[key] = val
        self._vk[val] = key
        assert len(self._kv) == len(self._vk)
    
    def update(self, key, val):
        self.__setitem__(key, val)
        
    def from_iter(self, iterable, clear=True):
        if clear:
            self.clear()
        for k, v in iterable:
            self.__setitem__(k, v)


class BisectList(list):
    """ bisect list(always sorted) impl, note this class is low performance """
    __slots__ = ()
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def make_set(self)->set:
        return set(self)
    
    def make_dict(self, aggregate = False)->dict:
        if not aggregate:
            return dict(enumerate(self))
        grouper = {}
        for index, val in enumerate(self):
            if val not in grouper:
                grouper[val] = array.array('L')
            grouper[val].append(index)
        return grouper
    
    def binary_search(self, x)->int:
        lo = 0
        hi = self.__len__()
        while lo < hi:
            mid = (lo + hi) >> 1
            if self[mid] < x:
                lo = mid + 1
            else:
                if self[mid] == x:
                    return mid
                hi = mid
            return -1

    def bisect(self, x, lo = 0, hi = None, right = False)->int:
        if lo < 0:
            raise ValueError('lo must be non-negative')
        if hi is None:
            hi = self.__len__()
        if right:
            while lo < hi:
                mid = (lo + hi) >> 1
                if self[mid] > x: 
                    hi = mid
                else: 
                    lo = mid + 1
        else:
            while lo < hi:
                mid = (lo + hi) >> 1
                if self[mid] < x: 
                    lo = mid + 1
                else: 
                    hi = mid
        return lo
    
    def insort(self, x, lo = 0, hi = None, right = False)->int:
        pos = self.bisect(x, lo, hi, right)
        self.insert(pos, x)
        return pos


class CounterType(defaultdict):
    """ a counter type for statistics and remove duplicates """
    def __init__(self, valuemaker=lambda:0):
        super().__init__(valuemaker)
    
    def sorted(self, reverse=True)->tuple:
        for k in sorted(self, key=lambda x:self[x], reverse=reverse):
            yield (k, self[k])
    
    def sorted_formated(self, formats='%d, %s', reverse=True)->str:
        for k, v in self.sorted(reverse):
            yield formats % (v, k)
    
    def __str__(self, *args, **kwargs):
        return super().__str__()


def make_counter(init=0):
    """ get defaultdict as a int counter """
    return defaultdict(lambda:init)

def make_nested_defaultdict2(init_func=lambda:0):
    """ make nested defaultdict, d['k1']['k2'] = something """
    return defaultdict(lambda:defaultdict(init_func))

def make_nested_counter2(init=0):
    """ make nested counter, c['k1']['k2'] += 1 """
    return make_nested_defaultdict2(lambda:init)

def make_nested_defaultdict3(init_func=lambda:0):
    """ make nested defaultdict, d['k1']['k2']['k3'] = something """
    return defaultdict(lambda:defaultdict(lambda:defaultdict(init_func)))

def make_nested_counter3(init=0):
    """ make nested counter, c['k1']['k2']['k3'] += 1 """
    return make_nested_defaultdict3(lambda:init)

def make_nested_defaultdict4(init_func=lambda:0):
    """ make nested defaultdict, d['k1']['k2']['k3']['k4'] = something """
    return defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:defaultdict(init_func))))

def make_nested_counter4(init=0):
    """ make nested counter, c['k1']['k2']['k3']['k4'] += 1 """
    return make_nested_defaultdict4(lambda:init)

def defaultdict_to_dict(dd):
    """ transform defaultdict or nested defaultdict into dict """
    for i in dd:
        di = dd[i]
        if isinstance(di, defaultdict):
            dd[i] = dict(di)
            for j in di:
                dj = di[j]
                if isinstance(dj, defaultdict):
                    dd[i][j] = dict(dj)
                    for k in dj:
                        dk = dj[k]
                        if isinstance(dk, defaultdict):
                            dd[i][j][k] = dict(dk)
                            for m in dk:
                                if isinstance(dk[m], defaultdict):
                                    raise TypeError('depth of nested defaultdict is ')
    return dict(dd)

def most_common(d:dict, topn=10)->list:
    """ List the n most common elements and their counts from the most
        common to the least.  If n is None, then list all element counts """
    if topn is None or topn == 0:
        return sorted(d.items(), key=itemgetter(1), reverse=True)
    return heapq.nlargest(topn, d.items(), key=itemgetter(1))


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
    
    def test_BiMap(self):
        d = [('name1', 'Mike'), ('name2', 'Jerry'), ('tag', 2.4), ('base', (1,2,3))]
        bt = BiMap()
        bt.from_iter(d)
        self.assertEqual(bt.get_value('name1'), 'Mike')
        self.assertEqual(bt.get_key(2.4), 'tag')
        bt['name3'] = 'Ops'
        self.assertEqual(bt['name3'], 'Ops')
        bt.update('bingo', 'Cherry')
        bt.update('Cherry', 'bingo')
        try:
            bt.update('tag', 'bingo')
        except ValueError:
            pass
        self.assertIsNotNone(bt.has_key('base'))
        self.assertIsNotNone(bt.has_value((1,2,3)))
        self.assertTrue('name1' in bt)
        self.assertTrue('name4' not in bt)
    
    def test_BisectList(self):
        sl = BisectList()
        for i in range(100000):
            sl.append(random.randint(1,100000))
        sl.sort()
        for i in range(100000, 200000):
            assert sl.binary_search(i) == -1
        for i in range(10000):
            sl.insort(i)
            sl.insort(i, right=True)
        tmp = sl[:]
        sl.sort()
        for i in range(100000):
            self.assertEqual(tmp[i], sl[i])
        s = sl.make_set()
        d = sl.make_dict(False)
        self.assertTrue(len(s) <= len(d))
        d = sl.make_dict(True)
        self.assertTrue(len(s) == len(d))
        self.assertTrue(len(d) <= len(sl))
        
    def test_dict(self):
        d4 = make_nested_counter4(0)
        d4['go1']['in']['key0']['name1'] += 100
        d4['go2']['in']['key2']['name2'] += 100
        d4['go2']['in']['key2']['name2'] += 100
        d4['go5']['in']['key5']['name4'] += 100
        d4 = defaultdict_to_dict(d4)
        self.assertTrue(isinstance(d4, dict))
        self.assertTrue(isinstance(d4['go1'], dict))
        self.assertTrue(isinstance(d4['go1']['in']['key0'], dict))
        self.assertTrue(d4['go2']['in']['key2']['name2'] == 200)
        d = make_counter(10)
        d[1] += 1
        d[2] += 2
        d[3] += 3
        d[4] += 4
        self.assertTrue(most_common(d, 2) == [(4, 14), (3, 13)])


if __name__ == '__main__':
    unittest.main()


