# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import sys
import random
import array 
from collections import defaultdict

class BiMap(object):
    """ like bimap, can search v from k and search k from v,
        ensure k and v must be unique, using this as dict
    """
    __slots__ = ('_kv', '_vk', '_hasher')
    def __init__(self, keyhasher = None):
        super().__init__()
        self._kv = {}
        self._vk = {}
        self._hasher = keyhasher
    
    def __str__(self):
        return '%s\n%s' % (self._kv, self._vk)

    def __len__(self):
        assert len(self._kv) == len(self._vk)
        return len(self._kv)
    
    def size(self):
        return (self.__len__(), sys.getsizeof(self))
    
    def clear(self):
        self._kv.clear()
        del self._kv
        self._kv = {}
        self._vk.clear()
        del self._vk
        self._vk = {}
    
    def iterkv(self):
        return iter(self._kv.items())
    
    def itervk(self):
        return iter(self._vk.items())
    
    def zip(self):
        return zip(self._kv, self._vk)

    def haskey(self, key):
        if self._hasher:
            return self._hasher(key) in self._kv
        return key in self._kv
    
    def hasval(self, val):
        return val in self._vk
    
    def __contains__(self, obj):
        if self._hasher:
            if self._hasher(obj) in self._kv:
                return True
        elif obj in self._kv:
            return True
        if obj in self._vk:
            return True
        return False
    
    def getval(self, key):
        if self._hasher:
            return self._kv.get(self._hasher(key))
        return self._kv[key]
    
    def getkey(self, val):
        return self._vk[val]
    
    def __getitem__(self, obj):
        try:
            if self._hasher:
                return self._kv[self._hasher(obj)]
            return self._kv[obj]
        except KeyError:
            try:
                return self._vk[obj]
            except KeyError:
                raise ValueError(obj)
    
    def __setitem__(self, key, val):
        if self._hasher:
            key = self._hasher(key)
        if (val in self._vk) and (key != self._vk[val]):
            raise ValueError('BiMap : duplicate key or val {%s:%s}' % (key, val))
        if key in self._kv:
            self._vk.pop(self._kv[key])
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
    
    def makeset(self)->set:
        return set(self)
    
    def makedict(self, aggregate = False)->dict:
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
        return str(dict(self))


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
        bt = BiMap(keyhasher = None)
        bt.from_iter(d)
        self.assertEqual(bt.getval('name1'), 'Mike')
        self.assertEqual(bt.getkey(2.4), 'tag')
        bt['name2'] = 'Ops'
        self.assertEqual(bt['name2'], 'Ops')
        bt.update('bingo', 'Cherry')
        bt.update('Cherry', 'bingo')
        try:
            bt.update('tag', 'bingo')
        except ValueError:
            pass
        self.assertIsNotNone(bt.haskey('base'))
        self.assertIsNotNone(bt.hasval((1,2,3)))
        self.assertTrue('name1' in bt)
        self.assertTrue('name3' not in bt)
        for i in range(1000000):
            bt[i] = 1000000 + (i + 1)
        dd = {}
        for i in range(1000000):
            dd[i] = 1000000 + (i + 1)
        for i in range(1000000):
            dd[i] = 1000000 + (i + 1)
        self.assertEqual(bt.size(), (1000006, 64))
    
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
        s = sl.makeset()
        d = sl.makedict(False)
        self.assertTrue(len(s) <= len(d))
        d = sl.makedict(True)
        self.assertTrue(len(s) == len(d))
        self.assertTrue(len(d) <= len(sl))


if __name__ == '__main__':
    unittest.main()


