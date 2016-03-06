#!/usr/bin/env python3
# -*- coding:utf-8 -*-
## author : cypro666
## date   : 2015.06.06
try:
    import numpy
except Exception:
    raise ImportError('warning: numpy can Not be loaded!\n')


def readCSV(name:str, delim=',', 
            withhead=False, strip=True, convert=None, 
            encoding='utf-8', errors='strict')->([],[]):
    """ read csv file, return head and body as list """
    if convert and strip:
        make = lambda s: convert(s.strip(b'" ').decode(encoding, errors))
    elif convert:
        make = lambda s: convert(s.decode(encoding, errors))
    elif strip:
        make = lambda s: s.strip(b'" ').decode(encoding, errors)
    else:
        make = lambda s: s.decode(encoding, errors)
    head, body = [], []
    delim = delim.encode()
    if isinstance(name, str):
        name = open(name, 'rb')
    if withhead:
        line = next(name).rstrip()
        head = [i.decode() for i in line.rstrip(b',').split(delim)]
    for line in name:
        body.append([make(i) for i in line.rstrip(b'"\r\n, ').split(delim)])
    return head, body


def writeCSV(name:str, delim:str, body:list, head:list, 
             bodyFormat=None, headFormat=None)->int:
    """ write data to csv file, note body should be a bivariate table """
    if not headFormat:
        headFormat = (delim).join(['%s']*len(head)) + '\n'
    elif not headFormat.endswith('\n'):
        headFormat += '\n'
    if not bodyFormat:
        try:
            bodyFormat = delim.join(['%s']*len(body[0])) + '\n'
        except TypeError:
            try:
                bodyFormat = delim.join(['%s']*numpy.shape(body)[1]) + '\n'
            except IndexError:
                bodyFormat = delim.join(['%s']*numpy.shape(body)[0]) + '\n'
    elif not bodyFormat.endswith('\n'):
        bodyFormat += '\n'
    with open(name, 'w') as fout:
        nlines = 0
        if head:
            nlines += 1
            fout.write(headFormat % tuple(head))
        if len(numpy.shape(body)) == 1:
            for i in body:
                fout.write('%s\n' % i)
                nlines += 1
        else:
            for row in body:
                fout.write(bodyFormat % tuple(row))
                nlines += 1
    return nlines


class BivTableBase(object):
    """ Base class of bivariate table """
    def __init__(self, head=[], body=[]):
        self.check(head, body)
        self.head = head
        self.body = body

    @property
    def header(self)->list:
        return self.head
    
    @property
    def table(self)->list:
        return self.body
    
    def read(self, *args, **kwargs):
        raise NotImplementedError
    
    def write(self, *args, **kwargs):
        raise NotImplementedError
    
    def check(self, *args):
        raise NotImplementedError
    
    def show(self):
        raise NotImplementedError
    
    def __iter__(self):
        """ body iterator """
        return iter(self.body)

    def reset(self):
        """ clear and reset all """
        self.head.clear()
        del self.head
        self.body.clear()
        del self.body
        self.head, self.body = [], []

    def matrix(self, deleteRaw=False)->numpy.mat:
        """ return a numpy matrix object """
        mat = numpy.mat(self.body)
        if deleteRaw:
            self.reset()
        return mat

    def array(self, deleteRaw=False)->numpy.array:
        """ return a numpy ndarray object """
        narr = numpy.array(self.body)
        if deleteRaw:
            self.reset()
        return narr


class BivTable(BivTableBase):
    """ Usage: 
        table = BivTable()
        table.read('/home/user/mydata.csv', withhead=1, convert=int)
        table.show()
        mat = table.matrix() """
    def __init__(self, head=[], body=[]):
        super().__init__(head, body)

    def read(self, name, delim=',', withhead=False, strip=False, convert=None, encoding='utf-8'):
        """ read from file, `convert` should be a function like int/str/float/lambda """
        self.head, self.body = readCSV(name, delim, withhead, strip, convert, encoding)
        self.check(self.head, self.body)

    def check(self, head, body):
        """ check types """
        if not isinstance(head, (list, tuple)):
            raise TypeError
        if not isinstance(body, (list, tuple, set, numpy.matrix, numpy.ndarray)):
            raise TypeError

    def write(self, name, delim=',', bodyFormat=None, headFormat=None):
        """ write data to csv file """
        return writeCSV(name, delim, self.body, self.head, bodyFormat, headFormat)
    
    @property
    def shape(self)->tuple:
        """ number of rows and cols """
        if self.head:
            assert len(self.head) == len(self.body[0])
        return len(self.body), len(self.body[0])
    
    def show(self):
        """ print whole table """
        print(', '.join(self.head))
        for row in self.body:
            print(str(row)[1:-1])


class CSV(BivTable):
    """ using as a 'CSV'-like object """
    def __init__(self, *args):
        super().__init__(*args)
    
    def __getitem__(self, nrow):
        return self.body[nrow]


# Not-Applicable flag
NAFlag = 'N/A'

def indexed(vec, start, offset=1)->dict:
    """ indexed an array-like object(list,tuple,etc) by integer, auto increment """
    assert hasattr(vec, '__iter__')
    assert isinstance(vec, (tuple, list, numpy.ndarray))
    vec = list(set(vec))
    vec.sort()
    end = len(vec) * 2 + start
    return {k:v for k,v in zip(vec, range(start, end, offset))}

def indexedByColumn(table:CSV, dtype=float, start=1, NA=0)->numpy.mat:
    """[['a', 'b', 'c'],      [[1.  2.  3.]
        ['e', 'a', 'd'],  =>   [3.  1.  2.]
        ['f', 'f', 'b'],       [2.  2.  1.]
        ['a', 'c', 'd'],       [1.  2.  3.]]
    """
    nr, nc = table.shape
    mat = numpy.zeros(shape=(nr, nc), dtype=dtype)
    dtmp = [indexed(i, start=start) for i in table.T]
    for i in range(nr):
        row = table[i]
        for j in range(nc):
            mat[i, j] = dtmp[j][row[j]] if row[j] != NAFlag else NA
    return mat

def indexedByRow(table:CSV, dtype=float, start=1, NA=0)->numpy.mat:
    """[['a', 'b', 'b'],      [[1.  2.  2.]
        ['e', 'a', 'd'],  =>   [2.  1.  3.]
        ['f', 'f', 'b'],       [3.  4.  1.]
        ['a', 'c', 'd'],       [1.  3.  3.]]
    """
    nr, nc = table.shape
    mat = numpy.zeros(shape=(nr, nc), dtype=dtype)
    dtmp = [indexed(i, start=start) for i in table]
    for i in range(nr):
        row = table[i]
        for j in range(nc):
            mat[i, j] = dtmp[i][row[j]] if row[j] != NAFlag else NA
    return mat

def indexedAll(table:CSV, dtype=float, start=1, NA=0)->numpy.mat:
    d = {}
    n = start
    for row in table:
        for k in row:
            if k not in d:
                d[k] = n
                n += 1
    nr, nc = table.shape
    mat = numpy.zeros(shape=(nr, nc), dtype=dtype)
    for i in range(nr):
        row = table[i]
        for j in range(nc):
            mat[i, j] = d[row[j]] if row[j] != NAFlag else NA
    return mat

def quantize(arr, mapper, unique=True)->dict:
    """  """
    if unique:
        arr = list(set(arr))
        arr.sort()
    return {k:mapper(k) for k in arr}


def test():
    from io import BytesIO
    s = b"""a,b,c,d,e,f,g,h,j,k 
            1,2,3,4,5,6,7,8,9,0 
            0,9,8,7,6,5,4,3,2,1 
            1,2,3,4,5,6,7,8,9,0 
            0,9,8,7,6,5,4,3,2,1 
            1,2,3,4,5,6,7,8,9,0 
            6,6,6,6,6,6,6,6,6,6"""
    csv = CSV()
    csv.read(BytesIO(s), withhead=1, convert=int)
    csv.show()
    print()
    csv.read(BytesIO(s), withhead=1, convert=float)
    try:
        print(csv.matrix())
    except:
        pass
    csv.write('csv.csv', delim=',', bodyFormat=','.join(['%.2f']*10))
    m = numpy.array([['a','b','c'],
                     ['b','c','d'],
                     ['d','e','f'],
                     ['a','f','c'],
                     ['g','d','e']])
    print()
    print(indexedByRow(m))
    print()
    print(indexedByColumn(m))
    print()
    print(indexedAll(m))
    print()


if __name__ == '__main__':
    test()
