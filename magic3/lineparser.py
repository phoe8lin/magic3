#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# author : cypro666
# date   : 2015.06.06
import os
import re
from _io import open, DEFAULT_BUFFER_SIZE
from abc import abstractmethod, ABCMeta
from magic3.utils import isotime
from magic3.filesystem import user_dir,list_dir
from magic3.awkaux import read_from_awk

bestIOBufferSize = DEFAULT_BUFFER_SIZE << 2

class LineParserBase(metaclass=ABCMeta):
    """ inherit this class and implement `run` and `parse_line` method """
    def __init__(self, filenames=[], filedir='', namefilter='.*'):
        self._files = filenames if filenames else []
        self._dir = filedir
        self._filter = re.compile(namefilter)
        self.check_args()
    
    def check_args(self):
        if self._dir and not isinstance(self._dir, str):
            raise TypeError
        if self._dir and not self._dir.startswith(user_dir()):
            self._dir = user_dir() + self._dir.lstrip(os.sep)
        if self._files and not isinstance(self._files, (list, tuple, set)):
            raise TypeError
        if not self._dir and not self._files:
            raise ValueError
        if self._dir:
            tmp = [fn for fn in list_dir(self._dir, lambda s:self._filter.match(s))]
        else:
            tmp = []
        tmp.extend(fn for fn in self._files if self._filter.match(fn))
        self._files = tuple(tmp)
        return self._files

    def read(self, fn, mode='rb', encoding='utf-8-sig', errors='replace'):
        bufsize = bestIOBufferSize
        parser_ = self.parse_line
        if 'b' in mode:
            for line in open(fn, mode, buffering=bufsize):
                parser_(line.rstrip())
        else:
            for line in open(fn, mode, buffering=bufsize, encoding=encoding, errors=errors):
                parser_(line.rstrip())
    
    def read_all(self, mode='rb', encoding='utf-8-sig'):
        for each in self._files:
            if __debug__:
                print(isotime(), each)
            self.read(each, mode, encoding)
    
    @abstractmethod
    def parse_line(self, line):
        raise NotImplementedError('inherit this method in subclasses!')
    
    def run(self):
        self.read_all()


class AWKLineParserBase(LineParserBase):
    """ like LineParserBase, this class using awk subprocess for formatted text file, such as log file
        tip: str.split of python always very slow, if there're many fields each line, using this solution
    """
    def __init__(self, filenames=[], filedir='', namefilter='.*'):
        super().__init__(filenames=filenames, filedir=filedir, namefilter=namefilter)
        self._fields = None
        self._delim = None

    def read(self, fn):
        delimb = bytes(self._delim, 'utf-8')
        parser_ = self.parse_line
        for line in read_from_awk([fn], self._fields, self._delim):
            parser_(line.rstrip().split(delimb))

    @abstractmethod
    def parse_line(self, seps:list):
        raise NotImplementedError('inherit this method in subclasses!')

    def read_all(self):
        for each in self._files:
            if __debug__:
                print(isotime(), each)
            self.read(each)

    def run(self, fields:list, delim:str=' '):
        self._fields = tuple(fields)
        self._delim = delim
        self.read_all()


def test():
    class Parser(LineParserBase):
        def __init__(self, name):
            super().__init__(filenames=[name])
            self.count = 0
        def parse_line(self, line):
            words = [s for s in line.split() if len(s) >= 1]
            self.count += len(words)
        
    class AWKParser(AWKLineParserBase):
        def __init__(self, name):
            super().__init__(filenames=[name])
            self.count = 0
        def parse_line(self, seps):
            words = [s for s in seps if len(s) >= 1]
            self.count += len(words)

    p1 = Parser(__file__)
    p1.run()
    print(p1.count)
    p2 = AWKParser(__file__)
    p2.run([3,4,5,6])
    print(p2.count)


if __name__ == '__main__':
    test()


