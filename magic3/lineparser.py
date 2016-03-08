#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# author : cypro666
# date   : 2015.06.06
import os
import re
from _io import open, DEFAULT_BUFFER_SIZE
from abc import abstractmethod, ABCMeta
from magic3.utils import isotime
from magic3.filesystem import userDir,listDir
from magic3.awkaux import readFromAWK

bestIOBufferSize = DEFAULT_BUFFER_SIZE << 1

class LineParserBase(metaclass=ABCMeta):
    """ inherit this class and implement `run` and `parseLine` method """
    def __init__(self, fileNames=[], fileDir='', nameFilter='.*'):
        self._files = fileNames if fileNames else []
        self._dir = fileDir
        self._filter = re.compile(nameFilter)
        self.checkArgs()
    
    def checkArgs(self):
        if self._dir and not isinstance(self._dir, str):
            raise TypeError
        if self._dir and not self._dir.startswith(userDir()):
            self._dir = userDir() + self._dir.lstrip(os.sep)
        if self._files and not isinstance(self._files, (list, tuple, set)):
            raise TypeError
        if not self._dir and not self._files:
            raise ValueError
        if self._dir:
            tmp = [fn for fn in listDir(self._dir, lambda s:self._filter.match(s))]
        else:
            tmp = []
        tmp.extend(fn for fn in self._files if self._filter.match(fn))
        self._files = tuple(tmp)
        return self._files

    def read(self, fn, mode='rb', encoding='utf-8-sig', errors='replace'):
        bufsize = bestIOBufferSize
        doParse = self.parseLine
        if 'b' in mode:
            for line in open(fn, mode, buffering=bufsize):
                doParse(line.rstrip())
        else:
            for line in open(fn, mode, buffering=bufsize, encoding=encoding, errors=errors):
                doParse(line.rstrip())
    
    def readAll(self, mode='rb', encoding='utf-8-sig'):
        for each in self._files:
            if __debug__:
                print(isotime(), each)
            self.read(each, mode, encoding)
    
    @abstractmethod
    def parseLine(self, line):
        raise NotImplementedError('inherit this method in subclasses!')
    
    def run(self):
        self.readAll()


class AWKLineParserBase(LineParserBase):
    """ like LineParserBase, this class using awk subprocess for formatted text file, such as log file
        tip: str.split of python always very slow, if there're many fields each line, using this solution
    """
    def __init__(self, fileNames=[], fileDir='', nameFilter='.*'):
        super().__init__(fileNames=fileNames, fileDir=fileDir, nameFilter=nameFilter)
        self._fields = None
        self._delim = None

    def read(self, fn):
        delimb = bytes(self._delim, 'utf-8')
        doParse = self.parseLine
        for line in readFromAWK([fn], self._fields, self._delim):
            doParse(line.rstrip().split(delimb))

    @abstractmethod
    def parseLine(self, seps:list):
        raise NotImplementedError('inherit this method in subclasses!')

    def readAll(self):
        for each in self._files:
            if __debug__:
                print(isotime(), each)
            self.read(each)

    def run(self, fields:list, delim:str=' '):
        self._fields = tuple(fields)
        self._delim = delim
        self.readAll()


def test():
    class Parser(LineParserBase):
        def __init__(self, name):
            super().__init__(fileNames=[name])
            self.count = 0
        def parseLine(self, line):
            words = [s for s in line.split() if len(s) >= 1]
            self.count += len(words)
        
    class AWKParser(AWKLineParserBase):
        def __init__(self, name):
            super().__init__(fileNames=[name])
            self.count = 0
        def parseLine(self, seps):
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


