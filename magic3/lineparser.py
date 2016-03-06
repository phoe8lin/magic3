#!/usr/bin/env python3
# -*- coding:utf-8 -*-
## author : cypro666
## date   : 2015.06.06
import os
import re
from abc import abstractmethod, ABCMeta
from _io import open, DEFAULT_BUFFER_SIZE
from magic3.filesystem import userDir,listDir

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

    def read(self, fn, mode='rb', encoding='utf-8', errors='replace'):
        bufsize = bestIOBufferSize
        if 'b' in mode:
            for line in open(fn, mode, buffering=bufsize):
                self.parseLine(line.rstrip())
        else:
            for line in open(fn, mode, buffering=bufsize, encoding=encoding, errors=errors):
                self.parseLine(line.rstrip())
    
    def readAll(self, mode='rb'):
        for fn in self._files:
            self.read(fn, mode)
    
    @abstractmethod
    def parseLine(self, line):
        raise NotImplementedError
    
    def run(self):
        self.readAll()


def test():
    class Parser(LineParserBase):
        def __init__(self, name):
            super().__init__(fileNames=[name])
            self.count = 0
        def parseLine(self, line):
            words = [s for s in line.split() if len(s) >= 1]
            self.count += len(words)
    p = Parser(__file__)
    p.run()
    print(p.count)

if __name__ == '__main__':
    test()


