#!/usr/bin/env python3
# -*- coding:utf-8 -*-
## author : cypro666
## date   : 2015.06.06
import os
import re
from abc import abstractmethod, ABCMeta
from _io import open, DEFAULT_BUFFER_SIZE
from magic3.filesystem import userdir,list_dir

best_io_buffer_size = DEFAULT_BUFFER_SIZE << 1

class LineParserBase(metaclass=ABCMeta):
    """ inherit this class and implement `run` and `parseline` method """
    def __init__(self, fdir=None, files=None, xvalid='.*'):
        self._dir = fdir
        self._files = files if files else []
        self._valid = re.compile(xvalid)
        self.check_args()
    
    def check_args(self):
        if self._dir and not isinstance(self._dir, str):
            raise TypeError
        if self._dir and not self._dir.startswith(userdir()):
            self._dir = userdir() + self._dir.lstrip(os.sep)
        if self._files and not isinstance(self._files, (list, tuple, set)):
            raise TypeError
        if not self._dir and not self._files:
            raise ValueError
        if self._dir:
            toparse = [fn for fn in list_dir(self._dir, lambda s:self._valid.match(s))]
        else:
            toparse = []
        toparse.extend(fn for fn in self._files if self._valid.match(fn))
        self._files = toparse
        return self._files

    def read(self, fn, mode='rb', encoding='utf-8', errors='replace'):
        bufsize = best_io_buffer_size
        if 'b' in mode:
            for line in open(fn, mode, buffering=bufsize):
                self.parseline(line.rstrip())
        else:
            for line in open(fn, mode, buffering=bufsize, encoding=encoding, errors=errors):
                self.parseline(line.rstrip())
    
    def read_all(self, mode='rb'):
        for fn in self._files:
            self.read(fn, mode)
    
    @abstractmethod
    def parseline(self, line):
        raise NotImplementedError
    
    @abstractmethod
    def run(self):
        raise NotImplemented




