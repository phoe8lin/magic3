#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# author : cypro666
# date   : 2015.06.06
import os
import re
from os.path import exists
from _io import open, DEFAULT_BUFFER_SIZE, BytesIO
from abc import abstractmethod, ABCMeta
from string import Template
from magic3.Utils import Debug
from magic3.FileSystem import UserDir,ListDir
from magic3.System import OSCommand

AWK_CMD = Template("""awk -F "${delim}" '{print ${vargs}}' ${files} 2>&1""")
bestIOBufferSize = DEFAULT_BUFFER_SIZE << 2
TWO_GB = (1024*1024*1024*2)

def OpenAWK(filelist:list, pos_args:list, delim):
    """ call awk command and return an opened pipe for read the output of awk, eg:
        for line in OpenAWK([file1, file2], [2,3,4], ',').stdout:
            print(line)   
    """
    if isinstance(delim, (bytes, bytearray)):
        delim = str(delim, 'utf-8')
    if not filelist:
        raise ValueError('filelist')
    if not pos_args:
        raise ValueError('pos_args')
    if len(delim) != 1:
        raise ValueError('delim must be a length 1 char')
    for fn in filelist:
        if not exists(fn):
            raise FileNotFoundError(fn)
    for i in pos_args:
        if not isinstance(i, (int, str)):
            raise TypeError(pos_args)
        if isinstance(i, str) and not i.isdigit():
            raise TypeError(pos_args)
    cmd = AWK_CMD.safe_substitute(delim = delim,
                                  vargs = ','.join(map(lambda x:'$' + str(x), pos_args)),
                                  files = ' '.join(filelist))
    if __debug__:
        print(cmd)
    return OSCommand.popen(cmd)

def ReadFromAWK(filelist:list, pos_args:list, delim=' ')->iter:
    """ call awk command and return an iterator for reading the output of awk """
    reader = OpenAWK(filelist, pos_args, delim).stdout
    if reader.readable() and not reader.closed:
        return iter(reader)
    else:
        raise RuntimeError('OpenAWK failed')

def ReadFromAWKWithCallback(callback, filelist:list, pos_args:list, delim=' ')->iter:
    """ call awk command and return an iterator of callback's return for each line in output """
    reader = OpenAWK(filelist, pos_args, delim).stdout
    if reader.readable() and not reader.closed:
        return map(callback, reader)
    else:
        raise RuntimeError('OpenAWK failed')

def OpenAsByteStream(filename):
    """ if filesize < TWO_GB, read whole file as BytesIO object """
    filesize = os.path.getsize(filename)
    if filesize < TWO_GB:
        with open(filename, 'rb') as f:
            return BytesIO(f.read(filesize))
    else:
        return open(filename, 'rb', buffering=bestIOBufferSize)


class LineParserBase(metaclass=ABCMeta):
    """ inherit this class and implement `Run` and `parse_line` method """
    def __init__(self, filenames=[], filedir='', namefilter='.*'):
        self._files = filenames if filenames else []
        self._dir = filedir
        self._filter = re.compile(namefilter)
        self.CheckArgs()
    
    def CheckArgs(self):
        if self._dir and not isinstance(self._dir, str):
            raise TypeError
        if self._dir and not self._dir.startswith(UserDir()):
            self._dir = UserDir() + self._dir.lstrip(os.sep)
        if self._files and not isinstance(self._files, (list, tuple, set)):
            raise TypeError
        if not self._dir and not self._files:
            raise ValueError
        if self._dir:
            tmp = [fn for fn in ListDir(self._dir, lambda s:self._filter.match(s))]
        else:
            tmp = []
        tmp.extend(fn for fn in self._files if self._filter.match(fn))
        self._files = tuple(tmp)
        return self._files

    def Read(self, fn, mode='rb', encoding='utf-8-sig', errors='replace'):
        bufsize = bestIOBufferSize
        parser_ = self.parse_line
        if 'b' in mode:
            for line in open(fn, mode, buffering=bufsize):
                parser_(line.rstrip())
        else:
            for line in open(fn, mode, buffering=bufsize, encoding=encoding, errors=errors):
                parser_(line.rstrip())
    
    def ReadAll(self, mode='rb', encoding='utf-8-sig'):
        for each in self._files:
            if __debug__:
                Debug(each)
            self.Read(each, mode, encoding)
    
    @abstractmethod
    def parse_line(self, line):
        raise NotImplementedError('inherit this method in subclasses!')
    
    def Run(self):
        self.ReadAll()


class AWKLineParserBase(LineParserBase):
    """ like LineParserBase, this class using awk subprocess for formatted text file, such as log file
        tip: str.split of python always very slow, if there're many fields each line, using this solution
    """
    def __init__(self, filenames=[], filedir='', namefilter='.*'):
        super().__init__(filenames=filenames, filedir=filedir, namefilter=namefilter)
        self._fields = None
        self._delim = None

    def Read(self, fn):
        delimb = bytes(self._delim, 'utf-8')
        parser_ = self.parse_line
        for line in ReadFromAWK([fn], self._fields, self._delim):
            parser_(line.rstrip().split(delimb))

    @abstractmethod
    def parse_line(self, seps:list):
        raise NotImplementedError('inherit this method in subclasses!')

    def ReadAll(self):
        for each in self._files:
            if __debug__:
                Debug(each)
            self.Read(each)

    def Run(self, fields:list, delim:str=' '):
        self._fields = tuple(fields)
        self._delim = delim
        self.ReadAll()


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
    p1.Run()
    print(p1.count)
    p2 = AWKParser(__file__)
    p2.Run([3,4,5,6])
    print(p2.count)

    d = {}
    for line in ReadFromAWK([__file__, __file__, __file__], [2, 3]):
        for s in line.split():
            try: d[s] += 1
            except: d[s] = 1
    for k, v in d.items():
        assert v >= 3
    print('test OK')


if __name__ == '__main__':
    test()


