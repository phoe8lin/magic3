#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# author : cypro666
# date   : 2015.06.06
import os, re, math
from _io import open, DEFAULT_BUFFER_SIZE, BytesIO
from abc import abstractmethod, ABCMeta
from os.path import exists
from string import Template
from magic3.utils import debug
from magic3.filesystem import user_dir, list_dir, PathSpliter
from magic3.system import OSCommand

AWK_CMD = Template('''awk -F "${delim}" '{print ${vargs}}' ${files} 2>&1''')

bestIOBufferSize = DEFAULT_BUFFER_SIZE << 2

TWO_GB = (1024 * 1024 * 1024 * 2)


def open_awk(filelist:list, pos_args:list, delim):
    ''' Call awk command and return an opened pipe for read the output of awk, eg:
        for line in open_awk([file1, file2], [2,3,4], ',').stdout:
            print(line)   
    '''
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

    cmd = AWK_CMD.safe_substitute(delim=delim,
                                  vargs=','.join(map(lambda x:'$' + str(x), pos_args)),
                                  files=' '.join(filelist))
    if __debug__:
        print(cmd)

    return OSCommand.popen(cmd)


def read_from_awk(filelist:list, pos_args:list, delim=' ') -> iter:
    ''' Call awk command and return an iterator for reading the output of awk '''
    reader = open_awk(filelist, pos_args, delim).stdout
    if reader.readable() and not reader.closed:
        return iter(reader)
    else:
        raise RuntimeError('open_awk failed')

def read_from_awk_with_callback(callback, filelist:list, pos_args:list, delim=' ') -> iter:
    ''' Call awk command and return an iterator of callback's return for each line in output '''
    reader = open_awk(filelist, pos_args, delim).stdout
    if reader.readable() and not reader.closed:
        return map(callback, reader)
    else:
        raise RuntimeError('open_awk failed')


def open_as_bytes_stream(filename):
    ''' If filesize < TWO_GB, read whole file as BytesIO object '''
    filesize = os.path.getsize(filename)
    if filesize < TWO_GB:
        with open(filename, 'rb') as f:
            return BytesIO(f.read(filesize))
    else:
        return open(filename, 'rb', buffering=bestIOBufferSize)


class LineParserBase(metaclass=ABCMeta):
    ''' Inherit this class and implement `run` and `parse_line` method '''

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
                debug(each)
            self.read(each, mode, encoding)

    @abstractmethod
    def parse_line(self, line):
        raise NotImplementedError('inherit this method in subclasses!')

    def run(self):
        self.read_all()


class AWKLineParserBase(LineParserBase):
    ''' Like LineParserBase, this class using awk subprocess for formatted text file, such as log file
        tip: str.split of python always very slow, if there're many fields each line, using this solution
    '''

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
                debug(each)
            self.read(each)

    def run(self, fields:list, delim:str=' '):
        self._fields = tuple(fields)
        self._delim = delim
        self.read_all()


def file_count(textfile:str) -> tuple:
    ''' Count lines, words, bytes in text file '''
    err, ret = OSCommand.call('wc ' + textfile, False)
    if err:
        raise RuntimeError(os.strerror(err))
    if ret:
        nline, nword, nbyte, _ = ret.split()
    return int(nline), int(nword), int(nbyte)


class FileSpliter(object):
    ''' Split text file in different ways '''

    def __init__(self, filename):
        '''  '''
        assert exists(filename)
        self._fname = filename
        self._nbyte = 0
        self._nword = 0
        self._nline = 0
        ps = PathSpliter(self._fname)
        self._basename = ps.basename
        self._dirname = ps.dirname
        self.count()

    def count(self):
        ''' Same as file_count '''
        if self._nline:
            return self._nline, self._nword, self._nbyte
        else:
            self._nline, self._nword, self._nbyte = file_count(self._fname)
        return self._nline, self._nword, self._nbyte

    def splited_names(self, nfile):
        ''' Get result file names '''
        if '.' in self._basename:
            ext = '.' + self._basename.split('.')[-1]
            newname = self._dirname + self._basename.replace(ext, '-%s' + ext)
        else:
            newname = self._dirname + '-%s'
        return [newname % (i + 1) for i in range(nfile)]

    def __howmany(self, m, n):
        ''' A little magic to ensure how many file created '''
        if n >= m:
            return 2
        else:
            n = (m + 1) // n
        if not (m % n):
            return n
        else:
            return n + 1

    def split_by_lines(self, nline:int):
        ''' Split by every `nline` lines '''
        nfile = self.__howmany(self._nline, nline)
        newnames = self.splited_names(nfile)
        inames = iter(newnames)
        fout = open(next(inames), 'wb')
        n = 1

        with open_as_bytes_stream(self._fname) as stream:
            for line in stream:
                if not (n % nline):
                    fout.close()
                    fout = open(next(inames), 'wb')
                n += 1
                fout.write(line)
            fout.close()

        return newnames

    def split_by_size(self, nbyte:int):
        ''' Split by every `nbyte` bytes '''
        nfile = self.__howmany(self._nbyte, nbyte)
        newnames = self.splited_names(nfile)
        inames = iter(newnames)

        with open_as_bytes_stream(self._fname) as stream:
            buf = stream.read(nbyte)
            while buf:
                fout = open(next(inames), 'wb')
                fout.write(buf)
                fout.close()
                buf = stream.read(nbyte)

        return newnames

    def splitN(self, num:int):
        ''' Split into `num` files '''
        nfile = num
        nbyte = int(math.ceil((self._nbyte + 1.0) / nfile))
        return self.split_by_size(nbyte)

    def split_by_words(self, nword:int):
        ''' implemnet in futrue ... '''
        raise NotImplementedError


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
    p2.run([3, 4, 5, 6])
    print(p2.count)

    d = {}
    for line in read_from_awk([__file__, __file__, __file__], [2, 3]):
        for s in line.split():
            try: d[s] += 1
            except: d[s] = 1

    for k, v in d.items():
        assert v >= 3

    fspliter = FileSpliter(__file__)
    ret = fspliter.split_by_size(1000)

    print(ret)

    for fn in ret:
        os.system('unlink ' + fn)

    print('test OK')


if __name__ == '__main__':
    test()


