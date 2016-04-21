# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import sys, os, time, re
from time import localtime
from pathlib import Path
from platform import platform
from magic3.Utils import Debug

def UserDir():
    """ return current user's dir with os.sep """
    return os.path.expanduser('~') + os.sep

def HomeDir():
    """ return home dir of current user with os.sep """
    return str(Path.home()) + os.sep

def CurDir():
    """ return current dir of fn """
    return str(Path(os.path.curdir).absolute())

def ParentDir(fn):
    """ return parent directory """
    return str(Path(fn).parent.absolute()) + os.sep

def CMAFileTime(fn):
    """ get file time : ctime, mtime, atime """
    assert os.path.exists(fn)
    fstat = os.lstat(fn)
    ct, mt, at = fstat.st_ctime, fstat.st_mtime, fstat.st_atime
    return (localtime(ct), localtime(mt), localtime(at))

def SpanWildcard(path:str, wildcard:str)->list:
    """ span the wildcard in path """
    print(os.path.realpath(path))
    return [str(p) for p in Path(os.path.realpath(path)).glob(wildcard)]
    
def SpanWildcardRecurse(path:str, wildcard:str)->list:
    """ span the wildcard in path recursive """
    print(os.path.realpath(path))
    return [str(p) for p in Path(os.path.realpath(path)).rglob(wildcard)]


class PathSpliter(object):
    """ split standard path stirng """
    def __init__(self, name):
        self._fname = name
        self._parts = tuple(os.path.realpath(name).split(os.sep))
    
    def __str__(self)->str:
        return self._fname
    
    @property
    def exist(self)->bool:
        """ check exist or not """
        return os.path.exists(self._fname)
    
    @property
    def basename(self)->str:
        """ get basename of the filename """
        return os.path.basename(self._fname)
    
    def UpDir(self, n=1)->str:
        """ return a list of parent dirs """
        assert n >= 0
        if not n:
            return self._parts[-1]
        if abs(n) == len(self._parts) - 1:
            root = self._parts[0]
            return root if root else os.sep 
        return self._parts[-(n+1)]
    
    def __getitem__(self, n:int)->str:
        """ [] operator """
        assert n >= 0
        if not n:
            return os.sep.join(self._parts)
        if abs(n) == len(self._parts) - 1:
            root = self._parts[0]
            return root if root else os.sep 
        return os.sep.join(self._parts[:-n])


class PathWalker(object):
    """ walking in path and call user's function with file names in path """
    def __init__(self, path, predicate):
        assert path and predicate
        if not os.path.exists(path):
            raise ValueError('No such dir : ' + path)
        self._path = path
        self._pred = predicate

    def Walk(self):
        """ walking the path recursive """
        result = []
        for root, dirs, names in os.walk(self._path):
            if root[-1] != os.sep:
                root += os.sep
            for f in names:
                if self._pred(root + f):
                    result.append(root + f)
            for d in dirs:
                if d[-1] != os.sep:
                    d += os.sep
                if self._pred(root + d):
                    result.append(root + d)
        return result

def IsValidDir(*args) ->bool:
    """ check is valid dir """
    for d in args:
        assert isinstance(d, str)
        if not os.path.isdir(d):
            return False
        if d[0] in ('.', '~'):
            return False
    if 'linux' in platform().lower():
        return d.startswith('/')
    return True

def ListDir(path, pred=lambda x:True)->list:
    """ get all file names in `path` recursive """
    return PathWalker(path, pred).Walk()

def ListMatched(path, sre):
    """ list all filenames in `path` which matched `sre` recursive """
    x = re.compile(sre)
    return [fn for fn in ListDir(path) if x.match(fn)]

def ScanDir(path):
    """ like ListDir, but without recursive!!! """
    if sys.version_info.minor >= 5:
        return tuple(os.scandir(path))
    else:
        return os.listdir(path)


def test(path):
    print('user dir:', UserDir())
    print('cwd dir:', CurDir())
    print('parent dir:', ParentDir(CurDir()))
    print('test PathWalker:')
    ps = PathSpliter('/home/user/temp/domains.txt')
    print(str(ps))
    print(ps.UpDir(0))
    print(ps.UpDir(1))
    print(ps.UpDir(2))
    print(ps.UpDir(3))
    print(ps.exist)
    print(ps[0])
    print(ps[1])
    print(ps[2])
    print(ps[3])
    print('\ntest file time stat:')
    c, m ,a = CMAFileTime(path)
    print(c.tm_year, c.tm_mon, c.tm_mday)
    print(m.tm_year, m.tm_mon, m.tm_mday)
    print(a.tm_year, a.tm_mon, a.tm_mday)
    print('\ntest ListDir with lambda:')
    cb = lambda fn : fn.endswith('.py')
    fs = ListDir(CurDir(), cb)
    print(fs)
    print('\ntest ScanDir:')
    print(ScanDir(path))
    print('test OK')


if __name__ == '__main__':
    test(UserDir() + 'tmp')

