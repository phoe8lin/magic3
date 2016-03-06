# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import sys, os, threading, sched, time, re
from time import localtime
from platform import platform
from magic3.utils import debug

def userDir():
    ''' return current user's dir with os.sep '''
    return os.path.expanduser('~') + os.sep

def curDir(fn):
    ''' return current dir of fn '''
    return os.path.dirname(os.path.realpath(fn))

def parentDir(fn):
    ''' return parent directory '''
    return os.path.dirname(os.path.dirname(os.path.realpath(fn)))

def cmaFileTime(fn):
    ''' get file time : ctime, mtime, atime '''
    assert os.path.exists(fn)
    fstat = os.lstat(fn)
    ct, mt, at = fstat.st_ctime, fstat.st_mtime, fstat.st_atime
    return (localtime(ct), localtime(mt), localtime(at))

class PathSpliter(object):
    ''' split standard path stirng '''
    def __init__(self, name):
        self._fname = name
        self._parts = tuple(os.path.realpath(name).split(os.sep))
    
    def __str__(self)->str:
        return self._fname
    
    @property
    def exist(self)->bool:
        ''' check exist or not '''
        return os.path.exists(self._fname)
    
    @property
    def basename(self)->str:
        return os.path.basename(self._fname)
    
    def updir(self, n=1)->str:
        ''' return a list of parent dirs '''
        assert n >= 0
        if not n:
            return self._parts[-1]
        if abs(n) == len(self._parts) - 1:
            root = self._parts[0]
            return root if root else os.sep 
        return self._parts[-(n+1)]
    
    def __getitem__(self, n:int)->str:
        ''' [] operator '''
        assert n >= 0
        if not n:
            return os.sep.join(self._parts)
        if abs(n) == len(self._parts) - 1:
            root = self._parts[0]
            return root if root else os.sep 
        return os.sep.join(self._parts[:-n])


class PathWalker(threading.Thread):
    ''' walking in path and call user's function with file names in path '''
    def __init__(self, path, callback, delay = 0, times = 1):
        threading.Thread.__init__(self)
        assert path and callback and delay >= 0 and times >= 1
        if not os.path.exists(path):
            raise ValueError('PathWalker : no such dir : ' + path)
        self._path = path
        self._delay = delay
        self._times = times
        self._callback = callback
        self.daemon = True 

    def walk(self):
        ''' walking impl '''
        exceptions = []
        for root, dirs, filenames in os.walk(self._path):
            try:
                if root[-1] != os.sep:
                    root += os.sep
                for fn in filenames:
                    self._callback(root + fn)
                for d in dirs:
                    self._callback(root + d)
            except Exception as e:
                exceptions.append(e)
        return exceptions

    def run(self):
        ''' thread start '''
        sc = sched.scheduler(time.time, time.sleep)
        for i in range(self._times):
            sc.enter(self._delay * (i+1), 1, self.walk)
        sc.run()

def isValidDir(*args) ->bool:
    ''' check is valid dir '''
    for d in args:
        assert isinstance(d, str)
        if not os.path.isdir(d):
            return False
        if d[0] in ('.', '~'):
            return False
    if 'linux' in platform().lower():
        return d.startswith('/')
    return True

def listDir(path, usercb=lambda x:True, timeout=10)->list:
    ''' get all file names in `path` '''
    filenames = []
    def _callback(fn):
        if usercb(fn):
            filenames.append(fn)
    pw = PathWalker(path, _callback, 0, 1)
    pw.start()
    pw.join(timeout)
    return filenames

def listMatched(path, sre):
    ''' list all filenames in `path` which matched `sre` '''
    x = re.compile(sre)
    return list(fn for fn in listDir(path) if x.match(fn))

def scanDir(path):
    ''' like listDir, but without recursive!!! '''
    if sys.version_info.minor >= 5:
        return tuple(os.scandir(path))
    else:
        return os.listdir(path)

def test(path):
    print('user dir:', userDir())
    print('test PathWalker:')
    ps = PathSpliter('/home/user/temp/domains.txt')
    print(str(ps))
    print(ps.updir(0))
    print(ps.updir(1))
    print(ps.updir(2))
    print(ps.updir(3))
    print(ps.exist)
    print(ps[0])
    print(ps[1])
    print(ps[2])
    print(ps[3])
    print('\ntest file time stat:')
    c, m ,a = cmaFileTime(path)
    print(c.tm_year, c.tm_mon, c.tm_mday)
    print(m.tm_year, m.tm_mon, m.tm_mday)
    print(a.tm_year, a.tm_mon, a.tm_mday)
    print('\ntest listDir with lambda:')
    cb = lambda fn : print(fn, time.ctime(os.path.getctime(fn)))
    pw = PathWalker(os.getcwd(), cb, 2.0, 1)
    pw.start()
    pw.join(3)
    print('\ntest scanDir:')
    print(scanDir(path))


if __name__ == '__main__':
    test(userDir() + 'tmp')

