# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import os, sys, traceback, socket
import json, re, functools
from threading import Thread 
from base64 import b64encode, b64decode
from ipaddress import ip_address
from time import time, strftime, sleep

def isValidIP(ip)->bool:
    """ Returns true if the given string is a well-formed IP address(v4/v6) """
    def _fromStr(_ip):
        if not _ip or '\x00' in _ip:
            return False
        try:
            res = socket.getaddrinfo(_ip, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_NUMERICHOST)
            return bool(res)
        except Exception:
            return False
    def _fromInt(_ip):
        try: 
            ip_address(_ip)
            return True
        except ValueError:
            return False
    if isinstance(ip, str): 
        return _fromStr(ip)
    elif isinstance(ip, int):  
        return _fromInt(ip)
    else: 
        raise TypeError('isValidIP')

class IPv4Macher:
    pattern = "^\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])$"
    compiled = re.compile(pattern)
    compiled2 = re.compile(pattern.encode(encoding='utf-8'))

def toBytes(obj:object)->bytes:
    """ make object to bytes if supports, using `ascii` as defualt encode """
    if isinstance(obj, str):
        try:
            return bytes(obj, 'ascii')
        except UnicodeEncodeError:
            return bytes(obj, 'utf-8')
    elif isinstance(obj, (bytes, bytearray)):
        return obj
    return memoryview(obj).tobytes()

def recursiveEncode(s:str, level:int=10)->str:
    """ recursive encode `s` using base64,
        `level` is depth of recursive, the max value is 32 """
    assert level <= 32
    if level <= 0:
        return str(s, 'utf-8')
    if not isinstance(s, (bytearray,bytes)):
        s = bytes(s, 'utf-8')
    return recursiveEncode(b64encode(s), level-1)

def recursiveDecode(s:str, level:int=10)->str:
    """ recursive decode `s`(encoded by `recursiveEncode`) using base64,
        `level` is depth of recursive, the max value is 32 """
    assert level <= 32
    if level <= 0:
        return str(s, 'utf-8')
    if not isinstance(s, (bytearray,bytes)):
        s = bytes(s, 'utf-8')
    return recursiveDecode(b64decode(s), level-1)

def utf8(s, errors='replace')->str:
    """ transform s to 'utf-8' coding """ 
    return str(s, 'utf-8', errors=errors)

def loadJson(name, objHook=None)->dict:
    """ load json from file return dict """
    with open(name, encoding='utf-8', errors='replace') as f:
        return json.loads(f.read(), encoding='utf-8', object_hook=objHook)

def dumpJson(obj:dict, name=None)->str:
    """ dump json(dict) to file """
    str = json.dumps(obj, indent=4, ensure_ascii=False)
    if name:
        with open(name, 'w') as f:
            f.write(str)
    return str

def debug(*args, **kwargs):
    """ print to stderr not stdout """
    print(strftime('%Y-%m-%d %H:%M:%S'), *args, file=sys.stderr, flush=True, **kwargs)

class IncreamentID(int):
    """ auto increament id """
    @staticmethod
    def create(init=0):
        return iter(IncreamentID(init))
    def __iter__(self):
        while True: 
            yield self 
            self += 1
    def __init__(self, init=0): self = 0
    def __str__(self):  return str(self)

class DummyLock(object):
    """ dummy lock for non-multithread """
    def __init__(self): pass
    def __enter__(self): pass
    def __exit__(self, exctype, excinst, exctb): pass
    def acquire(self, *args, **kwargs): pass
    def release(self, *args, **kwargs): pass

def isotime():
    """ return iso datetime, like 2014-03-28 19:45:59 """ 
    return strftime('%Y-%m-%d %H:%M:%S')

def isTimePoint(timeIn24hFormat):
    """ check now is specify time """
    return strftime('%H:%M:%S') == timeIn24hFormat

def waitUntil(timeIn24hFormat):
    """ return until now is hour:minute:second """
    while True:
        if strftime('%H:%M:%S') == timeIn24hFormat:
            break
        sleep(0.5)

def timeMeter(src=os.path.basename(__file__)):
    """ print time when enter wrapped function and leave wrapped function """
    def _wrapper(func):
        @functools.wraps(func)
        def _call(*args, **kwargs):
            debug(isotime() + (' %s : %s started...' % (src, _call.__name__)))
            ret = func(*args, **kwargs)
            debug(isotime() + (' %s : %s finished...' % (src, _call.__name__)))
            return ret
        return _call
    return _wrapper

def printException(name, output=sys.stderr):
    """ print exception with extra info(name) and limit traceback in 2 """
    assert name
    assert output is sys.stderr or output is sys.stdout
    exc_type, exc_val, exc_tb = sys.exc_info()
    exc_type = str(exc_type).lstrip('class <').rstrip('>')
    sys.stderr.write('%s : %s from :%s\n' % (exc_type, exc_val, name))
    traceback.print_tb(exc_tb, limit=2, file=output)

class IOFlusher(Thread):
    """ used for global io flusher """
    def __init__(self, delay=5, ios=(sys.stdout, sys.stderr)):
        """ flush stdout/stderr every `delay` seconds """
        Thread.__init__(self)
        self.daemon = True
        self._ios = ios
        self._delay = delay
    
    def run(self):
        """ run in a daemon thread forever """
        while True:
            for each in self._ios:
                each.flush()
            sleep(self._delay)

# the global io flusher
__flusher = IOFlusher()

def runIoFlusher()->bool:
    """ make global io flusher running, call only once in '__main__' """
    global __flusher
    if __flusher.is_alive():
        return False
    __flusher.start()
    return True

class Timer(object):
    """ a simple timer for debug using """
    def __init__(self):
        self.t = time()
    def reset(self):
        self.t = time()
    def __str__(self):
        return str(round((time() - self.t), 4))
    def __repr__(self):
        return self.__str__()
    def __float__(self):
        return time() - self.t
    def __int__(self):
        return int(time() - self.t)
    def show(self):
        print(str(self))

# get python version tuple like (3, 4)
PythonVersion = (sys.version_info.major, sys.version_info.minor)

def singleton(cls, *args, **kw):
    """ singleton object wrapper, usage:
        @singleton
        class MyClass(object):
            pass
    """
    instances = {} 
    def _object(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _object


def test():
    assert(isValidIP('127.0.0.1'))
    assert(isValidIP('4.4.4.4'))
    assert(isValidIP('192.168.255.0'))
    assert(isValidIP('::1'))
    assert(isValidIP('2620:0:1cfe:face:b00c::3'))
    assert(not isValidIP('www.google.com'))
    assert(not isValidIP('localhost'))
    assert(not isValidIP('[4.4.4.4]'))
    assert(not isValidIP('127.0.0.1.2.3'))
    assert(not isValidIP('123123123123'))
    assert(not isValidIP('\x00\x01\x02\x03'))
    assert(not isValidIP('123.123.321.456'))
    assert(not isValidIP(''))
    assert(isTimePoint(isotime().split()[1]))
    t = isotime().split()[1]
    sleep(1)
    assert(not isTimePoint(t))
    print(dumpJson({t:'go', 'key':PythonVersion}))

if __name__ == '__main__':
    test()


