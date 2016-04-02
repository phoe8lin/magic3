# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import os, sys, traceback
import asyncio, threading, socket
import json, re, functools
import _io, codecs
from _hashlib import openssl_md5
from base64 import b64encode, b64decode
from ipaddress import ip_address
from time import time, strftime, sleep

# get python version tuple like (3, 4)
PythonVersion = (sys.version_info.major, sys.version_info.minor)


def is_valid_ip(ip)->bool:
    """ Returns true if the given string is a well-formed IP address(v4/v6) """
    def from_str(_ip):
        if not _ip or '\x00' in _ip:
            return False
        try:
            res = socket.getaddrinfo(_ip, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_NUMERICHOST)
            return bool(res)
        except Exception:
            return False
    def from_int(_ip):
        try: 
            ip_address(_ip)
            return True
        except ValueError:
            return False
    if isinstance(ip, str): 
        return from_str(ip)
    elif isinstance(ip, int):  
        return from_int(ip)
    else: 
        raise TypeError('is_valid_ip')

class IPv4Macher:
    pattern = "^\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])$"
    compiled = re.compile(pattern)
    compiled2 = re.compile(pattern.encode(encoding='utf-8'))


def to_bytes(obj:object)->bytes:
    """ make object to bytes if supports, using `ascii` as defualt encode """
    if isinstance(obj, str):
        try:
            return bytes(obj, 'ascii')
        except UnicodeEncodeError:
            return bytes(obj, 'utf-8')
    elif isinstance(obj, (bytes, bytearray)):
        return obj
    return memoryview(obj).tobytes()

def utf8(s, errors='replace')->str:
    """ transform s to 'utf-8' coding """ 
    return str(s, 'utf-8', errors=errors)


def MD5(buf:bytes)->str:
    """ get md5 hexdigest of bytes """
    return openssl_md5(buf).hexdigest()

def recursive_encode(s:str, level:int=10)->str:
    """ recursive encode `s` using base64,
        `level` is depth of recursive, the max value is 32 """
    assert level <= 32
    if level <= 0:
        return str(s, 'utf-8')
    if not isinstance(s, (bytearray,bytes)):
        s = bytes(s, 'utf-8')
    return recursive_encode(b64encode(s), level-1)

def recursive_decode(s:str, level:int=10)->str:
    """ recursive decode `s`(encoded by `recursive_encode`) using base64,
        `level` is depth of recursive, the max value is 32 """
    assert level <= 32
    if level <= 0:
        return str(s, 'utf-8')
    if not isinstance(s, (bytearray,bytes)):
        s = bytes(s, 'utf-8')
    return recursive_decode(b64decode(s), level-1)


def load_json(name, objHook=None)->dict:
    """ load json from file return dict """
    with _io.open(name, encoding='utf-8', errors='replace') as f:
        return json.loads(f.read(), encoding='utf-8', object_hook=objHook)

def dump_json(obj:dict, name=None)->str:
    """ dump json(dict) to file """
    str = json.dumps(obj, indent=4, ensure_ascii=False)
    if name:
        with _io.open(name, 'w') as f:
            f.write(str)
    return str

def debug(*args, **kwargs):
    """ print to stderr not stdout """
    ts = time()
    dt = strftime('%F %T') + ('.%03d' % ((ts - int(ts)) * 1000))
    print(dt, *args, file=sys.stderr, flush=True, **kwargs)


class DummyLock:
    """ dummy lock for non-multithread """
    __slots__ = ()
    def __init__(self): pass
    def __enter__(self): pass
    def __exit__(self, exctype, excinst, exctb): pass
    def acquire(self, *args, **kwargs): pass
    def release(self, *args, **kwargs): pass


def isotime():
    """ return iso datetime, like 2014-03-28 19:45:59 """ 
    return strftime('%F %T')

def is_time_point(time_in_24h):
    """ check now is specify time """
    return strftime('%H:%M:%S') == time_in_24h

def wait_until(time_in_24h):
    """ return until now is hour:minute:second """
    while True:
        if strftime('%H:%M:%S') == time_in_24h:
            break
        sleep(0.5)

def time_meter(src=os.path.basename(__file__)):
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

def print_error(name, output=sys.stderr):
    """ print exception with extra info(name) and limit traceback in 2 """
    assert name
    assert output is sys.stderr or output is sys.stdout
    exc_type, exc_val, exc_tb = sys.exc_info()
    exc_type = str(exc_type).lstrip('class <').rstrip('>')
    sys.stderr.write('%s : %s from :%s\n' % (exc_type, exc_val, name))
    traceback.print_tb(exc_tb, limit=2, file=output)


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


def aio_loop():
    """ get event loop """
    return asyncio.get_event_loop()

def aio_tasks(*future):
    """ wrap more futures as one waitable future """
    assert isinstance(future, (list, tuple))
    return asyncio.tasks.wait(future);

def aio_run(*future, loop=None):
    """ run until complete """
    if not loop:
        loop = aio_loop()
    if len(future) > 1:
        return loop.run_until_complete(aio_tasks(*future))
    else:
        return loop.run_until_complete(future[0])

def aio_to_future(coro, loop=None):
    """ make coroutine to asyncio.Future """
    return asyncio.ensure_future(coro, loop=loop)

def aio_call_at():
    pass

def aio_call_later():
    pass

def aio_call_soon():
    pass


class BomHelper(object):
    """ helper for check/insert/remove BOM head to small utf-8 files,
        the filesize can Not be larger than 1GB, multi-threads safe!
    """
    __lock__ = threading.Lock()
    def __init__(self, filename, defaultBOM=codecs.BOM_UTF8):
        """ defaultBOM can be specified by user from valid values in codecs """
        self.reset(filename, defaultBOM)

    def reset(self, filename, defaultBOM=codecs.BOM_UTF8):
        """ """
        if os.path.getsize(filename) > (1<<30):
            raise RuntimeError('Error: file %s is too large!!!' % self.__name)
        def _reset():
            self.__name = filename
            self.__bom = defaultBOM
        self.__sync(_reset)

    def __sync(self, fileop):
        with BomHelper.__lock__:
            return fileop()

    def __has(self):
        with open(self.__name, 'rb') as f:
            return f.read(3) == self.__bom

    def has(self):
        """ check file has BOM or not """
        return self.__sync(self.__has)

    def insert(self):
        """ add BOM to file, the filesize can Not be larger than 1GB """
        def _insert():
            if self.__has():
                return False
            with open(self.__name, 'rb') as fin:
                buf = fin.read(os.path.getsize(self.__name))
            with open(self.__name, 'wb') as fout:
                fout.write(self.__bom)
                fout.write(buf)
            return True
        return self.__sync(_insert)

    def remove(self):
        """ remove BOM from file, the filesize can Not be larger than 1GB """
        def _remove():
            if not self.__has():
                return False
            with open(self.__name, 'rb') as fin:
                fin.read(len(self.__bom))
                buf = fin.read(os.path.getsize(self.__name))
            with open(self.__name, 'wb') as fout:
                fout.write(buf)
            return True
        return self.__sync(_remove)

    def value(self):
        """ get default BOM value in bytes """
        return self.__bom


def test():
    assert(is_valid_ip('127.0.0.1'))
    assert(is_valid_ip('4.4.4.4'))
    assert(is_valid_ip('192.168.255.0'))
    assert(is_valid_ip('::1'))
    assert(is_valid_ip('2620:0:1cfe:face:b00c::3'))
    assert(not is_valid_ip('www.google.com'))
    assert(not is_valid_ip('localhost'))
    assert(not is_valid_ip('[4.4.4.4]'))
    assert(not is_valid_ip('127.0.0.1.2.3'))
    assert(not is_valid_ip('123123123123'))
    assert(not is_valid_ip('\x00\x01\x02\x03'))
    assert(not is_valid_ip('123.123.321.456'))
    assert(not is_valid_ip(''))
    assert(is_time_point(isotime().split()[1]))
    t = isotime().split()[1]
    sleep(1)
    assert(not is_time_point(t))
    j = dump_json({t:'go', 'key':PythonVersion})
    d = json.loads(j)
    assert(t in d and 3 in d['key']) 
    assert(MD5(b'abcdef0987654321') == 'eaa1c1d22e330b10903dfdbfed5e6ff9')
    print('test OK')


if __name__ == '__main__':
    test()


