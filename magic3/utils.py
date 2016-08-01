# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import os, sys, traceback
import threading, socket
import json, re
import functools
import _io, codecs

try:
    from _hashlib import openssl_md5 as _md5hash
except ImportError:
    from hashlib import md5 as _md5hash

from base64 import b64encode, b64decode
from ipaddress import ip_address
from time import time, strftime, sleep
from datetime import datetime, date

# Get python version tuple like (3, 4)
PythonVersion = (sys.version_info.major, sys.version_info.minor)


def is_valid_ip(ip) -> bool:
    ''' Returns true if the given string is a well-formed IP address(v4/v6) '''

    if isinstance(ip, str):
        if not ip or '\x00' in ip:
            return False
        try:
            res = socket.getaddrinfo(ip, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_NUMERICHOST)
            return bool(res)
        except Exception:
            return False

    elif isinstance(ip, int):
        try:
            ip_address(ip)
            return True
        except ValueError:
            return False

    else:
        raise TypeError('is_valid_ip: ' + str(ip))


class IPv4Macher:
    ''' Use IPv4Macher.compiled to match strings is IP address or not '''

    pattern = "^\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])\.\
([0-9]|[1-9][0-9]|1\d\d|2[0-4]\d|25[0-5])$"

    compiled = re.compile(pattern)
    compiled2 = re.compile(pattern.encode(encoding='utf-8'))


def to_bytes(obj:object) -> bytes:
    ''' Make object to bytes if supports, using `ascii` as defualt encode '''

    if isinstance(obj, str):
        try:
            return bytes(obj, 'ascii')
        except UnicodeEncodeError:
            return bytes(obj, 'utf-8')
    elif isinstance(obj, (bytes, bytearray)):
        return obj

    return memoryview(obj).tobytes()

def utf8(s, errors='replace') -> str:
    ''' Transform s to 'utf-8' coding '''
    return str(s, 'utf-8', errors=errors)


def md5(buf:bytes) -> str:
    ''' Get md5 hexdigest of bytes '''
    return _md5hash(buf).hexdigest()

def recursive_encode(s:str, level:int=10) -> str:
    ''' Recursive encode `s` using base64,
        `level` is depth of recursive, the max value is 32 '''
    assert level <= 32

    if level <= 0:
        return str(s, 'utf-8')

    if not isinstance(s, (bytearray, bytes)):
        s = bytes(s, 'utf-8')

    return recursive_encode(b64encode(s), level - 1)

def recursive_decode(s:str, level:int=10) -> str:
    ''' Recursive decode `s`(encoded by `recursive_encode`) using base64,
        `level` is depth of recursive, the max value is 32 '''
    assert level <= 32

    if level <= 0:
        return str(s, 'utf-8')

    if not isinstance(s, (bytearray, bytes)):
        s = bytes(s, 'utf-8')

    return recursive_decode(b64decode(s), level - 1)


def loadjson(name, objHook=None) -> dict:
    ''' Load json from file return dict '''
    try:
        with _io.open(name, encoding='utf-8', errors='replace') as f:
            return json.loads(f.read(), encoding='utf-8', object_hook=objHook)
    except Exception as e:
        if 'BOM' in str(e):
            with _io.open(name, encoding='utf-8-sig', errors='replace') as f:
                return json.loads(f.read(), encoding='utf-8-sig', object_hook=objHook)

def dumpjson(obj:dict, name=None) -> str:
    ''' Dump json(dict) to file '''
    str = json.dumps(obj, indent=4, ensure_ascii=False)
    if name:
        with _io.open(name, 'w') as f:
            f.write(str)
    return str


def debug(*args, **kwargs):
    ''' Print to stderr not stdout '''
    ts = time()
    dt = strftime('%F %T') + ('.%03d' % ((ts - int(ts)) * 1000))
    print(dt, *args, file=sys.stderr, flush=True, **kwargs)


class DummyLock:
    ''' Dummy lock for non-multithread '''
    __slots__ = ()

    def __init__(self): pass
    def __enter__(self): pass
    def __exit__(self, exctype, excinst, exctb): pass
    def acquire(self, *args, **kwargs): pass
    def release(self, *args, **kwargs): pass


def isotime():
    ''' Return iso datetime, like 2014-03-28 19:45:59 '''
    return strftime('%F %T')

def is_time(hms_in_24h:str):
    ''' Check now is specify time '''
    return strftime('%H:%M:%S') == hms_in_24h

def wait_until(hms_in_24h:str):
    ''' Return until now is Hour:Minute:Second '''
    while True:
        if strftime('%H:%M:%S') == hms_in_24h:
            break
        sleep(0.5)

def time_meter(src=os.path.basename(__file__)):
    ''' Print time when enter wrapped function and leave wrapped function '''

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
    ''' Print exception with extra info(name) and limit traceback in 2 '''
    assert name
    assert output is sys.stderr or output is sys.stdout
    exc_type, exc_val, exc_tb = sys.exc_info()
    exc_type = str(exc_type).lstrip('class <').rstrip('>')
    sys.stderr.write('%s : %s from :%s\n' % (exc_type, exc_val, name))
    traceback.print_tb(exc_tb, limit=2, file=output)


class Timer(object):
    ''' A simple timer for debug using '''
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


def Singleton(cls, *args, **kw):
    ''' Singleton object wrapper, usage:
        @Singleton
        class MyClass(object):
            pass
    '''
    instances = {}

    def _object(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _object


class BomHelper(object):
    ''' Helper for check/insert/remove BOM head to small utf-8 files,
        the filesize can Not be larger than 1GB, multi-threads safe!
    '''
    __lock__ = threading.Lock()

    def __init__(self, filename, defaultBOM=codecs.BOM_UTF8):
        ''' DefaultBOM can be specified by user from valid values in codecs '''
        self.reset(filename, defaultBOM)

    def reset(self, filename, defaultBOM=codecs.BOM_UTF8):
        ''' '''
        if os.path.getsize(filename) > (1 << 30):
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
        ''' check file has BOM or not '''
        return self.__sync(self.__has)

    def insert(self):
        ''' add BOM to file, the filesize can Not be larger than 1GB '''

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
        ''' remove BOM from file, the filesize can Not be larger than 1GB '''

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
        ''' get default BOM value in bytes '''
        return self.__bom


# For indexing readable weekday strings
_weekdays = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')

_weekdays_zh = ('星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期日')

_isoweekdays = (None, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')

def make_calendar(year=None):
    ''' Return a list contains all days datetime object with weekday in the `year` '''
    if not year:
        year = datetime.today().year

    cal = [None]

    for i in range(1, 13):
        month = [(None, None)]
        for j in range(1, 32):
            try:
                dt = date(year, i, j)
                month.append((dt, _weekdays[dt.weekday()]))
            except ValueError:
                pass
        cal.append(month)

    return cal

def make_week_table(year=None):
    ''' Return a table of collected days on each weeks in a year(always 52 weeks) '''
    if not year:
        year = datetime.today().year

    cal = make_calendar(year)
    wdt = {}

    if cal[1][1][1] == 'Monday':
        num = 0
    else:
        num = 1

    for m in cal[1:]:
        for d, w in m:
            if not d:
                continue
            if w == 'Monday':
                num += 1
            wdt[d] = num

    return wdt

def date_delta(y1, m1, d1, y2, m2, d2):
    ''' Return timedelta object of two date, eg:
        d = date_delta(2015, 3, 2, 2016, 3, 2)
    '''

    y1, m1, d1 = int(y1), int(m1), int(d1)
    y2, m2, d2 = int(y2), int(m2), int(d2)
    d1 = datetime(y1, m1, d1, 0, 0, 0)
    d2 = datetime(y2, m2, d2, 0, 0, 0)

    return d2 - d1 if d2 > d1 else d1 - d2

def time_delta(H1, M1, S1, H2, M2, S2):
    ''' Return timedelta object of two time, eg:
        d = time_delta(22, 33, 44, 23, 59, 0)
    '''

    H1, M1, S1 = int(H1), int(M1), int(S1)
    H2, M2, S2 = int(H2), int(M2), int(S2)
    d1 = datetime(1970, 1, 1, H1, M1, S1)
    d2 = datetime(1970, 1, 1, H2, M2, S2)

    return d2 - d1 if d2 > d1 else d1 - d2

def datetime_delta(y1, m1, d1, H1, M1, S1, y2, m2, d2, H2, M2, S2):
    ''' Return timedelta object of two datetime '''

    y1, m1, d1, H1, M1, S1 = int(y1), int(m1), int(d1), int(H1), int(M1), int(S1)
    y2, m2, d2, H2, M2, S2 = int(y2), int(m2), int(d2), int(H2), int(M2), int(S2)
    d1 = datetime(y1, m1, d1, H1, M1, S1)
    d2 = datetime(y2, m2, d2, H2, M2, S2)

    return d2 - d1 if d2 > d1 else d1 - d2


class WeekDayUtil:
    ''' Utils for datetime operations on weekdays, see test below '''

    weekdays = _weekdays
    weekdays_zhmap = dict(zip(_weekdays, _weekdays_zh))
    isoweekdays = _isoweekdays

    calendar = make_calendar()
    weektable = make_week_table()

    @staticmethod
    def first_someday(month, weekday='Monday'):
        ''' Get first Monday of the month '''
        assert weekday in _weekdays
        cal = WeekDayUtil.calendar
        return next(filter(lambda _:_[1] == weekday, cal[month]))[0]

    @staticmethod
    def all_someday(month, weekday='Monday'):
        ''' Get all specified weekdays of the month '''
        assert weekday in _weekdays
        cal = WeekDayUtil.calendar
        return list(t[0] for t in filter(lambda _:_[1] == weekday, cal[month]))

    @staticmethod
    def reset(year):
        WeekDayUtil.calendar = make_calendar(year)
        WeekDayUtil.weektable = make_week_table(year)


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

    assert(is_time(isotime().split()[1]))
    t = isotime().split()[1]
    sleep(1)

    assert(not is_time(t))
    j = dumpjson({t:'go', 'key':PythonVersion})
    d = json.loads(j)

    assert(t in d and 3 in d['key'])
    assert(md5(b'abcdef0987654321') == 'eaa1c1d22e330b10903dfdbfed5e6ff9')
    assert(recursive_decode(recursive_encode('github.com')) == 'github.com')
    assert(BomHelper(__file__).value() == codecs.BOM_UTF8)

    print(date_delta(2015, 1, 1, 2014, 1, 1))
    print(time_delta(21, 30, 0, 22, 0, 0))
    print(datetime_delta(2015, 5, 2, 0, 30, 30, 2015, 5, 12, 12, 30, 30))
    print(WeekDayUtil.first_someday(5, 'Monday'))
    print(WeekDayUtil.all_someday(5, 'Sunday'))

    WeekDayUtil.reset(2017)

    print(WeekDayUtil.first_someday(1, 'Monday'))
    print(WeekDayUtil.all_someday(1, 'Sunday'))
    print(WeekDayUtil.weekdays_zhmap)

    debug(__file__, ': Test OK')


if __name__ == '__main__':
    test()

