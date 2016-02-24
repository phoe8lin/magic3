# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import redis as pyredis
from threading import RLock
from magic3.utils import DummyLock,debug

# default redis host and port
DEFAULT_REDIS_HOST = 'localhost'
DEFAULT_REDIS_PORT = 6379

class RedisCMD(object):
    """ RedisCMD is both redis client and command wildcard template wrapper """ 
    __slots__ = ('_client','_cmd','_lock')
    def __init__(self, cmd, host=None, port=None, lock=False):
        """ if `lock` is true, using RLock else DummyLock """
        self._cmd = cmd
        if host and port:
            self._client = pyredis.Redis(host=host, port=port)
        else:
            self._client = pyredis.Redis(host=DEFAULT_REDIS_HOST, port=6379)
        try:    self._client.ping()
        except: raise RuntimeError('Could Not Connect to %cmd:%d\n' % (host, port))
        self._lock = RLock() if lock else DummyLock()
    
    def __str__(self):
        """ return current command str template """
        return self._cmd
    
    def __repr__(self):
        """ return current command str template """
        return self._cmd
    
    def execute(self, *args):
        """ fill command template with `args` and execute command """
        with self._lock:
            try:    return self._client.execute_command(*(self._cmd % args).split())
            except: debug('error cmd:', *(self._cmd % args).split())
    
    def __call__(self, *args):
        """ callable support, see test for more details """
        with self._lock:
            return self._client.execute_command(*(self._cmd % args).split()) 
        
    def assign(self, cmd):
        """ assign new command wildcard template """
        self._cmd = cmd
        return self


def test():
    rcmd = RedisCMD('SET %cmd %cmd', DEFAULT_REDIS_HOST, DEFAULT_REDIS_PORT, True)
    print(rcmd)
    print(rcmd.execute('foo', 'bar'))
    print(rcmd.assign('DBSIZE').execute())
    rcmd.assign('GET %cmd')
    print(rcmd.execute('foo'))
    print(rcmd.assign('DEL foo').execute())
    print(rcmd.assign('DBSIZE')())


