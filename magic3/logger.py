# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import logging, os, sys
import traceback, inspect
from threading import Lock
from magic3.utils import Singleton, DummyLock


def _caller(depth = 1):
    ''' Get caller of current frame '''
    cf = inspect.currentframe()
    
    for i in range(depth+1):
        cf = cf.f_back
        
    try:
        return '[%s] ' % inspect.getframeinfo(cf).function
    except AttributeError:
        return '[%s] ' % inspect.getframeinfo(inspect.currentframe()).function


@Singleton
class Logger(object):
    ''' A Singleton logger depends on logging module, usage:
        log = Logger('/var/project/yourname.log')
        log('some message')  # default level is INFO
        log.error('error happened!')
        log.debug('some debug info')
        log.info('name:', somevar, 'id:', 54321)
        ...
        Logger supports 5 levels common used, they are:
        DEBUG, INFO, WARN, ERROR, CRITICAL
        Note this logger is not fast, do Not use it if performance is important!
    '''
    
    def __init__(self, logfile, append=True, locktype=DummyLock):
        ''' Make sure logfile's path is existed and valid '''
        self.__logfile = logfile
        self.__nlevel = {'DEBUG':0, 'INFO':0, 'WARN':0, 'ERROR':0, 'FATAL':0}
        self.__nlines = 0
        self.__lock = locktype()
        
        self.mode = 'a' if append else 'w'
        self.level = logging.NOTSET
        self.dtfmt = '%Y-%m-%d %H:%M:%S'
        self.msgfmt = '%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s'
        
        logging.basicConfig(filename=logfile,
                            filemode=self.mode,
                            level=self.level,
                            datefmt=self.dtfmt,
                            format=self.msgfmt)
        
        self.__log = { 'DEBUG' : logging.debug,
                       'INFO'  : logging.info,
                       'WARN'  : logging.warn,
                       'ERROR' : logging.error,
                       'FATAL' : logging.fatal }
        
        self.INFO = self.info
        self.DEBUG = self.debug
        self.WARN = self.warn
        self.ERROR = self.error
        self.FATAL = self.fatal

    def __del__(self):
        logging.shutdown()
    
    def check(self)->bool:
        ''' Check logfile exists or not '''
        return os.path.exists(self.__logfile);
    
    @property
    def name(self)->str:
        ''' Return real path of logfile '''
        return os.path.realpath(self.__logfile)
    
    @property
    def size(self)->int:
        ''' Return size in byte of logfile '''
        return os.path.getsize(self.__logfile)

    def __call__(self, *messages, level = 'INFO', tb = ''):
        ''' Logging messages to logfile under level given '''
        try:
            self.__lock.acquire()
            
            if not tb:
                tb = _caller(1)
                
            msg = ' '.join(map(lambda x : str(x), messages))
            
            if '\n' in msg:
                for s in msg.split('\n'):
                    self.__log[level](tb + s);
                    self.__nlines += 1
                    self.__nlevel[level] += 1
            else:
                self.__log[level](tb + msg)
                self.__nlines += 1
                self.__nlevel[level] += 1
                
        except KeyError:
            raise KeyError(level, 'is not a valid level')
        
        finally:
            self.__lock.release()

    def record(self, dictargs):
        ''' Multi record '''
        for msg, lv in dictargs.items():
            self.__call__(msg, level=lv, tb=_caller(1))

    def debug(self, *messages):
        self.__call__(*messages, level='DEBUG', tb=_caller(1))

    def info(self, *messages):
        self.__call__(*messages, level='INFO', tb=_caller(1))
    
    def warn(self, *messages):
        self.__call__(*messages, level='WARN', tb=_caller(1))
        
    def error(self, *messages):
        self.__call__(*messages, level='ERROR', tb=_caller(1))
        
    def fatal(self, *messages):
        self.__call__(*messages, level='FATAL', tb=_caller(1))
    
    def current_lines(self)->int:
        ''' Get number lines in current log file ''' 
        return self.__nlines
    
    def current_levels(self)->dict:
        ''' Get number levels in current log file ''' 
        return self.__nlevel.copy()

    

def test():
    ''' Test for Logger '''
    log = Logger(os.path.expanduser('~') + os.sep + 'test.log')
    log.check()
    print('log file : %s' % log.name)
    
    INFO = log.info
    WARN = log.warn
    ERROR = log.error
    DEBUG = log.debug
    FATAL = log.fatal
    
    INFO('log test start...')
    DEBUG('this is debug: %s' % __name__)
    INFO('this is info')
    WARN('this is warning!')
    WARN('this is also warning')
    ERROR('this is error!')
    ERROR('this is also error!')
    
    INFO('some value:', 123456)
    INFO({'the first':'info', 'the second':'info'})
    FATAL('fuck!!!')
    INFO('log test finish...')
    
    print('log lines:', log.current_lines())
    print('log levels:', log.current_levels())
    
    print('log file size : %.3fKB' % (log.size / 1024))


if __name__ == '__main__':
    test()



