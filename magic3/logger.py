# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import logging, os, sys
import traceback, inspect
from threading import Lock
from magic3.utils import singleton, DummyLock

def _caller(depth = 1):
    ''' get caller of current frame '''
    cf = inspect.currentframe()
    for i in range(depth+1):
        cf = cf.f_back
    try:
        return '[%s] ' % inspect.getframeinfo(cf).function
    except AttributeError:
        return '[%s] ' % inspect.getframeinfo(inspect.currentframe()).function


@singleton
class Logger(object):
    """ A singleton logger depends on logging module, usage:
        log = Logger('/var/project/yourname.log')
        log('some message')  # default level is INFO
        log.error('error happened!')
        log.debug('some debug info')
        log.info('name:', somevar, 'id:', 54321)
        ...
        Logger supports 5 levels common used, they are:
        DEBUG, INFO, WARN, ERROR, CRITICAL
        Note this logger is not fast, do Not use it if performance is important!
    """
    def __init__(self, logfile, append=True, locktype=DummyLock):
        """ make sure logfile's path is existed and valid """
        self.__logfile = logfile
        self.__nlevel = {'debug':0, 'info':0, 'warn':0, 'error':0, 'fatal':0}
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
        self.__log = { 'debug' : logging.debug,
                       'info'  : logging.info,
                       'warn'  : logging.warn,
                       'error' : logging.error,
                       'fatal' : logging.fatal }

    def __del__(self):
        """ not nessesary """
        logging.shutdown()
    
    def check(self)->bool:
        """ check logfile exists or not """
        return os.path.exists(self.__logfile);
    
    def filename(self)->str:
        """ return real path of logfile """
        return os.path.realpath(self.__logfile)
    
    def filesize(self)->int:
        """ return size in byte of logfile """
        return os.path.getsize(self.__logfile)

    def __call__(self, *messages, level = 'info', tb = ''):
        """ logging messages to logfile under level given """
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
        """ multi record """
        for msg, lv in dictargs.items():
            self.__call__(msg, level=lv, tb=_caller(1))

    def debug(self, *messages):
        self.__call__(*messages, level='debug', tb=_caller(1))

    def info(self, *messages):
        self.__call__(*messages, level='info', tb=_caller(1))
    
    def warn(self, *messages):
        self.__call__(*messages, level='warn', tb=_caller(1))
        
    def error(self, *messages):
        self.__call__(*messages, level='error', tb=_caller(1))
        
    def fatal(self, *messages):
        self.__call__(*messages, level='fatal', tb=_caller(1))
    
    def current_lines(self)->int:
        """ get number lines in current log file """ 
        return self.__nlines
    
    def current_levels(self)->dict:
        """ get number levels in current log file """ 
        return self.__nlevel.copy()


def test():
    """ test for Logger """
    log = Logger(os.path.expanduser('~') + os.sep + 'test.log')
    log.check()
    print('log file : %s' % log.filename())
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
    print('log file size : %.3fKB' % (log.filesize() / 1024))


if __name__ == '__main__':
    test()



