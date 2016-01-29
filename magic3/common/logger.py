# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import logging, os, sys
import traceback, inspect
from threading import Lock
from magic3.common.utils import singleton

@singleton
class Logger(object):
    """ a singleton logger depends on logging module, usage:
        log = Logger('/var/project/yourname.log')
        log('some message')  # default level is INFO
        log.error('error happened!')
        log.debug('some debug info')
        ...
        Logger supports 5 levels common used, they are:
        'debug', 'info', 'warn', 'error', 'critical'
    """
    def __init__(self, logfile, append=True):
        """ make sure logfile's path is existed and valid """
        self.__logfile = logfile
        self.__nlevel = {'debug':0, 'info':0, 'warn':0, 'error':0, 'critical':0}
        self.__nlines = 0
        self.__lock = Lock()
        self.mode = 'a' if append else 'w'
        self.level = logging.NOTSET
        self.datefmt = '%Y-%m-%d %H:%M:%S'
        self.msgformat = '%(asctime)s [%(levelname)s] %(message)s'
        logging.basicConfig(filename=logfile,
                            filemode=self.mode,
                            level=self.level,
                            datefmt=self.datefmt,
                            format=self.msgformat)
        self.__log = { 'debug'    : logging.debug,
                       'info'     : logging.info,
                       'warn'     : logging.warn,
                       'error'    : logging.error,
                       'critical' : logging.critical }

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

    def __caller(self, depth):
        cf = inspect.currentframe()
        for i in range(depth+1):
            cf = cf.f_back
        try:
            return '[%s] ' % inspect.getframeinfo(cf).function
        except AttributeError:
            return '[%s] ' % inspect.getframeinfo(inspect.currentframe()).function

    def __call__(self, *messages, level = 'info', tb = ''):
        """ logging messages to logfile under level given """
        try:
            self.__lock.acquire()
            if not tb:
                tb = self.__caller(1)
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
            self.__call__(msg, level=lv, tb=self.__caller(1))

    def debug(self, *messages):
        self.__call__(*messages, level='debug', tb=self.__caller(1))

    def info(self, *messages):
        self.__call__(*messages, level='info', tb=self.__caller(1))
    
    def warn(self, *messages):
        self.__call__(*messages, level='warn', tb=self.__caller(1))
        
    def error(self, *messages):
        self.__call__(*messages, level='error', tb=self.__caller(1))
        
    def critical(self, *messages):
        self.__call__(*messages, level='critical', tb=self.__caller(1))
    
    def current_lines(self)->int:
        """ get number lines in current log file """ 
        return self.__nlines
    
    def current_levels(self)->dict:
        """ get number levels in current log file """ 
        return self.__nlevel.copy()


def test():
    """ test for Logger """
    def test_inner(*args):
        args[0].info(*args[1:])
    log = Logger(os.path.expanduser('~') + os.sep + 'test.log')
    id1 = id(log)
    log = Logger(os.path.expanduser('~') + os.sep + 'test.log')
    id2 = id(log)
    assert id1 == id2
    log.check()
    print('log file : %s' % log.filename())
    log('log test start...')
    log.debug('this is debug: %s' % __name__)
    log('this is info', level='info')
    log.info('this is also info!')
    log('this is warning!', level='warn')
    log.warn('this is also warning')
    log('this is error!', level='error')
    log.error('this is also error!')
    log('some value:', 123456, level='info')
    test_inner(log, 1.2, 3.4, 666, 'xyz')
    test_inner(log, 'more lines:\nsome line1\nsome line2\nsome line3')
    log.record({'the first':'info', 'the second':'info'})
    log('log test finish...')
    print('lines:', log.current_lines())
    print('levels:', log.current_levels())
    print('log file size : %d' % log.filesize())


if __name__ == '__main__':
    test()


