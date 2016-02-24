# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import sys, gc

BEST_RECURSION_LIMIT = 256
BEST_CHECK_INTERVAL = 1024

def _set_excepthook(excepthook):
    """ set exception hook
        excepthook looks like:
        def user_hook(exctype, value, traceback):
            pass
    """
    sys.excepthook = excepthook

def global_config():
    """ set global best configs, eg: gc and recursion limit """
    sys.stderr.write('-' * 100 + '\n')
    sys.stderr.write('sys.platform : %s\n' % sys.platform)
    sys.stderr.write('sys.maxsize : %s\n' % hex(sys.maxsize))
    sys.stderr.write('sys.path : %s\n' % sys.path)
    sys.stderr.write('sys.excepthook : %s\n' % sys.excepthook)
    sys.stderr.write('old sys.checkinterval : %s\n' % sys.getcheckinterval())
    sys.setcheckinterval(BEST_CHECK_INTERVAL)
    sys.stderr.write('new sys.checkinterval : %s\n' % sys.getcheckinterval())
    sys.stderr.write('old sys.recursionlimit : %s\n' % sys.getrecursionlimit())
    sys.setrecursionlimit(BEST_RECURSION_LIMIT)
    sys.stderr.write('new sys.recursionlimit : %s\n' % sys.getrecursionlimit())
    sys.stderr.write('old gc.threshold : %s\n' % str(gc.get_threshold()))
    gc.set_threshold(512, 8, 4)
    sys.stderr.write('new gc.threshold : %s\n' % str(gc.get_threshold()))
    sys._clear_type_cache()
    sys.stderr.write('sys._clear_type_cache : performed\n')
    sys.stderr.write('-' * 100 + '\n\n')


if __name__ == '__main__':
    def user_hook(exctype, value, traceback):
        sys.stderr.write('Bingo! %s %s %s\n' % (exctype, value, traceback))
    _set_excepthook(user_hook)
    global_config()
    raise ValueError('Uncatched Error(for test)')

