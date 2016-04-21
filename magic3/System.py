# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
""" this module only works on unix/linux systems!!! """
import sys, gc, time, os
from threading import Thread
from subprocess import Popen, PIPE, getstatusoutput

BEST_RECURSION_LIMIT = 4096
BEST_CHECK_INTERVAL = 1024

def SetUnexceptedHook(excepthook):
    """ set exception hook
        excepthook looks like:
        def user_hook(exctype, value, traceback):
            pass
    """
    sys.excepthook = excepthook

def GetCpuCount():
    """ Return the number of CPUs in the system; return None if indeterminable """
    return os.cpu_count()

def SysReConfig():
    """ set global best configs, eg: gc and recursion limit """
    cfg = {}
    cfg['sys.platform'] = sys.platform
    cfg['sys.maxsize'] = sys.maxsize
    cfg['sys.path'] = sys.path
    cfg['sys.excepthook'] = sys.excepthook
    cfg['old sys.checkinterval'] = sys.getcheckinterval()
    sys.setcheckinterval(BEST_CHECK_INTERVAL)
    cfg['new sys.checkinterval'] = sys.getcheckinterval()
    cfg['old sys.recursionlimit'] = sys.getrecursionlimit()
    sys.setrecursionlimit(BEST_RECURSION_LIMIT)
    cfg['new sys.recursionlimit'] = sys.getrecursionlimit()
    cfg['old gc.threshold'] = str(gc.get_threshold())
    gc.set_threshold(512, 8, 6)
    cfg['new gc.threshold'] = str(gc.get_threshold())
    sys._clear_type_cache()
    cfg['sys._clear_type_cache'] = True
    return cfg


class OSCommand(object):
    """ call system command, all methods are staticmethod """
    @staticmethod
    def Call(cmd:str, showcmd=False)->str:
        return getstatusoutput(cmd)
    
    @staticmethod
    def Execute(cmd:str, showcmd=False, stdout=PIPE, stderr=PIPE)->bytes:
        """ execute system command using Popen and return the result str """
        output = Popen(cmd, shell=True, stdout=stdout, stderr=stderr)
        return output.stdout.read()

    @staticmethod
    def Popen(cmd:str, showcmd=False, stdout=PIPE, stderr=PIPE)->Popen:
        """ same as execute, but return stdout of pipe """
        return Popen(cmd, shell=True, stdout=stdout, stderr=stderr)

    @staticmethod
    def ProcessCount(keyword:str)->tuple:
        """ count process specified in `keyword` using 'ps|egrep' system command """
        output = Popen('ps ax | egrep %s' % keyword, shell=True, stdout=PIPE)
        return tuple(line for line in map(lambda l:l.decode('utf-8').rstrip(), output.stdout)
                     if 'egrep '+keyword not in line)

    @staticmethod
    def ProcessCountExtra(keyword:str, exclude:str)->tuple:
        """ like process_count, but exclude `exclude` word """
        output = Popen('ps ax | egrep %s' % keyword, shell=True, stdout=PIPE)
        return tuple(line for line in map(lambda l:l.decode('utf-8').rstrip(), output.stdout)
                     if 'egrep '+keyword not in line and exclude not in line)

    @staticmethod
    def GetPid(keyword:str)->tuple:
        """ getpid """
        output = getstatusoutput('pgrep %s' % keyword)
        return tuple(int(i) for i in output[-1].split('\n'))


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
            time.sleep(self._delay)


# the global io flusher
__GlobalFlusher = IOFlusher()

def RunIOFlusher()->bool:
    """ make global io flusher running, call only once in '__main__' """
    global __GlobalFlusher
    if __GlobalFlusher.is_alive():
        return False
    __GlobalFlusher.start()
    return True


def test():
    import time, os, pprint
    assert RunIOFlusher()
    r = OSCommand.Call('date -u', True)
    assert r[0] == 0 and r[1]
    print("return code:", r[0], os.strerror(r[0]), '\n')
    p = OSCommand.Popen('locale -v', True)
    print(p.stdout.read().decode('utf-8'))
    print()
    print('count "python3"')
    print(OSCommand.ProcessCount('python3'))
    print()
    print('count "python3" but not "pydev"')
    print(OSCommand.ProcessCountExtra('python3', 'pydev'))
    print()
    print(OSCommand.GetPid('python3'))
    print()
    
    for _ in range(3):
        print(OSCommand.Execute('uptime', True).decode('utf-8'))
        print(OSCommand.Execute('free -mo', True).decode('utf-8'))
        time.sleep(1.0)
    
    def user_hook(exctype, value, traceback):
        sys.stderr.write('Bingo! %s %s %s\n' % (exctype, value, traceback))
    
    SetUnexceptedHook(user_hook)
    try:
        raise ValueError('Uncatched Error(for test)')
    except:
        pass
    pprint.pprint(SysReConfig())
    
    assert (GetCpuCount() >= 1)


if __name__ == '__main__':
    test() 


