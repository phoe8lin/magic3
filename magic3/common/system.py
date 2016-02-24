# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
""" this module only works on unix/linux systems!!! """
from subprocess import Popen, PIPE, getstatusoutput


class OSCommand(object):
    """ call system command, all methods are staticmethod """
    @staticmethod
    def call(cmd:str, showcmd=False)->str:
        return getstatusoutput(cmd)
    
    @staticmethod
    def execute(cmd:str, showcmd=False, stdout=PIPE, stderr=PIPE)->bytes:
        """ execute system command using Popen and return the result str """
        output = Popen(cmd, shell=True, stdout=stdout, stderr=stderr)
        return output.stdout.read()

    @staticmethod
    def popen(cmd:str, showcmd=False, stdout=PIPE, stderr=PIPE)->Popen:
        """ same as execute, but return stdout of pipe """
        return Popen(cmd, shell=True, stdout=stdout, stderr=stderr)

    @staticmethod
    def process_count(keyword:str)->tuple:
        """ count process specified in `keyword` using 'ps|egrep' system command """
        output = Popen('ps ax | egrep %s' % keyword, shell=True, stdout=PIPE)
        return tuple(line for line in map(lambda l:l.decode('utf-8').rstrip(), output.stdout)
                     if 'egrep '+keyword not in line)

    @staticmethod
    def process_countex(keyword:str, exclude:str)->tuple:
        """ like process_count, but exclude `exclude` word """
        output = Popen('ps ax | egrep %s' % keyword, shell=True, stdout=PIPE)
        return tuple(line for line in map(lambda l:l.decode('utf-8').rstrip(), output.stdout)
                     if 'egrep '+keyword not in line and exclude not in line)

    @staticmethod
    def getpid(keyword:str)->tuple:
        """ getpid """
        output = getstatusoutput('pgrep %s' % keyword)
        return tuple(int(i) for i in output[-1].split('\n'))


def test():
    import time, os
    r = OSCommand.call('date -u', True)
    assert r[0] == 0 and r[1]
    print("return code:", r[0], os.strerror(r[0]), '\n')
    p = OSCommand.popen('locale -v', True)
    print(p.stdout.read().decode('utf-8'))
    print()
    print('count "python3"')
    print(OSCommand.process_count('python3'))
    print()
    print('count "python3" but not "pydev"')
    print(OSCommand.process_countex('python3', 'pydev'))
    print()
    print(OSCommand.getpid('python3'))
    print()
    for _ in range(3):
        print(OSCommand.execute('uptime', True).decode('utf-8'))
        print(OSCommand.execute('free -mo', True).decode('utf-8'))
        time.sleep(1.0)

if __name__ == '__main__':
    test() 

