# -*- coding:utf-8 -*-
# author : cypro666
# date   : 2016-03-08
from os.path import exists
from io import BufferedReader
from string import Template
from magic3.system import OSCommand

AWK_CMD = Template("""awk -F "$delim" '{print $vargs}' $files 2>&1""")

def openAWK(fileList:list, argList:list, delim)->BufferedReader:
    """ call awk command and return an opened pipe for read the output of awk, eg:
        for line in openAWK([file1, file2], [2,3,4], ',').stdout:
            print(line)   
    """
    if isinstance(delim, (bytes, bytearray)):
        delim = str(delim, 'utf-8')
    if not fileList:
        raise ValueError('fileList')
    if not argList:
        raise ValueError('argList')
    if len(delim) != 1:
        raise ValueError('delim must be a length 1 char')
    for fn in fileList:
        if not exists(fn):
            raise FileNotFoundError(fn)
    for i in argList:
        if not isinstance(i, (int, str)):
            raise TypeError(argList)
        if isinstance(i, str) and not i.isdigit():
            raise TypeError(argList)
    cmd = AWK_CMD.safe_substitute(delim = delim,
                                  vargs = ','.join(map(lambda x:'$' + str(x), argList)),
                                  files = ' '.join(fileList))
    if __debug__:
        print(cmd)
    return OSCommand.popen(cmd)


def readFromAWK(fileList:list, argList:list, delim=' ')->iter:
    """ call awk command and return an iterator for reading the output of awk """
    reader = openAWK(fileList, argList, delim).stdout
    if reader.readable() and not reader.closed:
        return iter(reader)
    else:
        raise RuntimeError('openAWK failed')


def readFromAwkWithCallback(callback, fileList:list, argList:list, delim=' ')->iter:
    """ call awk command and return an iterator of callback's return for each line in output """
    reader = openAWK(fileList, argList, delim).stdout
    if reader.readable() and not reader.closed:
        return map(callback, reader)
    else:
        raise RuntimeError('openAWK failed')


def test(name):
    d = {}
    for line in readFromAWK([name, name, name], [2, 3]):
        for s in line.split():
            try: d[s] += 1
            except: d[s] = 1
    for k, v in d.items():
        assert v >= 3
    print('test OK')


if __name__ == '__main__':
    test(__file__)





