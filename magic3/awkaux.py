# -*- coding:utf-8 -*-
# author : cypro666
# date   : 2016-03-08
from os.path import exists
from io import BufferedReader
from string import Template
from magic3.system import OSCommand

AWK_CMD = Template("""awk -F "$delim" '{print $vargs}' $files 2>&1""")

def open_awk(filelist:list, pos_args:list, delim)->BufferedReader:
    """ call awk command and return an opened pipe for read the output of awk, eg:
        for line in open_awk([file1, file2], [2,3,4], ',').stdout:
            print(line)   
    """
    if isinstance(delim, (bytes, bytearray)):
        delim = str(delim, 'utf-8')
    if not filelist:
        raise ValueError('filelist')
    if not pos_args:
        raise ValueError('pos_args')
    if len(delim) != 1:
        raise ValueError('delim must be a length 1 char')
    for fn in filelist:
        if not exists(fn):
            raise FileNotFoundError(fn)
    for i in pos_args:
        if not isinstance(i, (int, str)):
            raise TypeError(pos_args)
        if isinstance(i, str) and not i.isdigit():
            raise TypeError(pos_args)
    cmd = AWK_CMD.safe_substitute(delim = delim,
                                  vargs = ','.join(map(lambda x:'$' + str(x), pos_args)),
                                  files = ' '.join(filelist))
    if __debug__:
        print(cmd)
    return OSCommand.popen(cmd)


def read_from_awk(filelist:list, pos_args:list, delim=' ')->iter:
    """ call awk command and return an iterator for reading the output of awk """
    reader = open_awk(filelist, pos_args, delim).stdout
    if reader.readable() and not reader.closed:
        return iter(reader)
    else:
        raise RuntimeError('open_awk failed')


def read_from_awk_with_callback(callback, filelist:list, pos_args:list, delim=' ')->iter:
    """ call awk command and return an iterator of callback's return for each line in output """
    reader = open_awk(filelist, pos_args, delim).stdout
    if reader.readable() and not reader.closed:
        return map(callback, reader)
    else:
        raise RuntimeError('open_awk failed')


def test(name):
    d = {}
    for line in read_from_awk([name, name, name], [2, 3]):
        for s in line.split():
            try: d[s] += 1
            except: d[s] = 1
    for k, v in d.items():
        assert v >= 3
    print('test OK')


if __name__ == '__main__':
    test(__file__)





