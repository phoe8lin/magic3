# -*- coding:utf-8 -*-
# author : cypro666
# date   : 2016.03.31
import sys, argparse

class OptionParser(object):
    """ 
    This is a simple wrapper of argparse.ArgumentParser for easier using
    Example:
        opts = OptionParser()\
        .add('-f', '--file', type=str, metavar='FILE', help='user file')\
        .add('-n', '--num',  type=int, metavar='NUM',  default=321, help='how many times')\
        .parse(sys.argv)\
        .options()
        print(opts)
    """
    def __init__(self, usage='', description='', note=''):
        """ note is a epilog info after usage and description """
        if not usage:
            usage = 'python3 %s [OPTIONS]' % sys.argv[0]
        self._parser = argparse.ArgumentParser(usage=usage, description=description, epilog=note)
        self._option = None
        
    def PrintHelp(self, file=sys.stdout):
        """ print standard help informations to file """
        self._parser.print_help(file)
        print(file=file)
        
    def Add(self, *args, **kwargs):
        """ kwargs: type, metavar, default, help ... """
        assert kwargs
        self._parser.add_argument(*args, **kwargs)
        return self
    
    def Parse(self, argv=sys.argv, exit=True):
        """ parse argv[1:], call sys.exit() if `exit` is True """
        if len(argv) < 2:
            self.print_help()
            if exit:
                sys.exit()
            return self
        else:
            self._option = self._parser.parse_args(argv[1:])
            return self
    
    def Options(self):
        """ return a dict of parsed options """
        return dict(self._option.__dict__)



def test():
    OptionParser().PrintHelp()
    opts = OptionParser()\
    .Add('-f', '--file', type=str, metavar='FILE', help='user file')\
    .Add('-n', '--num',  type=int, metavar='NUM',  default=321, help='how many times')\
    .Parse([__file__, '-f', 'myfile.txt'])\
    .Options()
    assert opts['file'] == 'myfile.txt'
    assert opts['num'] == 321


if __name__ == '__main__':
    test() 


