#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# author : cypro666
# date   : 2014.12.22
''' run `python3 setup.py install` to install this module '''

from distutils.core import setup
from magic3 import __version__,__author__,__need__

print('Dependents:', str(__need__).strip('()'))

setup(name = "magic3",
      description = 'Basic Components on Python3 for Magic',
      long_description = 'Basic Components on Python3 for Magic, dependents: ' + str(__need__).strip('()'),
      version = __version__,
      author = __author__,
      author_email = 'cypro666@gmail.com',
      license = 'MIT License',
      keywords = ['web', 'http', 'database', 'text', 'utils'],
      platforms = 'Linux',
      packages = ['magic3'],
      download_url = 'https://github.com/cypro666/magic3',
      url='https://github.com/cypro666')

