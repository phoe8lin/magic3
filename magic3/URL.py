# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import re
try:
    import requests
    from tornado.netutil import Resolver
except ImportError:
    raise ImportError('magic3.crawler depends on tornado and requests library')
from urllib.parse import urlparse, unquote_plus

class URLMacher:
    """ url/uri regex and compiled """
    pattern = "((http|ftp|https)://[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?)"
    pattern2 = b"((http|ftp|https)://[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?)"
    compiled = re.compile(pattern, re.S)
    compiled2 = re.compile(pattern2, re.S)

def ExtractURLFromText(text:'str or bytes')->frozenset:
    """ extract all url unduplicate in text """
    if isinstance(text, str):
        return set(i.group(0) for i in list(URLMacher.compiled.finditer(text)))
    elif isinstance(text, bytes):
        return set(i.group(0) for i in list(URLMacher.compiled2.finditer(text)))
    raise ValueError('ExtractURLFromText')

def AddHttp(uri):
    """ add 'http://' prefix to url """
    return uri if uri[:7] == 'http://' else 'http://' + uri

def AddHttp2(uri):
    """ add 'http://' prefix to url """
    return uri if uri[:7] == b'http://' else b'http://' + uri

def TrimHttp(uri):
    """ strip 'http://' prefix to url """
    return uri if uri[:7] != 'http://' else uri[7:]

def TrimHttp2(uri):
    """ strip 'http://' prefix to url """
    return uri if uri[:7] != b'http://' else uri[7:]

def URLPath(url:str)->str:
    """ get url's path(strip params) """
    return url.split('?', 1)[0]

def URLPath2(url:bytes)->bytes:
    """ get url's path(strip params) """
    return url.split(b'?', 1)[0]

def URLHost(url:str)->str:
    """ get url's host(domain name) """
    return TrimHttp(url).split('/', 1)[0]

def URLHost2(url:bytes)->bytes:
    """ get url's host(domain name) """
    return TrimHttp2(url).split(b'/', 1)[0]

def URLSplit(url:str)->(str,str,str):
    """ split `url` to 3 part: raw url, url without params, url domain """
    url = TrimHttp(url)
    seps = url.split('?', 1)
    host = seps[0].split('/', 1)[0]
    if len(seps) > 1:
        return host, seps[0], seps[1]
    else:
        return host, seps[0], ''

def URLSplit2(url:bytes)->(bytes,bytes,bytes):
    """ split `url` to 3 part: raw url, url without params, url domain """
    url = TrimHttp2(url)
    seps = url.split(b'?', 1)
    host = seps[0].split(b'/', 1)[0]
    if len(seps) > 1:
        return host, seps[0], seps[1]
    else:
        return host, seps[0], b''

def UnQuote(s, errors='strict')->str:
    """ unquote url or others strictly, try step:
        first  'utf-8'
        second 'gbk'
        last   'latin-1'
        return None if failed """
    for c in ('utf-8', 'gbk', 'latin-1'):
        try:    
            return unquote_plus(s, c, errors)
        except: 
            continue
    return None


class URLString(str):
    """ a str wrapper, has more supports of URL """ 
    __slots__ = ('parsed', 'solver')    
    def __new__(cls, s):
        """ new hook """
        return str.__new__(cls, s)
    
    def __init__(self, s):
        super().__init__()
        self.parsed = urlparse(self)
        self.solver = Resolver()
    
    @classmethod
    def ConfigSolver(cls, solver_type='tornado.netutil.BlockingResolver'):
        Resolver.configure(solver_type)

    @property
    def Resolved(self)->list:
        """ DNS resolve """
        return self.solver.resolve(self.parsed.netloc, port=80).result()
    
    def HEAD(self, **kwargs)->bytes:
        """ http HEAD method """
        assert self.parsed.scheme
        return requests.head(self, **kwargs).content
    
    def GET(self, **kwargs)->bytes:
        """ http GET method """
        assert self.parsed.scheme
        return requests.get(self, **kwargs).content
    
    def POST(self, data=dict(), **kwargs)->bytes:
        """ http POST method """
        assert self.parsed.scheme
        return requests.post(self, data = data, **kwargs).content
    
    def __getattr__(self, attr):
        """ get attributes support """
        return self.parsed.__getattribute__(attr)


def test():
    assert(b'www.baidu.com' == URLHost2(b'www.baidu.com'))
    assert(b'http://www.baidu.com/s' == URLPath2(b'http://www.baidu.com/s'))
    h, p, q = URLSplit2(b'www.baidu.com/s?ie=utf-8&wd=123')
    assert(b'ie=utf-8&wd=123' == q)
    text = b"""<html>
    <p>URL</p><p> http://www.sina.com.cn </p><p>URL</p>
    </html>"""
    for url in ExtractURLFromText(text):
        print(url)
        s = URLString(url.decode())
        print(s.Resolved[0], s.scheme)
    print('OK')

if __name__ == '__main__':
    test()

