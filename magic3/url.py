# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import re
from urllib.parse import urlparse, unquote_plus

try:
    import requests
    from tornado.netutil import Resolver
except ImportError:
    raise ImportError('magic3.crawler depends on tornado and requests library')


DEFAULT_HTTP_HEADER = {
    'User-Agent'      : 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0',
    'Accept'          : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0',
    'Accept-Language' : 'zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3',
    'Accept-Encoding' : 'gzip, deflate',
    'Connection'      : 'keep-alive'
}


class CharsetCache(object):
    ''' A simple cache for decoding http content(html) from charset in <meta> tag '''

    def __init__(self, schema='host'):
        ''' Schema can be 'host' or 'path' '''
        if schema.lower() == 'host':
            self._delim = '/'
        elif schema.lower() == 'path':
            self._delim = '?'

        self._cache = dict()
        self._charset = re.compile(b"<meta.+?charset(?:\\W+)(?P<code>[-_0-9a-zA-Z]+).*?>", re.IGNORECASE)

    def key(self, url) -> str:
        if url.startswith('http://') or url.startswith('https://'):
            return url.split(self._delim, 2)[-1]
        else:
            return url.split(self._delim, 1)[0]

    def value(self, url) -> str:
        return self._cache[self.key(url)]

    def decode(self, url, html, errors='replace') -> bytes:
        ''' Decode bytes in html specified in <meta> tag '''
        try:
            return html.decode(self.value(url), errors=errors)
        except KeyError:
            code = self._charset.search(html).group('code').decode('utf-8')
            code = code.lower()
            self._cache[self.key(url)] = code
            return html.decode(code, errors=errors)

    def clear(self):
        self._cache.clear()


class Response(object):
    ''' As the result type of all fetchers below '''
    __slots__ = ('url', 'header', 'content')

    def __init__(self, url, header, content, decoder=None):
        self.url = url
        self.header = header
        self.content = decoder(content) if decoder else content

    def __len__(self):
        return len(self.header) + len(self.content)

    def size(self):
        return (len(self.header), len(self.content))


class URLMacher:
    ''' URL/URI regex and compiled '''

    pattern = "((http|ftp|https)://[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?)"
    pattern2 = b"((http|ftp|https)://[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?)"

    compiled = re.compile(pattern, re.S)
    compiled2 = re.compile(pattern2, re.S)


def extract_url_from_text(text:'str or bytes') -> frozenset:
    ''' Extract all url unduplicate in text '''
    if isinstance(text, str):
        return set(i.group(0) for i in list(URLMacher.compiled.finditer(text)))
    elif isinstance(text, bytes):
        return set(i.group(0) for i in list(URLMacher.compiled2.finditer(text)))
    raise ValueError('extract_url_from_text')


def addhttp(uri):
    ''' Add 'http://' prefix to url '''
    return uri if uri[:7] == 'http://' else 'http://' + uri

def addhttp2(uri):
    ''' Add 'http://' prefix to url '''
    return uri if uri[:7] == b'http://' else b'http://' + uri

def trimhttp(uri):
    ''' Strip 'http://' prefix to url '''
    return uri if uri[:7] != 'http://' else uri[7:]

def trimhttp2(uri):
    ''' Strip 'http://' prefix to url '''
    return uri if uri[:7] != b'http://' else uri[7:]

def urlpath(url:str) -> str:
    ''' Get url's path(strip params) '''
    return url.split('?', 1)[0]

def urlpath2(url:bytes) -> bytes:
    ''' Get url's path(strip params) '''
    return url.split(b'?', 1)[0]

def urlhost(url:str) -> str:
    ''' Get url's host(domain name) '''
    return trimhttp(url).split('/', 1)[0]

def urlhost2(url:bytes) -> bytes:
    ''' Get url's host(domain name) '''
    return trimhttp2(url).split(b'/', 1)[0]

def urlsplit(url:str) -> (str, str, str):
    ''' Split `url` to 3 part: raw url, url without params, url domain '''
    url = trimhttp(url)
    seps = url.split('?', 1)
    host = seps[0].split('/', 1)[0]
    if len(seps) > 1:
        return host, seps[0], seps[1]
    else:
        return host, seps[0], ''

def urlsplit2(url:bytes) -> (bytes, bytes, bytes):
    ''' Split `url` to 3 part: raw url, url without params, url domain '''
    url = trimhttp2(url)
    seps = url.split(b'?', 1)
    host = seps[0].split(b'/', 1)[0]
    if len(seps) > 1:
        return host, seps[0], seps[1]
    else:
        return host, seps[0], b''


def try_unquote(s, errors='strict') -> str:
    ''' Unquote url or others strictly, try step:
        first  'utf-8'
        second 'gbk'
        last   'latin-1'
        return None if failed '''

    for c in ('utf-8', 'gbk', 'latin-1'):
        try:
            return unquote_plus(s, c, errors)
        except:
            continue

    return None


class URLString(str):
    ''' A str wrapper, has more supports of URL '''
    __slots__ = ('parsed', 'solver')

    def __new__(cls, s):
        ''' New hook '''
        return str.__new__(cls, s)

    def __init__(self, s):
        super().__init__()
        self.parsed = urlparse(self)
        self.solver = Resolver()

    @classmethod
    def config_solver(cls, solver_type='tornado.netutil.BlockingResolver'):
        Resolver.configure(solver_type)

    @property
    def resolve(self) -> list:
        ''' DNS resolve '''
        return self.solver.resolve(self.parsed.netloc, port=80).result()

    def HEAD(self, **kwargs) -> bytes:
        ''' http HEAD method '''
        assert self.parsed.scheme
        return requests.head(self, **kwargs).content

    def GET(self, **kwargs) -> bytes:
        ''' HTTP GET method '''
        assert self.parsed.scheme
        return requests.get(self, **kwargs).content

    def POST(self, data=dict(), **kwargs) -> bytes:
        ''' HTTP POST method '''
        assert self.parsed.scheme
        return requests.post(self, data=data, **kwargs).content

    def __getattr__(self, attr):
        ''' Get attributes support '''
        return self.parsed.__getattribute__(attr)


def test():
    assert(b'www.baidu.com' == urlhost2(b'www.baidu.com'))
    assert(b'http://www.baidu.com/s' == urlpath2(b'http://www.baidu.com/s'))

    h, p, q = urlsplit2(b'www.baidu.com/s?ie=utf-8&wd=123')

    assert(b'ie=utf-8&wd=123' == q)

    text = b'''<html>
    <p>URL</p><p> http://www.sina.com.cn </p><p>URL</p>
    </html>'''

    for url in extract_url_from_text(text):
        print(url)
        s = URLString(url.decode())
        print(s.resolve[0], s.scheme)

    print('OK')

if __name__ == '__main__':
    test()

