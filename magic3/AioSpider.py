# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import io, re, os
import asyncio
try:
    import aiohttp
except ImportError:
    raise ImportError('magic3.crawler depends on aiohttp library')
from magic3.Utils import *

# regex for checking a URL is valid or not 
validURL = re.compile("^https?://\\w+[^\\s]*$(?ai)")

# default http header for curl
httpHeader = {
    'User-Agent'      : 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0',
    'Accept'          : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0',
    'Accept-Language' : 'zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3',
    'Accept-Encoding' : 'gzip, deflate',
    'Connection'      : 'keep-alive'
}

class CharsetCache(object):
    """ a simple cache for decoding http content(html) from charset in <meta> tag """
    def __init__(self, schema='host'):
        """ schema can be 'host' or 'path' """
        if schema.lower() == 'host':
            self._delim = '/'
        elif schema.lower() == 'path':
            self._delim = '?'
        self._cache = dict()
        self._charset = re.compile(b"<meta.+?charset(?:\\W+)(?P<code>[-_0-9a-zA-Z]+).*?>", re.IGNORECASE)

    def Key(self, url)->str:
        if url.startswith('http://') or url.startswith('https://'):
            return url.split(self._delim, 2)[-1]
        else:
            return url.split(self._delim, 1)[0]

    def Value(self, url)->str:
        return self._cache[self.Key(url)]

    def Decode(self, url, html, errors='replace')->bytes:
        """ decode bytes in html specified in <meta> tag """
        try:
            return html.decode(self.Value(url), errors=errors)
        except KeyError:
            code = self._charset.search(html).group('code').decode('utf-8')
            code = code.lower()
            self._cache[self.Key(url)] = code
            return html.decode(code, errors=errors)
    
    def Clear(self):
        self._cache.clear()


class Response(object):
    """ as the result type of all fetchers below """
    __slots__ = ('url', 'header', 'content')
    def __init__(self, url, header, content, decoder=None):
        self.url = url
        self.header = header
        self.content = decoder(content) if decoder else content

    def __len__(self):
        return len(self.header) + len(self.content)

    def Size(self):
        return (len(self.header), len(self.content))


async def FetchWithSession(method, session, callback, url, **options):
    """ default options:
        params = None
        data = None
        allow_redirects = True
        max_redirects = 5
    `method` should be GET/POST/HEAD
    """
    if not validURL.match(url):
        raise ValueError(url)
    if not options:
        options['allow_redirects'] = True
        options['max_redirects'] = 5
    if method == 'GET':
        method = session.get
    elif method == 'POST':
        method = session.post
    elif method == 'HEAD':
        method = session.head
    else:
        raise TypeError(method)
    with aiohttp.Timeout(10):
        async with method(url, headers=httpHeader, **options) as r:
            content = await r.content.read()
    return callback(Response(url, dict(r.headers), content))


async def Fetch(method, callback, url, **options):
    """ default options:
        params = None
        data = None 
        allow_redirects = True
        max_redirects = 5
    `method` should be GET/POST/HEAD
    """
    with aiohttp.ClientSession() as session:
        return await FetchWithSession(method, session, callback, url, **options)


async def FetchMultiWithSession(method, session, callback, urls:list, **options):
    """ fetch more than one url by specific session """
    fetchers = [ FetchWithSession(method, session, callback, url, **options) for url in urls ]
    return await asyncio.tasks.wait(fetchers)


async def FetchMulti(method, callback, urls:list, **options):
    """ fetch more than one url, see fetch above """
    fetchers = [ Fetch(method, callback, url, **options) for url in urls ]
    return await asyncio.tasks.wait(fetchers)


def TestFetch():
    AioRun(
        Fetch('GET', lambda r : print(r.url, len(r.content)), 'https://www.taobao.com'),
        Fetch('GET', lambda r : print(r.url, len(r.content)), 'http://www.csdn.net'),
        Fetch('GET', lambda r : print(r.url, len(r.content)), 'http://git.oschina.net')
    )

def TestDCache():
    cc = CharsetCache()
    callback = lambda r : print(re.search("<title>.+?</title>(?ai)", cc.Decode(r.url, r.content)).group())
    urls = ['https://www.baidu.com', 
            'http://www.sina.com.cn', 
            'http://www.163.com']
    AioRun(FetchMulti('GET', callback, urls))
    print(cc._cache)


if __name__ == '__main__':
    TestFetch()
    TestDCache()


