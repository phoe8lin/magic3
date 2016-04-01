# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import io, re, os
import asyncio
try:
    import aiohttp
except ImportError:
    raise ImportError('magic3.crawler depends on aiohttp library')
from magic3.utils import *

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

    def key(self, url)->str:
        if url.startswith('http://') or url.startswith('https://'):
            return url.split(self._delim, 2)[-1]
        else:
            return url.split(self._delim, 1)[0]

    def value(self, url)->str:
        return self._cache[self.key(url)]

    def decode(self, url, html, errors='replace')->bytes:
        """ decode bytes in html specified in <meta> tag """
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
    """ as the result type of all fetchers below """
    __slots__ = ('url', 'header', 'content')
    def __init__(self, url, header, content, decoder=None):
        self.url = url
        self.header = header
        self.content = decoder(content) if decoder else content

    def __len__(self):
        return len(self.header) + len(self.content)

    def size(self):
        return (len(self.header), len(self.content))


if PythonVersion >= (3, 5):
    async def fetch_with_session(session, callback, url, **options):
        """ default options:
            params = None
            data = None 
            allow_redirects = True
            max_redirects = 5
        """
        if not validURL.match(url):
            raise ValueError(url)
        if not options:
            options['allow_redirects'] = True
            options['max_redirects'] = 5
        with aiohttp.Timeout(10):
            async with session.get(url, headers=httpHeader, **options) as r:
                content = await r.content.read()
        return callback(Response(url, dict(r.headers), content))
    
    async def fetch(callback, url, **options):
        """ default options:
            params = None
            data = None 
            allow_redirects = True
            max_redirects = 5
        """
        with aiohttp.ClientSession() as session:
            return await fetch_with_session(session, callback, url, **options)
    
    async def fetch_multi_with_session(session, callback, urls:list, **options):
        """ fetch more than one url by specific session """
        fetchers = [ fetch_with_session(session, callback, url, **options) for url in urls ]
        return await asyncio.tasks.wait(fetchers)
    
    async def fetch_multi(callback, urls:list, **options):
        """ fetch more than one url, see fetch above """
        fetchers = [ fetch(callback, url, **options) for url in urls ]
        return await asyncio.tasks.wait(fetchers)

else:   # PythonVersion < (3, 5):
    @asyncio.coroutine
    def fetch_with_session(session, callback, url, **options):
        """ default options:
            params = None
            data = None 
            allow_redirects = True
            max_redirects = 5
        """
        if not validURL.match(url):
            raise ValueError(url)
        if not options:
            options['allow_redirects'] = True
            options['max_redirects'] = 5
        with aiohttp.Timeout(10):
            r = yield from session.get(url, headers=httpHeader, **options)
            try:
                content = yield from r.content.read()
            finally:
                r.close()
        return callback(Response(url, dict(r.headers), content))
    
    @asyncio.coroutine
    def fetch(callback, url, **options):
        """ default options:
            params = None
            data = None 
            allow_redirects = True
            max_redirects = 5
        """
        with aiohttp.ClientSession() as session:
            ret = yield from fetch_with_session(session, callback, url, **options)
            return ret
    
    @asyncio.coroutine
    def fetch_multi_with_session(session, callback, urls:list, **options):
        """ fetch more than one url by specific session """
        fetchers = [ fetch_with_session(session, callback, url, **options) for url in urls ]
        ret = yield from asyncio.tasks.wait(fetchers)
        return ret
    
    @asyncio.coroutine
    def fetch_multi(callback, urls:list, **options):
        """ fetch more than one url, see fetch above """
        fetchers = [ fetch(callback, url, **options) for url in urls ]
        ret = yield from asyncio.tasks.wait(fetchers)
        return ret


def test_fetch():
    aio_run(
        fetch(lambda r : print(r.url, len(r.content)), 'https://www.taobao.com'),
        fetch(lambda r : print(r.url, len(r.content)), 'http://www.csdn.net'),
        fetch(lambda r : print(r.url, len(r.content)), 'http://git.oschina.net')
    )

def test_dcache():
    cc = CharsetCache()
    callback = lambda r : print(re.search("<title>.+?</title>(?ai)", cc.decode(r.url, r.content)).group())
    urls = ['https://www.baidu.com', 
            'http://www.sina.com.cn', 
            'http://www.163.com']
    aio_run(fetch_multi(callback, urls))
    print(cc._cache)
    

if __name__ == '__main__':
    test_fetch()
    test_dcache()


