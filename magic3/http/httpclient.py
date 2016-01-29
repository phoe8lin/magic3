#!/usr/bin/env python3
# -*- coding:utf-8 -*-
## author : cypro666
## date   : 2015.06.06
import re, time
import threading
try:    import select
except: pass
try:    from tornado import gen, queues, ioloop
except: raise ImportError('magic3.async.httpclinet needs tornado library!')
try:    from tornado import curl_httpclient
except: from tornado import httpclient
from magic3.common.utils import time_meter


class Delay(object):
    """ better sleeper for delay """
    @staticmethod
    def sleep(seconds):
        """ sleep for `seconds`, float or int number, working on linux only """
        try:
            select.select([],[],[],seconds)
        except:
            time.sleep(seconds)  # for windows
    @staticmethod
    def wait(seconds):
        """ like sleep, but called wait """
        threading.Event().wait(seconds)


class DecodeCache(object):
    """ a simple cache for decoding http content(html) """
    def __init__(self, schema='host'):
        """ schema can be 'host' or 'path' """
        if schema == 'host':
            self._delim = '/'
        elif schema == 'path':
            self._delim = '?'
        self._cache = dict()
        self._charset = re.compile(b"<meta.+?charset(?:\\W+)(?P<code>[-_0-9a-zA-Z]+).*?>")

    def decode(self, url, html):
        """ decode bytes in html specified in <meta> tag """
        assert url.startswith('http://')
        key = url[7:].split(self._delim)[0]
        try:
            return html.decode(self._cache[key], 'replace')
        except KeyError:
            code = self._charset.search(html).group('code').decode('ascii')
            self._cache[key] = code
            return html.decode(code, 'replace')
    
    def __call__(self, url, html):
        return self.decode(url, html)


class AsyncHttpClient(object):
    """ used for `AsyncClientPool` """
    __slots__ = ('_response', '_etime', '_client', '_url', '_options')
    
    def __init__(self, url, method='GET', timeout=30, follow_redirects=True, max_redirects=5):
        """ `method` only supports 'GET' and 'HEAD' now... """
        super().__init__()
        if method not in ('GET', 'HEAD'):
            raise ValueError('only support `GET` and `HEAD` method now')
        self._url = url
        self._response = None
        self._etime = time.time()
        self._options = { 'method'            : method,
                          'connect_timeout'   : timeout,
                          'follow_redirects'  : follow_redirects,
                          'max_redirects'     : max_redirects,
                          'user_agent'        : 'Mozilla/5.0 Gecko Firefox/29.0',
                          'use_gzip'          : True,
                          'allow_nonstandard_methods' : False }
        try:
            self._client = curl_httpclient.CurlAsyncHTTPClient()
        except:
            self._client = httpclient.AsyncHTTPClient()
            
    @property
    def html(self):
        return self._response.body
    
    @property
    def header(self):
        return self._response.headers
    
    @property
    def url(self):
        return self._url
    
    @property
    def code(self):
        return self._response.code
    
    @property
    def reason(self):
        return self._response.reason
    
    @property
    def elapsed(self):
        return round(self._etime, 4);
    
    @gen.coroutine
    def fetch(self):
        #if __debug__: print(self._url)
        try:
            self._response = yield self._client.fetch(self._url, **self._options)
            self._etime = time.time() - self._etime
        except Exception as e:
            print('Failed: %s %s' % (e, self._url))


class AsyncHttpClientPool(threading.Thread):
    """ pool of clients  """
    def __init__(self, num_concurrency, callback):
        """ num_concurrency should Not be too large, 4~8 are suitable values """
        threading.Thread.__init__(self)
        self._numco = num_concurrency
        self._callback = callback
        self._queue = queues.Queue()
        self._fetched = 0
        self._tostop = threading.Event()
        self._tostop.clear()

    @property
    def fetched(self):
        return self._fetched
    
    @property
    def elapsed(self):
        return round(self._etime, 4)
    
    @gen.coroutine
    def stop(self):
        self._tostop.set()
        self._etime = time.time() - self._etime

    @gen.coroutine
    def _callback_wrapper(self, client):
        return self._callback(client)

    @gen.coroutine
    def fetch(self, url):
        yield self._queue.put(AsyncHttpClient(url))
    
    @gen.coroutine
    def _fetch_url(self):
        current = yield self._queue.get()
        try:
            yield current.fetch()
            yield self._callback_wrapper(current)
        finally:
            self._fetched += 1
            self._queue.task_done()

    @gen.coroutine
    def _work(self):
        while True:
            yield self._fetch_url()

    @gen.coroutine
    def __call__(self):
        """ Start workers, then wait for the work queue to be empty """
        for i in range(self._numco):
            self._work()
        yield self._queue.join()

    def run(self):
        """ call start method to run this thread """
        self._etime = time.time()
        while True:
            if self._tostop.is_set():
                return
            io_loop = ioloop.IOLoop.current()
            io_loop.run_sync(self)
            Delay.wait(0.1)


@time_meter(__file__)
def test():
    d = DecodeCache()
    c = AsyncHttpClient("http://www.baidu.com/")
    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(c.fetch)
    print(c.url, c.code, c.reason, c.elapsed)
    
    def usercb(c):
        try:
            print(c.url, c.code, c.reason, len(d(c.url, c.html)), c.elapsed)
        except:
            print(c.url, c.code, c.reason)
    
    spider = AsyncHttpClientPool(10, usercb)
    spider.start()
    spider.fetch("http://www.baidu.com")
    spider.fetch("http://www.sina.com.cn")
    spider.fetch("http://www.sohu.com")
    spider.fetch("http://www.csdn.net")
    spider.fetch("http://www.163.com")
    spider.fetch("http://www.baidu.com")
    spider.fetch("http://www.sina.com.cn")
    spider.fetch("http://www.sohu.com")
    spider.fetch("http://www.csdn.net")
    spider.fetch("http://www.cnzz.com")
    while spider.fetched != 10:
        Delay.wait(0.5)
        print(spider.fetched)
    spider.stop()
    spider.join()
    print('all', spider.elapsed)


if __name__ == '__main__':
    """ """
    from magic3.common.utils import run_stdio_flusher
    run_stdio_flusher()
    test()

