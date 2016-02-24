# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import io, re, os
import asyncio, threading, queue
from random import randint
try:
    import pycurl, requests
except ImportError:
    raise ImportError('magic3.crawler depends on pycurl and requests library')
from magic3.utils import debug

# default http request timeout in seconds
requestTimeout = 30

# max number threads for CurlCrawler
maxThreads = 256

# regex for checking a URL is valid or not 
xValidURL = re.compile("^https?://\\w+[^\\s]*$(?ai)")

# default cookie filename 
cookieFileName = os.path.expanduser('~') + os.sep + 'cookie.tmp.txt'

def set_cookie_filename(fname:str):
    """ ensure fn is a correct filename with full path """
    global cookieFileName
    cookieFileName = fname

# User-Agent optional
userAgents = (
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:29.0) Gecko/20100101 Firefox/29.0',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.91 Safari/537'
)

# default http header for curl
curlHeader = [
    'User-Agent: ' + userAgents[randint(0, len(userAgents)-1)],
    'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0',
    'Accept-Language: zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3',
    'Accept-Encoding: gzip, deflate',
    'Connection: keep-alive'
]

# default http header for requests
requestsHeader = {k:v for k,v in (s.split(': ') for s in curlHeader)}

class Response(object):
    """ as the result type of all fetchers below """
    __slots__ = ('url', 'header', 'content')
    def __init__(self, url, header, content, decoder=None):
        """ note `header` means http-head, not html-head, always bytes """
        super().__init__()
        self.url = url
        self.header = header
        self.content = decoder(content) if decoder else content

    def __len__(self):
        return len(self.header) + len(self.content)

    def size(self):
        return (len(self.header), len(self.content))

# this obj should be in global, do Not change these settings
globalCURLShare = pycurl.CurlShare() 
globalCURLShare.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)
globalCURLShare.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)
usingCurlShare = False

class CurlFetcher(object):
    """ fetch urls using pycurl, a wrapper of Curl object """
    def __init__(self):
        """ `url` should be a string match standard URL """
        self._response_header = bytearray()
        self._curl = pycurl.Curl()
        self._curl.setopt(pycurl.HEADERFUNCTION, self._curl_header_function)
        if usingCurlShare:
            self._curl.setopt(pycurl.SHARE, globalCURLShare)
        self.setopt()
        
    def _curl_header_function(self, buf):
        """ append new http head lines """
        self._response_header += buf
        return len(buf)
    
    def setopt(self, header = curlHeader, max_redirect = 9, follow_location = 1, forbid_reuse = 1, connect_timeout = requestTimeout):
        """ set options for CURL, do Not change default options if not sure the meaning """
        setopt = self._curl.setopt
        setopt(pycurl.VERBOSE, 0)
        setopt(pycurl.HTTPHEADER, header)
        setopt(pycurl.FOLLOWLOCATION, follow_location)
        setopt(pycurl.MAXREDIRS, max_redirect)
        setopt(pycurl.FORBID_REUSE, forbid_reuse)
        setopt(pycurl.CONNECTTIMEOUT, requestTimeout)
        setopt(pycurl.TIMEOUT, requestTimeout * 2)
        setopt(pycurl.NOSIGNAL, 1) 
        setopt(pycurl.DNS_CACHE_TIMEOUT, 3600) 
        setopt(pycurl.ENCODING, 'gzip')        
        setopt(pycurl.SSL_VERIFYHOST, 0)
        setopt(pycurl.COOKIEJAR, cookieFileName)
        setopt(pycurl.COOKIEFILE, cookieFileName)
    
    def get(self, url, callback=None)->Response:
        """ fetch url using 'GET' method """
        if not xValidURL.match(url):
            raise ValueError('CurlFetcher: {u} is not correct'.format(u=url))
        self._response_header.clear()
        try:
            bio = io.BytesIO()
            self._curl.setopt(pycurl.HTTPGET, 1)
            self._curl.setopt(pycurl.URL, url)
            self._curl.setopt(pycurl.WRITEFUNCTION, bio.write)
            self._curl.perform()
            statcode = self._curl.getinfo(pycurl.HTTP_CODE)
            if __debug__ and statcode != 200:
                debug('{c}: {u}...'.format(c=statcode, u=url))
        except Exception as e:
            debug('{e}: {u}'.format(e=str(e), u=url))
            return None
        if callback:
            return callback(Response(url, bytes(self._response_header), bio.getvalue()))
        else:
            return Response(url, bytes(self._response_header), bio.getvalue())
    
    def close(self):
        """ release resource """
        self._curl.close()


class CrawlerTimeoutError(RuntimeError):
    """ when raised, crawler had waited for a long time """ 
    pass

class CurlCrawler(threading.Thread):
    """ crawler based on multithreads and pycurl """
    def __init__(self, urls:list, max_threads:int, callback):
        """ `max_threads` should not be too large, less than 50 is always enough
            `callback` is a user function with Response as the unique argument  """
        assert isinstance(urls, (list, tuple, set))
        assert max_threads >= 1
        self._init(urls, max_threads, callback)

    def _init(self, urls, max_threads, callback):
        self._results = []
        self._callback = callback
        if max_threads > maxThreads:
            max_threads = maxThreads
        self._fetchers = [
            threading.Thread(target=self._dispatch, args=(CurlFetcher(),)) for i in range(max_threads)
        ]
        self._queue = queue.Queue(len(urls) + len(self._fetchers))
        for url in urls:
            if not xValidURL.match(url):
                raise ValueError('CurlCrawler: {0} is not correct'.format(url))
            self._queue.put(url)
        for i in range(len(self._fetchers)):
            self._queue.put(None)

    def results(self)->list:
        """ return depends on whether `callback` suplied in __init__ method """
        return self._results

    def _dispatch(self, curl):
        """ fetch url by `curl` in different threads """
        count = 0
        while True:
            try:
                url = self._queue.get()
                if url == None:
                    raise queue.Empty
                if self._callback:
                    self._results.append(curl.get(url, self._callback))
                else:
                    self._results.append(curl.get(url))
                count += 1
            except queue.Empty:
                break
            finally:
                self._queue.task_done()
        if __debug__:
            debug('{t} finished {n}'.format(t=threading.current_thread().name, n=count))

    def finished(self)->bool:
        """ if all finished return True """
        if not self._queue.empty():
            return False
        return not any(self._fetchers)
    
    def start(self, timeout):
        """ start to run, raise if still running after `timeout` seconds """
        def _timer(fetchers):
            threading.Event().wait(timeout)
            if any(fetchers):
                raise CrawlerTimeoutError
        timer = threading.Thread(target=_timer, args=(self._fetchers,))
        timer.daemon = True
        timer.start()
        for t in self._fetchers:
            t.start()
        self._queue.join()
        for i in range(len(self._fetchers)):
            self._fetchers[i].join()
            self._fetchers[i] = None
        debug('CurlCrawler finished!')


class AsyncioCrawler(object):
    """ crawler based on requests and asyncio """
    def __init__(self, urls:list, callback, event_loop=None):
        """ `event_loop` should be got from asyncio
            `callback` is a user function with Response as the unique argument
        """
        assert isinstance(urls, (list, tuple, set))
        self._init(urls, callback, event_loop)

    def _init(self, urls, callback, event_loop):
        if event_loop:
            self._loop = event_loop
        else:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        self._results = []
        self._callback = callback
        for url in urls:
            if not xValidURL.match(url):
                raise ValueError('CurlCrawler: {0} is not correct'.format(url))
        self._urls = tuple(urls)
        self._count = len(self._urls)

    def results(self)->list:
        """ return depends on whether `callback` suplied in __init__ method """
        return self._results

    @asyncio.coroutine
    def _wrapped_callback(self, r):
        """ user's callback may be not a coroutine, so wrap it! """
        return self._callback(r)

    @asyncio.coroutine
    def _fetch(self, url):
        return requests.get(url=url, headers=requestsHeader, timeout=requestTimeout)

    @asyncio.coroutine
    def _dispatch(self, url):
        """ fetch an url asynchronous """
        self._count -= 1
        ret = None
        try:
            r = yield from self._fetch(url)
            r.headers['status'] = '{0} {1}'.format(r.status_code, r.reason)
            if self._callback:
                ret = yield from self._wrapped_callback(Response(url, r.headers, r.content))
            else:
                ret = Response(url, r.headers, r.content)
            r.close()
        except Exception as err:
            debug('{e}: {u}'.format(e=str(err), u=url))
        self._results.append(ret)

    def finished(self):
        """ if all commited return True """
        return not self._count

    def stop(self):
        """ stop event loop forcely """
        try:
            self._loop.stop()
        except RuntimeError:
            pass
    
    def start(self, timeout):
        """ start to run, raise if still running after `timeout` seconds """
        def _timer(loop):
            threading.Event().wait(timeout)
            if not loop.is_closed():
                raise CrawlerTimeoutError
        timer = threading.Thread(target=_timer, args=(self._loop,))
        timer.daemon = True
        timer.start()
        tasks = [self._dispatch(url) for url in self._urls]
        try:
            self._loop.run_until_complete(asyncio.wait(tasks))
        finally:
            self._loop.close()
        debug('AsyncioCrawler finished!')
        assert self._count == 0


def test(urls):
    ''' simple test for crawler '''
    def user_callback(r):
        debug(r.url, end=' ')
        if isinstance(r.header, bytes):
            debug(r.header.split(b'\r\n')[0], len(r.content))
        else:
            debug(r.header['status'], len(r.content))
    debug('CurlCrawler')
    crawler = CurlCrawler(urls, 3, user_callback)
    crawler.start(30)
    debug('AsyncioCrawler')
    crawler = AsyncioCrawler(urls, user_callback)
    crawler.start(30)


if __name__ == '__main__':
    urls = [ 'http://www.sina.com.cn', 
             'http://www.csdn.net', 
             'http://www.baidu.com', 
             'http://www.oschina.net', 
             'http://www.sohu.com', 
             'http://www.tmall.com',
             'http://www.sina.com.cn', 
             'http://www.csdn.net', 
             'http://www.baidu.com', 
             'http://www.oschina.net', 
             'http://www.sohu.com', 
             'http://www.tmall.com'  ]
    test(urls)


