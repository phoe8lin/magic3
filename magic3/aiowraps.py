# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import asyncio, concurrent
import threading
import functools
import time

def aio_new(executor=None):
    ''' Get new event loop with ThreadPoolExecutor '''
    loop = asyncio.new_event_loop()

    if executor:
        loop.set_default_executor(executor)
    else:
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor())

    return loop

def aio_loop():
    ''' Get current event loop '''
    return asyncio.get_event_loop()

def aio_tasks(*future):
    ''' Wrap more futures as one waitable future '''
    assert isinstance(future, (list, tuple))
    return asyncio.tasks.wait(future);

def aio_run(*future, loop=None):
    ''' Run until complete '''
    if not loop:
        loop = aio_loop()

    if len(future) > 1:
        return loop.run_until_complete(aio_tasks(*future))
    else:
        return loop.run_until_complete(future[0])

def aio_to_future(coro, loop=None):
    ''' Make coroutine to asyncio.Future '''
    return asyncio.ensure_future(coro, loop=loop)

def aio_call_soon(function, *args, loop=None, **kwargs):
    ''' Arrange for a callback to be called as soon as possible
        Callbacks are called in the order in which they are registered
        Each callback will be called exactly once '''

    if not loop:
        loop = aio_loop()

    loop.call_soon(functools.partial(function, *args, **kwargs))

def aio_call_soon_safe(function, *args, loop=None, **kwargs):
    ''' Like aio_call_soon, but multi threads safe '''

    if not loop:
        loop = aio_loop()

    loop.call_soon_threadsafe(functools.partial(function, *args, **kwargs))

def aio_call_later(delay, function, *args, loop=None, **kwargs):
    ''' Arrange for a callback to be called at a given time 
        Return a handler with cancel method that can be used to cancel the call 
        The delay can be an int or float in seconds which always relative to the current time '''

    if not loop:
        loop = aio_loop()

    loop.call_later(delay, functools.partial(function, *args, **kwargs))

def aio_at(aiotime, function, *args, loop=None, **kwargs):
    ''' Like call_later(), but uses an absolute time which 
        corresponds to the event loop's time() method '''

    if not loop:
        loop = aio_loop()

    loop.call_at(aiotime, functools.partial(function, *args, **kwargs))


def make_aio_thread(new=False, daemon=True, name=None):
    ''' Make a pair of asyncio loop and run_forever thread '''
    if new:
        loop = aio_new()
    else:
        loop = aio_loop()

    aiothread = threading.Thread(target=loop.run_forever, name=name)
    aiothread.daemon = daemon
    aiothread.start()

    return (loop, aiothread)


def test():
    d = {}
    
    def userfunc(arg, **kwargs):
        d[arg] = kwargs['a']

    aio_call_soon(userfunc, 111, a=111)

    aio_call_soon_safe(userfunc, 222, a=222)

    aio_call_later(1, userfunc, 333, a=333)

    aio_at(aio_loop().time() + 2, userfunc, 444, a=444)

    loop, aioth = make_aio_thread()
    time.sleep(2)

    for i in range(1, 5):
        assert d[i * 111] == i * 111

    print(__file__, ': Test OK')


if __name__ == '__main__':
    test()


