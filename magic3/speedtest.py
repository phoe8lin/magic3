# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import math
import time
import os
import sys
import threading
import re
import signal
import socket
__all__ = ['speedtest']

# Some global variables we use
source = None
shutdown_event = None

socket_socket = socket.socket

try:
    import xml.etree.cElementTree as ET
except ImportError:
    try:
        import xml.etree.ElementTree as ET
    except ImportError:
        from xml.dom import minidom as DOM
        ET = None

try:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError, URLError
except ImportError:
    from urllib.request import urlopen, Request, HTTPError, URLError

try:
    from queue import Queue
except ImportError:
    from queue import Queue

try:
    from urllib.parse import urlparse
except ImportError:
    from urllib.parse import urlparse

try:
    from urllib.parse import parse_qs
except ImportError:
    try:
        from urllib.parse import parse_qs
    except ImportError:
        from cgi import parse_qs

try:
    from hashlib import md5
except ImportError:
    from md5 import md5

try:
    from argparse import ArgumentParser as ArgParser
except ImportError:
    from optparse import OptionParser as ArgParser

def print_(*args, **kwargs):
    fp = kwargs.pop("file", sys.stdout)
    if fp is None:
        return

    def write(data):
        if not isinstance(data, str):
            data = str(data)
        fp.write(data)

    want_unicode = False
    sep = kwargs.pop("sep", None)
    if sep is not None:
        if isinstance(sep, str):
            want_unicode = True
        elif not isinstance(sep, str):
            raise TypeError("sep must be None or a string")
    end = kwargs.pop("end", None)
    if end is not None:
        if isinstance(end, str):
            want_unicode = True
        elif not isinstance(end, str):
            raise TypeError("end must be None or a string")
    if kwargs:
        raise TypeError("invalid keyword arguments to sys.stdout.write)")
    if not want_unicode:
        for arg in args:
            if isinstance(arg, str):
                want_unicode = True
                break
    if want_unicode:
        newline = str("\n")
        space = str(" ")
    else:
        newline = "\n"
        space = " "
    if sep is None:
        sep = space
    if end is None:
        end = newline
    for i, arg in enumerate(args):
        if i:
            write(sep)
        write(arg)
    write(end)


def boundSocket(*args, **kwargs):
    global source
    sock = socket_socket(*args, **kwargs)
    sock.bind((source, 0))
    return sock


def distance(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371  # km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2)) * math.sin(dlon / 2)
         * math.sin(dlon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return d


class FileGetter(threading.Thread):
    def __init__(self, url, start):
        self.url = url
        self.result = None
        self.starttime = start
        threading.Thread.__init__(self)

    def run(self):
        self.result = [0]
        try:
            if (time.time() - self.starttime) <= 10:
                f = urlopen(self.url)
                while 1 and not shutdown_event.isSet():
                    self.result.append(len(f.read(10240)))
                    if self.result[-1] == 0:
                        break
                f.close()
        except IOError:
            pass


def downloadSpeed(files, quiet=False):
    start = time.time()

    def producer(q, files):
        for file in files:
            thread = FileGetter(file, start)
            thread.start()
            q.put(thread, True)
            if not quiet and not shutdown_event.isSet():
                sys.stdout.write('.')
                sys.stdout.flush()

    finished = []

    def consumer(q, total_files):
        while len(finished) < total_files:
            thread = q.get(True)
            while thread.isAlive():
                thread.join(timeout=0.1)
            finished.append(sum(thread.result))
            del thread

    q = Queue(6)
    prod_thread = threading.Thread(target=producer, args=(q, files))
    cons_thread = threading.Thread(target=consumer, args=(q, len(files)))
    start = time.time()
    prod_thread.start()
    cons_thread.start()
    while prod_thread.isAlive():
        prod_thread.join(timeout=0.1)
    while cons_thread.isAlive():
        cons_thread.join(timeout=0.1)
    return (sum(finished) / (time.time() - start))


class FilePutter(threading.Thread):
    def __init__(self, url, start, size):
        self.url = url
        chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        data = chars * (int(round(int(size) / 36.0)))
        self.data = ('content1=%s' % data[0:int(size) - 9]).encode()
        del data
        self.result = None
        self.starttime = start
        threading.Thread.__init__(self)

    def run(self):
        try:
            if ((time.time() - self.starttime) <= 10 and
                    not shutdown_event.isSet()):
                f = urlopen(self.url, self.data)
                f.read(11)
                f.close()
                self.result = len(self.data)
            else:
                self.result = 0
        except IOError:
            self.result = 0


def uploadSpeed(url, sizes, quiet=False):
    start = time.time()

    def producer(q, sizes):
        for size in sizes:
            thread = FilePutter(url, start, size)
            thread.start()
            q.put(thread, True)
            if not quiet and not shutdown_event.isSet():
                sys.stdout.write('.')
                sys.stdout.flush()

    finished = []

    def consumer(q, total_sizes):
        while len(finished) < total_sizes:
            thread = q.get(True)
            while thread.isAlive():
                thread.join(timeout=0.1)
            finished.append(thread.result)
            del thread

    q = Queue(6)
    prod_thread = threading.Thread(target=producer, args=(q, sizes))
    cons_thread = threading.Thread(target=consumer, args=(q, len(sizes)))
    start = time.time()
    prod_thread.start()
    cons_thread.start()
    while prod_thread.isAlive():
        prod_thread.join(timeout=0.1)
    while cons_thread.isAlive():
        cons_thread.join(timeout=0.1)
    return (sum(finished) / (time.time() - start))


def getAttributesByTagName(dom, tagName):
    elem = dom.getElementsByTagName(tagName)[0]
    return dict(list(elem.attributes.items()))


def getConfig():
    uh = urlopen('http://www.speedtest.net/speedtest-config.php')
    configxml = []
    while True:
        configxml.append(uh.read(10240))
        if len(configxml[-1]) == 0:
            break
    if int(uh.code) != 200:
        return None
    uh.close()
    try:
        root = ET.fromstring(''.encode().join(configxml))
        config = {
            'client': root.find('client').attrib,
            'times': root.find('times').attrib,
            'download': root.find('download').attrib,
            'upload': root.find('upload').attrib}
    except AttributeError:
        root = DOM.parseString(''.join(configxml))
        config = {
            'client': getAttributesByTagName(root, 'client'),
            'times': getAttributesByTagName(root, 'times'),
            'download': getAttributesByTagName(root, 'download'),
            'upload': getAttributesByTagName(root, 'upload')}
    del root
    del configxml
    return config


def closestServers(client, all=False):
    uh = urlopen('http://www.speedtest.net/speedtest-servers.php')
    serversxml = []
    while True:
        serversxml.append(uh.read(10240))
        if len(serversxml[-1]) == 0:
            break
    if int(uh.code) != 200:
        return None
    uh.close()
    try:
        root = ET.fromstring(''.encode().join(serversxml))
        elements = root.getiterator('server')
    except AttributeError:
        root = DOM.parseString(''.join(serversxml))
        elements = root.getElementsByTagName('server')
    servers = {}
    for server in elements:
        try:
            attrib = server.attrib
        except AttributeError:
            attrib = dict(list(server.attributes.items()))
        d = distance([float(client['lat']), float(client['lon'])],
                     [float(attrib.get('lat')), float(attrib.get('lon'))])
        attrib['d'] = d
        if d not in servers:
            servers[d] = [attrib]
        else:
            servers[d].append(attrib)
    del root
    del serversxml
    del elements

    closest = []
    for d in sorted(servers.keys()):
        for s in servers[d]:
            closest.append(s)
            if len(closest) == 5 and not all:
                break
        else:
            continue
        break

    del servers
    return closest


def getBestServer(servers):
    results = {}
    for server in servers:
        cum = []
        url = os.path.dirname(server['url'])
        for i in range(0, 3):
            try:
                uh = urlopen('%s/latency.txt' % url)
            except (HTTPError, URLError):
                cum.append(3600)
                continue
            start = time.time()
            text = uh.read(9)
            total = time.time() - start
            if int(uh.code) == 200 and text == 'test=test'.encode():
                cum.append(total)
            else:
                cum.append(3600)
            uh.close()
        avg = round((sum(cum) / 3) * 1000000, 3)
        results[avg] = server

    fastest = sorted(results.keys())[0]
    best = results[fastest]
    best['latency'] = fastest

    return best


def ctrl_c(signum, frame):
    """Catch Ctrl-C key sequence and set a shutdown_event for our threaded
    operations
    """

    global shutdown_event
    shutdown_event.set()
    raise SystemExit('\nCancelling...')


def version():
    """Print the version"""
    return '1.0.0'


def speedtest():
    """ run net speed test """
    global shutdown_event, source
    shutdown_event = threading.Event()

    signal.signal(signal.SIGINT, ctrl_c)

    description = ()

    parser = ArgParser(description=description)
    try:
        parser.add_argument = parser.add_option
    except AttributeError:
        pass
    parser.add_argument('--bytes', dest='units', action='store_const',
                        const=('bytes', 1), default=('bits', 8),
                        help='Display values in bytes instead of bits. Does '
                             'not affect the image generated by --share')
    parser.add_argument('--share', action='store_true',
                        help='Generate and provide a URL to the speedtest.net '
                             'share results image')
    parser.add_argument('--simple', action='store_true',
                        help='Suppress verbose output, only show basic '
                             'information')
    parser.add_argument('--list', action='store_true',
                        help='Display a list of speedtest.net servers '
                             'sorted by distance')
    parser.add_argument('--server', help='Specify a server ID to test against')
    parser.add_argument('--mini', help='URL of the Speedtest Mini server')
    parser.add_argument('--source', help='Source IP address to bind to')
    parser.add_argument('--version', action='store_true',
                        help='Show the version number and exit')

    options = parser.parse_args()
    if isinstance(options, tuple):
        args = options[0]
    else:
        args = options
    del options

    if args.version:
        version()

    if args.source:
        source = args.source
        socket.socket = boundSocket

    if not args.simple:
        print_('Retrieving speedtest.net configuration...')
    try:
        config = getConfig()
    except URLError:
        print_('Cannot retrieve speedtest configuration')
        sys.exit(1)

    if not args.simple:
        print_('Retrieving speedtest.net server list...')
    if args.list or args.server:
        servers = closestServers(config['client'], True)
        if args.list:
            serverList = []
            for server in servers:
                line = ('%(id)4s) %(sponsor)s (%(name)s, %(country)s) '
                        '[%(d)0.2f km]' % server)
                serverList.append(line)
            try:
                str()
                print_('\n'.join(serverList).encode('utf-8', 'ignore'))
            except NameError:
                print_('\n'.join(serverList))
            except IOError:
                pass
            sys.exit(0)
    else:
        servers = closestServers(config['client'])

    if not args.simple:
        print_('Testing from %(isp)s (%(ip)s)...' % config['client'])

    if args.server:
        try:
            best = getBestServer([x for x in servers if x['id'] == args.server])
        except IndexError:
            print_('Invalid server ID')
            sys.exit(1)
    elif args.mini:
        name, ext = os.path.splitext(args.mini)
        if ext:
            url = os.path.dirname(args.mini)
        else:
            url = args.mini
        urlparts = urlparse(url)
        try:
            f = urlopen(args.mini)
        except:
            print_('Invalid Speedtest Mini URL')
            sys.exit(1)
        else:
            text = f.read()
            f.close()
        extension = re.findall('upload_extension: "([^"]+)"', text.decode())
        if not extension:
            for ext in ['php', 'asp', 'aspx', 'jsp']:
                try:
                    f = urlopen('%s/speedtest/upload.%s' % (args.mini, ext))
                except:
                    pass
                else:
                    data = f.read().strip()
                    if (f.code == 200 and
                            len(data.splitlines()) == 1 and
                            re.match('size=[0-9]', data)):
                        extension = [ext]
                        break
        if not urlparts or not extension:
            print_('Please provide the full URL of your Speedtest Mini server')
            sys.exit(1)
        servers = [{
            'sponsor': 'Speedtest Mini',
            'name': urlparts[1],
            'd': 0,
            'url': '%s/speedtest/upload.%s' % (url.rstrip('/'), extension[0]),
            'latency': 0,
            'id': 0
        }]
        try:
            best = getBestServer(servers)
        except:
            best = servers[0]
    else:
        if not args.simple:
            print_('Selecting best server based on ping...')
        best = getBestServer(servers)

    if not args.simple:
        try:
            str()
            print_(('Hosted by %(sponsor)s (%(name)s) [%(d)0.2f km]: '
                   '%(latency)s ms' % best).encode('utf-8', 'ignore'))
        except NameError:
            print_('Hosted by %(sponsor)s (%(name)s) [%(d)0.2f km]: '
                   '%(latency)s ms' % best)
    else:
        print_('Ping: %(latency)s ms' % best)

    sizes = [350, 500, 750, 1000, 1500, 2000, 2500, 3000, 3500, 4000]
    urls = []
    for size in sizes:
        for i in range(0, 4):
            urls.append('%s/random%sx%s.jpg' %
                        (os.path.dirname(best['url']), size, size))
    if not args.simple:
        print_('Testing download speed', end='')
    dlspeed = downloadSpeed(urls, args.simple)
    if not args.simple:
        print_()
    print_('Download: %0.2f M%s/s' %
           ((dlspeed / 1000 / 1000) * args.units[1], args.units[0]))

    sizesizes = [int(.25 * 1000 * 1000), int(.5 * 1000 * 1000)]
    sizes = []
    for size in sizesizes:
        for i in range(0, 25):
            sizes.append(size)
    if not args.simple:
        print_('Testing upload speed', end='')
    ulspeed = uploadSpeed(best['url'], sizes, args.simple)
    if not args.simple:
        print_()
    print_('Upload: %0.2f M%s/s' %
           ((ulspeed / 1000 / 1000) * args.units[1], args.units[0]))

    if args.share and args.mini:
        print_('Cannot generate a speedtest.net share results image while '
               'testing against a Speedtest Mini server')
    elif args.share:
        dlspeedk = int(round((dlspeed / 1000) * 8, 0))
        ping = int(round(best['latency'], 0))
        ulspeedk = int(round((ulspeed / 1000) * 8, 0))

        apiData = [
            'download=%s' % dlspeedk,
            'ping=%s' % ping,
            'upload=%s' % ulspeedk,
            'promo=',
            'startmode=%s' % 'pingselect',
            'recommendedserverid=%s' % best['id'],
            'accuracy=%s' % 1,
            'serverid=%s' % best['id'],
            'hash=%s' % md5(('%s-%s-%s-%s' %
                             (ping, ulspeedk, dlspeedk, '297aae72'))
                            .encode()).hexdigest()]

        req = Request('http://www.speedtest.net/api/api.php',
                      data='&'.join(apiData).encode())
        req.add_header('Referer', 'http://c.speedtest.net/flash/speedtest.swf')
        f = urlopen(req)
        response = f.read()
        code = f.code
        f.close()

        if int(code) != 200:
            print_('Could not submit results to speedtest.net')
            sys.exit(1)

        qsargs = parse_qs(response.decode())
        resultid = qsargs.get('resultid')
        if not resultid or len(resultid) != 1:
            print_('Could not submit results to speedtest.net')
            sys.exit(1)

        print_('Share results: http://www.speedtest.net/result/%s.png' %
               resultid[0])


def test():
    ''' Simple tester for speedtest '''
    try:
        speedtest()
    except KeyboardInterrupt:
        print_('\nCancelling...')


if __name__ == '__main__':
    test()



