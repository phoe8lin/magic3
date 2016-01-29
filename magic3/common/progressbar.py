# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import sys, time
from array import array
try:
    from fcntl import ioctl
    import termios
except ImportError:
    pass # can using in windows :)
import signal
from threading import Thread, RLock

class BarBase(object):
    def update(self, pbar):
        pass

class FillerBase(object):
    def update(self, pbar, width):
        pass

class ETA(BarBase):
    def format_time(self, seconds):
        return time.strftime('%H:%M:%S', time.gmtime(seconds))
    def update(self, pbar):
        if pbar.currval == 0:
            return 'Time:  --:--:--'
        elif pbar.finished:
            return 'Time: %s' % self.format_time(pbar.seconds_elapsed)
        else:
            elapsed = pbar.seconds_elapsed
            eta = elapsed * pbar.maxval / pbar.currval - elapsed
            return 'Time: %s' % self.format_time(eta)

class Percentage(BarBase):
    def update(self, pbar):
        return '%3d%%' % pbar.percentage()

class BarFiller(FillerBase):
    """ Default filler of bar """
    def __init__(self, marker='>', left='>', right='>'):
        self.marker = marker
        self.left = left
        self.right = right
    def _format_marker(self, pbar):
        if isinstance(self.marker, str):
            return self.marker
        else:
            return self.marker.update(pbar)
    def update(self, pbar, width):
        percent = pbar.percentage()
        cwidth = (width - len(self.left) - len(self.right))*2
        marked_width = int(percent * cwidth / 100)
        m = self._format_marker(pbar)
        bar = (self.left + (m*marked_width).ljust(int(cwidth)) + self.right)
        return bar

default_widgets = [Percentage(), ' ', BarFiller()]

class ProgressBar(object):
    """ see test """
    def __init__(self, maxval=100, widgets=default_widgets, term_width=None, fd=sys.stderr):
        assert maxval > 0
        self.maxval = maxval
        self.widgets = widgets
        self.fd = fd
        self.signal_set = False
        if term_width is None:
            try:
                self.handle_resize(None,None)
                signal.signal(signal.SIGWINCH, self.handle_resize)
                self.signal_set = True
            except:
                self.term_width = 80
        else:
            self.term_width = term_width
        self.currval = 0
        self.finished = False
        self.prev_percentage = -1
        self.start_time = None
        self.seconds_elapsed = 0

    def handle_resize(self, signum, frame):
        h,w=array('h', ioctl(self.fd,termios.TIOCGWINSZ,'\0'*8))[:2]
        self.term_width = w

    def percentage(self):
        return self.currval*100.0 / self.maxval

    def _format_widgets(self):
        r = []
        hfill_inds = []
        num_hfill = 0
        currwidth = 0
        for i, w in enumerate(self.widgets):
            if isinstance(w, FillerBase):
                r.append(w)
                hfill_inds.append(i)
                num_hfill += 1
            elif isinstance(w, str):
                r.append(w)
                currwidth += len(w)
            else:
                weval = w.update(self)
                currwidth += len(weval)
                r.append(weval)
        for iw in hfill_inds:
            r[iw] = r[iw].update(self, (self.term_width-currwidth)/num_hfill)
        return r

    def _format_line(self):
        return ''.join(self._format_widgets()).ljust(self.term_width)

    def _need_update(self):
        return int(self.percentage()) != int(self.prev_percentage)

    def update(self, value):
        assert 0 <= value <= self.maxval
        self.currval = value
        if not self._need_update() or self.finished:
            return
        if not self.start_time:
            self.start_time = time.time()
        self.seconds_elapsed = time.time() - self.start_time
        self.prev_percentage = self.percentage()
        if value != self.maxval:
            self.fd.write(self._format_line() + '\r')
        else:
            self.finished = True
            self.fd.write(self._format_line() + '\n')

    def start(self):
        self.update(0)
        return self

    def finish(self):
        self.update(self.maxval)
        if self.signal_set:
            signal.signal(signal.SIGWINCH, signal.SIG_DFL)
        
class CustomerHead(FillerBase):
    """ extands FillerBase to custom use """
    def __init__(self):
        super().__init__()
        self.lock = RLock()
        self.head = ''
    def set(self, head):
        """ set user head, show at begin of bar line """
        with self.lock:
            self.head = head
    def update(self, pbar, width):
        """ update bar line """
        return self.head


def test(maxval, septime):
    head = CustomerHead()
    widgets = [head, '|', BarFiller(marker='='), '| ', Percentage(), ' ', ETA()]
    pbar = ProgressBar(widgets=widgets, maxval=maxval)
    pbar.start()
    for i in range(maxval):
        time.sleep(septime)
        if not (i % 10):
            head.set(str(i)+' ')
        pbar.update(i)
    head.set(str(maxval)+' ')
    pbar.update(maxval)
    pbar.finish()


if __name__=='__main__':
    task = Thread(target=test, args=(1000, 0.005))
    task.start()
    task.join()
    print('test finish\n')

