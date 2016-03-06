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
    def formatTime(self, seconds):
        return time.strftime('%H:%M:%S', time.gmtime(seconds))
    def update(self, pbar):
        if pbar.curValue == 0:
            return 'Time:  --:--:--'
        elif pbar.finished:
            return 'Time: %s' % self.formatTime(pbar.secondsElapsed)
        else:
            elapsed = pbar.secondsElapsed
            eta = elapsed * pbar.maxValue / pbar.curValue - elapsed
            return 'Time: %s' % self.formatTime(eta)

class Percentage(BarBase):
    def update(self, pbar):
        return '%3d%%' % pbar.percentage()

class BarFiller(FillerBase):
    """ Default filler of bar """
    def __init__(self, marker='>', left='>', right='>'):
        self.marker = marker
        self.left = left
        self.right = right
    def _formatMarker(self, pbar):
        if isinstance(self.marker, str):
            return self.marker
        else:
            return self.marker.update(pbar)
    def update(self, pbar, width):
        percent = pbar.percentage()
        cwidth = (width - len(self.left) - len(self.right))*2
        markedWidth = int(percent * cwidth / 100)
        m = self._formatMarker(pbar)
        bar = (self.left + (m*markedWidth).ljust(int(cwidth)) + self.right)
        return bar

gDefaultWidgets = [Percentage(), ' ', BarFiller()]

class ProgressBar(object):
    """ see test """
    def __init__(self, maxValue=100, widgets=gDefaultWidgets, termWidth=None, fd=sys.stderr):
        assert maxValue > 0
        self.maxValue = maxValue
        self.widgets = widgets
        self.fd = fd
        self.signalSet = False
        if termWidth is None:
            try:
                self.handleResize(None,None)
                signal.signal(signal.SIGWINCH, self.handleResize)
                self.signalSet = True
            except:
                self.termWidth = 80
        else:
            self.termWidth = termWidth
        self.curValue = 0
        self.finished = False
        self.prevPercentage = -1
        self.startTime = None
        self.secondsElapsed = 0

    def handleResize(self, signum, frame):
        h,w=array('h', ioctl(self.fd,termios.TIOCGWINSZ,'\0'*8))[:2]
        self.termWidth = w

    def percentage(self):
        return self.curValue*100.0 / self.maxValue

    def _formatWidgets(self):
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
            r[iw] = r[iw].update(self, (self.termWidth-currwidth)/num_hfill)
        return r

    def _formatLine(self):
        return ''.join(self._formatWidgets()).ljust(self.termWidth)

    def _needUpdate(self):
        return int(self.percentage()) != int(self.prevPercentage)

    def update(self, value):
        assert 0 <= value <= self.maxValue
        self.curValue = value
        if not self._needUpdate() or self.finished:
            return
        if not self.startTime:
            self.startTime = time.time()
        self.secondsElapsed = time.time() - self.startTime
        self.prevPercentage = self.percentage()
        if value != self.maxValue:
            self.fd.write(self._formatLine() + '\r')
        else:
            self.finished = True
            self.fd.write(self._formatLine() + '\n')

    def start(self):
        self.update(0)
        return self

    def finish(self):
        self.update(self.maxValue)
        if self.signalSet:
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


def test(maxValue, septime):
    head = CustomerHead()
    widgets = [head, '|', BarFiller(marker='='), '| ', Percentage(), ' ', ETA()]
    pbar = ProgressBar(widgets=widgets, maxValue=maxValue)
    pbar.start()
    for i in range(maxValue):
        time.sleep(septime)
        if not (i % 10):
            head.set(str(i)+' ')
        pbar.update(i)
    head.set(str(maxValue)+' ')
    pbar.update(maxValue)
    pbar.finish()


if __name__=='__main__':
    task = Thread(target=test, args=(1000, 0.005))
    task.start()
    task.join()
    print('test finish\n')

