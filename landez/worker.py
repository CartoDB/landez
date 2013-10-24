
import Queue
import threading

class Batch(object):

    def __init__(self, callback=None, nthreads=4):
        self.in_q = Queue.Queue()
        self.out_q = Queue.Queue()
        self.nthreads = nthreads
        self.callback = callback
        self._start_threads()

    def _start_threads(self):
        self.ths = [threading.Thread(target=self.consumer) for th in xrange(self.nthreads)]
        for x in self.ths: x.start()

    def process(self, items):
        for i in items:
            self.in_q.put(i)

    def task(self, data):
        if self.callback:
            self.callback(data)

    def consumer(self):
        while True:
            item = self.in_q.get()
            if not item: break 
            self.task(item)

    def wait(self):
        # put end guards
        for th in self.ths: self.in_q.put(None)
        # wait to finish
        for th in self.ths: th.join()

if __name__ == '__main__':
    def task(data):
        print data
    b = Batch(task)
    b.process(['a', 'b', 'c', 'd'])
    b.wait()


"""
def threaded(regs, nthreads=4):
    # ripped from http://www.dabeaz.com/generators/Generators.pdf
    def consumer(q):
        while True:
            item = q.get()
            if not item: break
            download_pdf(item)

    in_q = Queue.Queue()

    # start threads
    ths = [threading.Thread(target=consumer,args=(in_q,))
                for th in xrange(nthreads)]
    for x in ths: x.start()

    # put files to download
    for i in regs:
        in_q.put(i)

    # put end guards
    for th in xrange(nthreads): in_q.put(None)

    # wait to finish
    for x in ths: x.join()
"""
