import traceback
from queue import Queue
from threading import Thread
import logging

log = logging.getLogger(__name__)

class Processor:
    POISON_PILL = 'POISON_PILL'

    def __init__(self, threads=1):
        self.results = []
        self.threads = []
        self.tasks = Queue()
        for i in range(threads):
            thread = Thread(target=self._thread_func)
            thread.start()
            self.threads.append(thread)

    def __len__(self):
        return self.tasks.qsize()

    def _thread_func(self):
        while True:
            task = self.tasks.get()
            if task == self.POISON_PILL:
                return
            try:
                task()
            except Exception:
                log.exception('Exception')
            finally:
                self.tasks.task_done()

    def add(self, task):
        self.tasks.put(task)

    def wait_done(self):
        self.tasks.join()
        log.info('all done')

    def stop(self):
        for thread in self.threads:
            self.add(self.POISON_PILL)

        for thread in self.threads:
            thread.join()
        log.info('stopped')

    def add_tasks(self, tasks):
        for task in tasks:
            self.add(task)
        log.info('%s tasks added' % self.tasks.qsize())

    def run(self, tasks):
        self.add_tasks(tasks)
        self.wait_done()
        self.stop()
