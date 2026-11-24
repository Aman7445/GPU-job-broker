"""Advance the simulated broker clock while a server is running."""

import logging
from threading import Event, Thread
from time import monotonic


LOG = logging.getLogger("job_broker")


class Clock:
    def __init__(self, broker, speed):
        self.broker = broker
        self.speed = speed
        self.healthy = True
        self.stop = Event()
        self.thread = None

    def start(self):
        if self.speed:
            self.thread = Thread(target=self.run, name="sim-clock", daemon=True)
            self.thread.start()

    def run(self):
        last = monotonic()
        while not self.stop.wait(0.25):
            seconds = int((monotonic() - last) * self.speed)
            if seconds < 1:
                continue
            try:
                self.broker.advance(seconds=min(seconds, 86400))
                last += min(seconds, 86400) / self.speed
                self.healthy = True
            except Exception:
                self.healthy = False
                LOG.exception("event=clock_tick_failed")
                if self.stop.wait(2):
                    return

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join()
