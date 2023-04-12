import queue


class Broadcaster:
    def __init__(self):
        self.listeners = []

    def listen(self):
        que = queue.Queue(maxsize=60)
        self.listeners.append(que)
        return que

    def broadcast(self, msg):
        for i in reversed(range(len(self.listeners))):
            try:
                self.listeners[i].put_nowait(msg)
            except queue.Full:
                del self.listeners[i]
