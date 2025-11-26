import multiprocessing

_shared_queue = None


def init_queue(q):
    global _shared_queue
    _shared_queue = q


def get_queue():
    return _shared_queue