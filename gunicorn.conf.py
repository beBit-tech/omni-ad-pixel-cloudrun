from multiprocessing import Process, Queue

import queue_singleton
from writer_loop import writer_main

def on_starting(server):
    q = Queue()
    queue_singleton.init_queue(q)

    p = Process(target=writer_main, args=(q,), daemon=True)
    p.start()

    server.log.info("Started writer process pid=%s", p.pid)
    server.writer_process = p

def on_exit(server):
    q = queue_singleton.get_queue()
    if q is not None:
        try:
            from writer_loop import STOP_SENTINEL
            q.put(STOP_SENTINEL)
        except Exception:
            server.log.exception("Failed to send STOP to writer")

    p = getattr(server, "writer_process", None)
    if p is not None:
        p.join(timeout=10)
        server.log.info("Writer process joined")
