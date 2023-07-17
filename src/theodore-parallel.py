
import threading

def worker_thread(num):
    """Thread worker function"""
    print("Worker thread %s is running" % num)
    return

threads = []
for i in range(5):
    t = threading.Thread(target=worker_thread, args=(i,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()
