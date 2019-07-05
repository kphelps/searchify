import json
import random
import requests
import time
import threading
import uuid

URL = 'http://localhost'
PORTS = [8080, 8081, 8082]

LOOPS = 10
BULK_SIZE = 1000
THREADS = 100
INDEX_NAME = 'hello-world'

def build_batch():
    s = ''
    for i in range(BULK_SIZE):
        s += json.dumps({
            "index": {
                "_index": INDEX_NAME,
                "_id": str(uuid.uuid4()),
            }
        })
        s += '\n'
        s += json.dumps({"hello": str(uuid.uuid4())})
        s += '\n'
    return s

def bench_loop(n):
    batch = build_batch()
    for i in range(LOOPS):
        port = random.choice(PORTS)
        url = "{}:{}/{}/_bulk".format(
            URL,
            port,
            INDEX_NAME,
        )
        print("{} {}".format(n, url))
        print(requests.post(url, data=batch).content)

def run_benchmark():
    threads = []
    for n in range(THREADS):
        thread = threading.Thread(target=bench_loop, args=[n])
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

start = time.time()
run_benchmark()
end = time.time()

count = THREADS * LOOPS * BULK_SIZE
dt = end - start
rate = count / dt

print("Indexed {} documents in {} seconds ({} docs/sec)".format(
    count,
    dt,
    rate
))
