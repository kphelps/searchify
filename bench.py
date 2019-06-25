import json
import random
import requests
import time
import threading
import uuid

URL = 'http://localhost'
PORTS = [8080, 8081, 8082]

LOOPS = 10
BULK_SIZE = 1
THREADS = 8
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

def bench_loop():
    for i in range(LOOPS):
        port = random.choice(PORTS)
        url = "{}:{}/{}/_bulk".format(
            URL,
            port,
            INDEX_NAME,
        )
        print(url)
        requests.post(url, data=build_batch())

def run_benchmark():
    threads = []
    for _ in range(THREADS):
        thread = threading.Thread(target=bench_loop)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

start = time.time()
run_benchmark()
end = time.time()

print("Indexed {} documents in {} seconds".format(
    THREADS * LOOPS * BULK_SIZE,
    end - start,
))
