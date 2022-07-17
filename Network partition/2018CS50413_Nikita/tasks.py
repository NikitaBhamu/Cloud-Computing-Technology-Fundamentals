import json
import redis
from celery import Celery
from config import IPS

broker = f'pyamqp://test:test@{IPS[0]}'
app = Celery('tasks', backend='rpc', broker=broker)
reddises = [redis.Redis(host=ip, decode_responses=True, socket_timeout=5) for ip in IPS]

@app.task(acks_late=True, ignore_results=True, bind=True, max_retries=3)
def map(self, filename):
    wc = {}
    with open(filename, mode="r", newline="\r") as f:
        for text in f:
            if text == "\n":
                continue
            sp = text.split(",")[4:-2]
            tweet = " ".join(sp)
            for word in tweet.split(" "):
                if word not in wc:
                    wc[word] = 0
                wc[word] += 1
    
    #dump the word count of each file into a dictionary
    node = {filename: json.dumps(wc)}
    count=0
    nodes_done=[]

    #break when atleast two node get the same write
    while (count < 2):
        for i in range(3):
            if i in nodes_done:
                continue
            try:
                reddises[i].sadd("FILES", json.dumps(node))
                nodes_done.append(i)
                count+=1
            except:
                pass
   