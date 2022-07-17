from celery import Celery

app = Celery('tasks', backend='redis://localhost:6579', broker='pyamqp://guest@localhost//')

@app.task(acks_late=True)
def countWords(filelist, start, end):
    wc = {}
    for i in range(start, end):
        filename = filelist[i]
        with open(filename, mode='r', newline='\r') as f:
            for text in f:
                if text == '\n':
                    continue
                sp = text.split(',')[4:-2]
                tweet = " ".join(sp)
                for word in tweet.split(" "):
                    if word not in wc:
                        wc[word] = 0
                    wc[word] = wc[word] + 1
    return wc

@app.task(acks_late=True)
def mergeDicts(dicts):
    final_dict = {}
    for d in dicts:
        for word in d:
            if(final_dict.get(word,0)==0):
                final_dict[word] = 1
            else:
                final_dict[word] += 1
    return final_dict
