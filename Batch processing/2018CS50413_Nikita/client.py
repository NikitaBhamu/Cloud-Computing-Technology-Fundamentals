from celery import Celery
from celery import chord
from celery import group
import sys
import os
import tasks
import math

if (len(sys.argv) < 2):
    print("Use the command: python3 client.py <data_dir>")

DIR=sys.argv[1]
#k = int(sys.argv[2])

abs_files=[os.path.join(pth, f) for pth, dirs, files in os.walk(DIR) for f in files]

filelist = abs_files[:]
chunk_size = 100

task_list = []
for i in range(0,(len(filelist)//chunk_size)+1):
    k = min(i*chunk_size+chunk_size, len(filelist))
    task_list.append(tasks.countWords.s(filelist, i*chunk_size , k))

dicts = group(task_list)().get()
#final_dict = tasks.mergeDicts.s(final_dicts).get()
#final_dict = chord(tasks.countWords.s(filelist, i*chunk_size , min(i*chunk_size+chunk_size, len(filelist))) for i in range(0,(len(filelist)//chunk_size)+1))(tasks.mergeDicts.s()).get()

final_dict = {}
for d in dicts:
    for word in d:
        if(final_dict.get(word,0)==0):
            final_dict[word] = 1
        else:
            final_dict[word] += 1

print(final_dict)
