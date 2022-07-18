# Cloud-Computing-Technology-Fundamentals
This repository contains the assignments of the course COL733 : Cloud Computing Technology Fundamentals

## Batch processing
### Work count application with multiple worker nodes
Scalable and fault-tolerant Word count application implemented using Celery having multiple worker nodes. This application analyses the customer-support-on-twitter dataset. The efficiency of the application with increase in the worker threads and the input size is also analysed

## Network paritioning
### Word count application on 3 nodes' cluster
Deployed a scalable and fault-tolerant Word count application implemented using Celery n a cluster of 3 nodes, thus achieving a higher throughput. Handled the network partition between the replicated storage systems on the 3 nodes. The deployed system is always available but may be temporarily inconsistent. 
Spawned 24 celery workers (8 workers on each VM) and process 1 file in each celery task. Each celery worker processes the provided input file set and stores the word count results in Redis.
Worker failures are also taken care of; these are tested by triggering the cold shutdown of a worker and by killing the Redis server. Analysed the correctness of this word-count application without any network partition/failures and after that also analysed its fault-tolerance to celery worker failures and to Redis instance failures
