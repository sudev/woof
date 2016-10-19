# Woof
Persistent messaging at scale

## Introduction
Persistent messaging library which offers various flavors of messaging; optimizing for throughput, latency etc

Currently there is support for paritioned , persistent queues which use Apache Kafka as the backend. Future work will include low-latency messaging.

## Installation 

* Install requirements for woof.
```
pip install -r requirements.txt
```
* Install woof package
```
python setup.py install
```

## Sample Usage

**Producer**
```python
import sys
import os
import logging
from woof.producer import FeedProducer

fp = FeedProducer(server)
msg = "this"
fp.send(topic, " [MY MESSAGE] %s" %(msg))

```
**Consumer**

```python
import time, sys, logging, os
from woof.green_consumer import GreenFeedConsumer

fc = GreenFeedConsumer(srv,  group ='TestGroup')
print srv
print "listing to topic %s"%(topic)
fc.add_topic(topic, dummy)
fc.start()

time.sleep(60) // This is just to avoid the process exiting
```
