import logging
import os
import sys

from woof.partitioned_producer import CyclicPartitionedProducer, PartitionedProducer, dumb_hash
# import pdb; pdb.set_trace()
import time

if len(sys.argv) <= 3:
    topic = "test.3part"
    key = "JY"
    msg = "Hello cruel world"
else:
    topic = sys.argv[1]
    key = sys.argv[2]
    msg = sys.argv[3]

logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        filename='/tmp/kafkalog',
        level=logging.DEBUG
)

logger = logging.getLogger('kafka')
logger.setLevel(logging.INFO)

server = os.getenv("GOMSG_SRV", "localhost:9092")

print server
print topic, key, msg

t1 = time.time()
prod_cyclic = CyclicPartitionedProducer(server, async=True)
print "Cyclic Async Connect time ", time.time() - t1
t1 = time.time()
prod_cyclic.send(topic, key, " [%s] %s" % (str(t1), msg))
print "CyclicSend Async time ", time.time() - t1

t1 = time.time()
prod_keyed = PartitionedProducer(server)
print "Paritioned Connect time ", time.time() - t1
t1 = time.time()
prod_keyed.send(topic, key, " [%s] %s" % (str(t1), msg))
print "Paritioned time ", time.time() - t1

t1 = time.time()
prod_legacy = PartitionedProducer(server, partitioner=dumb_hash)
print "Legacy Prod Connect time ", time.time() - t1
t1 = time.time()
prod_legacy.send(topic, key, " [%s] %s" % (str(t1), msg))
print "Legacy Prod  ", time.time() - t1