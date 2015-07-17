import sys
import os
import logging
from woof.partitioned_producer import PartitionedProducer
#import pdb; pdb.set_trace()
import time

if len(sys.argv) <=3  :
    topic = "test.3part"
    key = "JY"
    msg = "Hello cruel world"
else :
    topic = sys.argv[1]
    key = sys.argv[2]
    msg = sys.argv[3]

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    filename='/tmp/kafkalog',
    level=logging.INFO
    )

logger = logging.getLogger('kafka')
logger.setLevel(logging.INFO)

server = os.getenv("GOMSG_SRV","localhost:9092")

print server
print topic,key,msg

t1 = time.time()
fp = PartitionedProducer(server)
print "Connect time ", time.time() -t1

t1 = time.time()
fp.send( topic, key , " [%s] %s" %(str(t1), msg))
print "Send time ", time.time() -t1

