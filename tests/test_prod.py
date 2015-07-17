import sys
import os
import logging
from woof.producer import FeedProducer
#import pdb; pdb.set_trace()
import time

if len(sys.argv) <=2  :
    topic = "default"
    msg = "Hello cruel world"
else :
    topic = sys.argv[1]
    msg = sys.argv[2]

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    filename='/tmp/kafkalog',
    level=logging.INFO
    )

logger = logging.getLogger('kafka')
logger.setLevel(logging.INFO)

server = os.getenv("GOMSG_SRV","localhost:9092")

print server

t1 = time.time()
fp = FeedProducer(server)
print "Connect time ", time.time() -t1

t1 = time.time()
fp.send(topic, " [%s] %s" %(str(t1), msg))
print "Send time ", time.time() -t1

