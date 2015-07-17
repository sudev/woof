
import logging,time
from woof.producer import FeedProducer

import time


topic = "NewT"
msg = "Hello cruel world"

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    filename='/tmp/kafkalog',
    level=logging.INFO
    )

logger = logging.getLogger('kafka')
logger.setLevel(logging.INFO)

server = "nmclkafka01:9092,nmclkafka02:9092,nmclkafka03:9092"

print server


fp = FeedProducer(server)



fp.send(topic , msg )

