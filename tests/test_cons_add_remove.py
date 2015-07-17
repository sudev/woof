import time,sys,logging

from woof.consumer import FeedConsumer

if len(sys.argv) <=1  :
	topic = "test1"
else :
	topic = sys.argv[1]

logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        filename='/tmp/kafkalog',
        level=logging.INFO
        )

logger = logging.getLogger('kafka')
logger.setLevel(logging.INFO)

def dummy(key,value):
	print "[x] Key %s :: Value %s"%(str(key),str(value)) 


def new_start(key,value):
	print "\txxx--  [x] Key %s :: Value %s"%(str(key),str(value)) 

print " Adding topic " + topic

fc = FeedConsumer("localhost:9092")
fc.add_topic(topic,dummy)
fc.start()
fc.add_topic("test1",new_start)
time.sleep(40)
print " Removing topic " + topic
fc.remove_topic(topic)
time.sleep(40)

