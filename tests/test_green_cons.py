import time, sys, logging, os
from woof.green_consumer import GreenFeedConsumer

if len(sys.argv) <= 1:
    topic = "test1"
else:
    topic = sys.argv[1]

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    filename='/tmp/wooflog',
    level=logging.INFO
)

logger = logging.getLogger('kafka')
logger.setLevel(logging.INFO)

srv = os.getenv("GOMSG_SRV","localhost:9092")

def dummy(key, value):
    print "[GREEN]  %s" % (str(value))

def callback_xx(key, value):
    print "[GREEN_XX]  %s" % (str(value))


fc = GreenFeedConsumer(srv,  group ='TestGroup')
print srv
print "listing to topic %s"%(topic)
fc.add_topic(topic, dummy)
fc.start()
#fc.add_topic(topic,callback_xx)

time.sleep(60)
