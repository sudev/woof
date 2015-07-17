import time, sys, logging, os, time
from woof.consumer import FeedConsumer

if len(sys.argv) <=1  :
    topic = "testxx"
else :
    topic = sys.argv[1]

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    filename='/tmp/wooflog',
    level=logging.INFO
    )

logger = logging.getLogger('kafka')
logger.setLevel(logging.INFO)
srv = os.getenv("GOMSG_SRV","localhost:9092")

def dummy(key,value):
    print "[%s]  Value %s"%(str(time.time()), str(value))

def print_start(key,value):
    print "[x] FROM START    %s"%(str(value))

def testxx_start(key,value):
    print "[x]FROM NEWER TOPIC  |  %s"%(str(value))




#fc = FeedConsumer(srv)
fc =  FeedConsumer(srv, offset= 'smallest', group ='TestGroup')
print "listing to topic %s"%(topic)
fc.add_topic(topic,dummy)
fc.start()
time.sleep(15)



"""
fc_from_start = FeedConsumer(srv,offset= 'smallest')
fc_from_start.add_topic("test_start",print_start)
fc_from_start.start()
time.sleep(30)
print "listing to topic text xx"
fc_from_start.add_topic("testxx",testxx_start)
"""


time.sleep(60)
