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



# to continue to use offsets from zk
#fc =  FeedConsumer(srv, offset= 'latest', group ='TestGroup',use_zk=True)
fc =  FeedConsumer(srv, offset= 'latest', group ='TestGroup')

print "listing to topic %s"%(topic)
fc.add_topic(topic,dummy)
fc.start()
time.sleep(15)





time.sleep(60)
