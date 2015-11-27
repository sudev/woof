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

def part0(key,value):
    print "[PARTITION 0]  Value %s"%( str(value))

def part1(key,value):
    print "[PARTITION 1]  Value %s"%( str(value))


def part2(key,value):
    print "[PARTITION 2]  Value %s"%( str(value))



#fc =  FeedConsumer(srv, group ='TestGroup', offset= 'smallest')
fc1 =  FeedConsumer(srv, group ='TestGroup')
fc1.add_topic(topic,part0, (0,))
# Or could have given
# fc.add_topic(topic, callback_for_1_and_2, (1,2))

# This will pass only if number of partitions for the topic >=2
fc2 =  FeedConsumer(srv, group ='TestGroup')
fc2.add_topic(topic,part1, (1,))

# This will pass only if number of partitions for the topic is = 3.
fc3 =  FeedConsumer(srv, group ='TestGroup')
fc3.add_topic(topic,part2, (2,))

print "listening..  "



fc1.start()
fc2.start()
fc3.start()

time.sleep(120)
