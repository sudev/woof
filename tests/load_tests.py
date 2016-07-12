#!/usr/bin/env python
import threading, logging, time, os, sys

from woof.consumer import FeedConsumer
from woof.producer import  FeedProducer

no_msgs = 1000
msg_size = 4096

if len(sys.argv) <=1  :
    topic = "testxx"
else :
    topic = sys.argv[1]


producer_stop = threading.Event()
consumer_stop = threading.Event()
server = os.getenv("GOMSG_SRV","localhost:9092")


class Producer(threading.Thread):
    big_msg = b'1' * msg_size

    def run(self):
        producer = FeedProducer(server)
        self.sent = 0
        for x in range(no_msgs):
            #self.big_msg = "MESSE %d"%x
            producer.send(topic, self.big_msg)
            self.sent += 1

        print "Sent %d messages" %self.sent


class Consumer(threading.Thread):

    msg_rcvd = 0
    cons_done = 0

    rlock = threading.RLock()

    @staticmethod
    def dummy(key,value):
        if len(value) == msg_size:
            Consumer.valid += 1
        else:
            Consumer.invalid += 1

        with Consumer.rlock:
            Consumer.msg_rcvd += 1
            if Consumer.msg_rcvd == no_msgs:
                Consumer.cons_done = 1
            else:
                print "%s got %d msgs" %("->",Consumer.msg_rcvd)



    def run(self):
        consumer = FeedConsumer(server, offset='latest', group ='LoadTestGroup')
        consumer.add_topic(topic, Consumer.dummy)
        Consumer.valid = 0
        Consumer.invalid = 0
        consumer.start()

        while 1 :
            if Consumer.cons_done == 1:
                print "Consumer done!"
                break
            else:
                time.sleep(5)


def main():
    print 'Starting tests with %d messages each of size %d' % (no_msgs, msg_size)
    threads = [
        Producer(),
        Consumer()
    ]

    for t in reversed(threads):
        t.start()
        time.sleep(.1)

    while threads[1].cons_done != 1:
        time.sleep(1)

    print 'Messages sent: %d' % threads[0].sent
    print 'Messages recvd: %d' % threads[1].valid
    print 'Messages invalid: %d' % threads[1].invalid

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        filename='/tmp/woofload',
        level=logging.INFO
        )
    main()