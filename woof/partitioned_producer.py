from itertools import cycle
import logging
from kafka.client import KafkaClient
from kafka.producer import Producer
from kafka.common import LeaderNotAvailableError,KafkaUnavailableError
from kafka.util import kafka_bytestring
import random

log = logging.getLogger("kafka")

BATCH_SEND_DEFAULT_INTERVAL = 20
BATCH_SEND_MSG_COUNT = 20


def dumb_hash(key):
    sum = 0
    str_key = str(key)
    for s in str_key:
        sum += ord(s)

    return sum


class PartitionedProducer(Producer):
    """
    Feed Producer class
    use send() to send to any topic
    """
    
    def __init__(self, broker, partitioner=dumb_hash, async=False,
                 req_acks=Producer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=Producer.DEFAULT_ACK_TIMEOUT,
                 codec=None,
                 batch_send=False,
                 batch_send_every_n=BATCH_SEND_MSG_COUNT,
                 batch_send_every_t=BATCH_SEND_DEFAULT_INTERVAL):
        self.partitions = {}
        self.hash_fn = partitioner
        try:
            self.client = KafkaClient(broker)
            super(PartitionedProducer, self).__init__(self.client,
                                            async, req_acks,
                                            ack_timeout, codec, batch_send,
                                            batch_send_every_n,
                                            batch_send_every_t)
        except KafkaUnavailableError:
            log.critical( "\nCluster Unavailable %s : Check broker string\n", broker)
            raise
        except:
            raise

    def _next_partition(self, topic, key):
        if topic not in self.partitions:
            if not self.client.has_metadata_for_topic(topic):
                self.client.load_metadata_for_topics(topic)
            self.partitions[topic] = self.client.get_partition_ids_for_topic(topic)

        log.debug( "\nPartitions are %s \n", str(self.partitions[topic] ))
        """
        print "---partitions are"
        print self.partitions[topic]
        print self.hash_fn(key)
        print self.hash_fn(key) % len(self.partitions[topic])
        print "---partitions are"
        """

        return self.partitions[topic][self.hash_fn(key) % len(self.partitions[topic])]
    
    def send(self, topic, key, *msg, **kwargs):
        try:
            topic = kafka_bytestring(topic)
            partition = kwargs.get('partition', None)
            if not partition:
                partition = self._next_partition(topic, key)
            return self._send_messages(topic, partition, *msg, key=key)
        except LeaderNotAvailableError:
            self.client.ensure_topic_exists(topic)
            return self.send(topic, *msg)
        except:
            raise


class CyclicPartitionedProducer(PartitionedProducer):

    def __init__(self, broker, random_start=True):
        self.partition_cycles = {}
        self.random_start = random_start
        super(CyclicPartitionedProducer, self).__init__(self.broker)

    def _next_partition(self, topic):
        if topic not in self.partition_cycles:
            if not self.client.has_metadata_for_topic(topic):
                self.client.load_metadata_for_topics(topic)

            self.partition_cycles[topic] = cycle(self.client.get_partition_ids_for_topic(topic))

            # Randomize the initial partition that is returned
            if self.random_start:
                num_partitions = len(self.client.get_partition_ids_for_topic(topic))
                for _ in xrange(random.randint(0, num_partitions-1)):
                    next(self.partition_cycles[topic])

        return next(self.partition_cycles[topic])
