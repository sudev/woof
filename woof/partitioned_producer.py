import logging
import random

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from kafka.partitioner.default import DefaultPartitioner
from common import CURRENT_PROD_BROKER_VERSION
from .transactions import make_kafka_safe

log = logging.getLogger("kafka")

BATCH_SEND_DEFAULT_INTERVAL = 20
BATCH_SEND_MSG_COUNT = 32


class PartitionedProducer():
    """
    use send() to send to any topic and distribute based on key
    """

    def __init__(self, broker,
                 partitioner=None,  # Note if the earlier hash is needed, need to explicitly pass dumb_hash
                 async=False,
                 req_acks=None,  # unused  - here for legacy support
                 ack_timeout=None,  # unused  - here for legacy support
                 codec=None,
                 batch_send=False,
                 batch_send_every_n=BATCH_SEND_MSG_COUNT,
                 batch_send_every_t=BATCH_SEND_DEFAULT_INTERVAL,  # unused  - here for legacy support
                 retries=3,
                 **kwargs):

        try:
            self.async = async
            if partitioner is not None:
                _partitioner = CustomPartitioner(partitioner)
            else:
                _partitioner = DefaultPartitioner()
            kwargs['api_version'] = kwargs.get('api_version',
                                               CURRENT_PROD_BROKER_VERSION)
            self.prod = KafkaProducer(bootstrap_servers=broker,
                                      key_serializer=make_kafka_safe,
                                      value_serializer=make_kafka_safe,
                                      batch_size=batch_send_every_n,
                                      retries=retries,
                                      partitioner=_partitioner,
                                      **kwargs)
        except Exception as e1:
            log.error("[partitionedproducer log] GEN err %s  /n", str(e1))
            raise

    def send(self, topic, key, *msg):
        try:
            for _msg in msg:
                self.prod.send(topic, key=key, value=_msg)

            # for async flush will happen in background
            if not self.async:
                self.prod.flush()

        except KafkaTimeoutError as e:
            log.error(
                "[feedproducer log] KafkaTimeoutError err %s topic %s  /n",
                str(e), topic)
            raise e
        except Exception as e1:
            log.error("[feedproducer log] GEN  err %s topic %s /n", str(e1),
                      topic)
            raise e1


# Note if the earlier hash is needed, need to explicitly pass dumb_hash
def dumb_hash(key):

    sum = 0
    str_key = str(key)
    for s in str_key:
        sum += ord(s)

    log.debug("[feedproducer log] dumb_hash , key = %s", sum)
    return sum


class CustomPartitioner(object):
    _hash_map = {}

    def __init__(self, hasher):
        CustomPartitioner._hash_map[1] = hasher

    @classmethod
    def __call__(cls, key, all_partitions, available):

        if key is None:
            if available:
                return random.choice(available)
            return random.choice(all_partitions)

        idx = cls._hash_map[1](key)
        idx &= 0x7fffffff
        idx %= len(all_partitions)
        return all_partitions[idx]


class CyclicPartitionedProducer(KafkaProducer):
    """
    use send() to send to any topic and distribute keys cyclically in partitions
    """

    def __init__(self, broker, async=True, random_start=True, **kwargs):
        self.partition_cycles = {}
        self.random_start = random_start
        self.async = async
        kwargs['api_version'] = kwargs.get('api_version',
                                           CURRENT_PROD_BROKER_VERSION)
        super(CyclicPartitionedProducer, self).__init__(
            bootstrap_servers=broker,
            key_serializer=make_kafka_safe,
            value_serializer=make_kafka_safe,
            **kwargs)

    def _partition(self, topic, partition, key, value, serialized_key,
                   serialized_value):
        if partition is not None:
            assert partition >= 0
            assert partition in self._metadata.partitions_for_topic(
                topic), 'Unrecognized partition'
            return partition

        all_partitions = list(self._metadata.partitions_for_topic(topic))
        n_partitions = len(all_partitions)

        try:
            offset = (self.partition_cycles[topic] + 1) % n_partitions
        except:
            if self.random_start:
                offset = random.randint(0, n_partitions - 1)
            else:
                offset = 0

        self.partition_cycles[topic] = offset
        return all_partitions[offset]

    def send(self, topic, key, *msg):
        try:
            for _msg in msg:
                super(CyclicPartitionedProducer, self).send(topic,
                                                            key=key,
                                                            value=_msg)

            # for async flush will happen in background
            if not self.async:
                self.prod.flush()

        except KafkaTimeoutError as e:
            log.error(
                "[feedproducer log] KafkaTimeoutError err %s topic %s  /n",
                str(e), topic)
            raise e
        except Exception as e1:
            log.error("[feedproducer log] GEN  err %s topic %s /n", str(e1),
                      topic)
            raise e1
