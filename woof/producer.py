import logging, time

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from common import CURRENT_PROD_BROKER_VERSION
from .transactions import make_kafka_safe

log = logging.getLogger("woof")


class FeedProducer(object):
    """
    Feed Producer class
    use send() to send to any topic
    """

    def __init__(self, broker, retries=3, async=False, **kwargs):
        try:
            kwargs['api_version'] = kwargs.get('api_version',
                                               CURRENT_PROD_BROKER_VERSION)
            self.prod = KafkaProducer(bootstrap_servers=broker,
                                      key_serializer=make_kafka_safe,
                                      value_serializer=make_kafka_safe,
                                      retries=retries,
                                      **kwargs)
            self.async = async
        except Exception as e:
            log.error("[feedproducer log] Constructor error ERROR %s  \n",
                      str(e))
            raise

    def send(self, topic, *msgs):
        try:
            for msg in msgs:
                future = self.prod.send(topic, msg)
                log.info("[feedproducer log] about to flush.. recordmeta %s message %s \n",
                         str(future.get(timeout=1)), msg)

        except KafkaTimeoutError as e:
            log.error(
                "[feedproducer log] KafkaTimeoutError err %s topic %s message %s \n",
                str(e), topic, str(msgs))
            raise e
        except Exception as e1:
            log.error("[feedproducer log] GEN  err %s topic %s message %s \n", str(e1),
                      topic, str(msgs))
            raise e1

    def flush(self):
        try:
            self.prod.flush()
        except KafkaTimeoutError as e:
            log.error(
                "[feedproducer log] KafkaTimeoutError err %s topic \n", str(e))
            raise e
        except Exception as e1:
            log.error("[feedproducer log] GEN  err %s topic %s \n", str(e1))
            raise e1
