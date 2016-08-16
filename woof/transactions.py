import logging
import socket
import time

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from common import CURRENT_PROD_BROKER_VERSION

log = logging.getLogger("woof")


class TransactionLogger(object):
    def __init__(self,
                 broker,
                 vertical,
                 host=socket.gethostname(),
                 async=False,
                 retries=1,
                 **kwargs):
        self.broker = broker
        self.this_host = host
        self.vertical = vertical
        self.async = async
        self.topic = _get_topic_from_vertical(vertical)
        kwargs['api_version'] = kwargs.get('api_version',
                                           CURRENT_PROD_BROKER_VERSION)
        # thread safe producer, uses default murmur2 partiioner by default
        # good for us
        self.producer = KafkaProducer(bootstrap_servers=broker,
                                      key_serializer=make_kafka_safe,
                                      value_serializer=make_kafka_safe,
                                      retries=retries,
                                      **kwargs)

    def New(self,
            txn_id,
            amount,
            skus,
            detail="#",
            userid="#",
            email="#",
            phone="#"):
        self._send_log("NEW", txn_id, amount, skus, detail, userid, email,
                       phone)

    def Modify(self,
               txn_id,
               amount="#",
               skus=[],
               detail="#",
               userid="#",
               email="#",
               phone="#"):
        self._send_log("MODIFY", txn_id, amount, skus, detail, userid, email,
                       phone)

    def Cancel(self,
               txn_id,
               amount="#",
               skus=[],
               detail="#",
               userid="#",
               email="#",
               phone="#"):
        self._send_log("CANCEL", txn_id, amount, skus, detail, userid, email,
                       phone)

    def Fulfil(self,
               txn_id,
               amount="#",
               skus=[],
               detail="#",
               userid="#",
               email="#",
               phone="#"):
        self._send_log("FULFIL", txn_id, amount, skus, detail, userid, email,
                       phone)

    def _send_log(self,
                  verb,
                  txn_id,
                  amount,
                  skus,
                  detail="#",
                  userid="#",
                  email="#",
                  phone="#",
                  retry=True,
                  retry_time_in_s=1):
        msg = self._format_message(verb, txn_id, amount, skus, detail, userid,
                                   email, phone)
        log.info("[transactions log] topic %s txnid %s msg %s /n", self.topic,
                 txn_id, msg)
        try:
            self.producer.send(self.topic, key=txn_id, value=msg)
            self.producer.flush()
        except KafkaTimeoutError as e:
            log.error(
                "[transactions log] KafkaTimeoutError ERROR %s topic %s txnid %s msg %s /n",
                str(e), self.topic, txn_id, msg)
            if retry:
                time.sleep(retry_time_in_s)
                self._send_log(verb,
                               txn_id,
                               amount,
                               skus,
                               detail,
                               userid,
                               email,
                               phone,
                               retry=False)
            else:
                raise e
        except Exception as e1:
            log.error(
                "[transactions log] GEN error ERROR %s topic %s txnid %s msg %s /n",
                str(e1), self.topic, txn_id, msg)
            raise e1

    def _format_message(self, verb, txn_id, amount, skus, detail, userid,
                        email, phone):
        """
        Generates log message.
        """
        separator = '\t'

        safe_skus = [make_kafka_safe(x) for x in skus]
        skus_as_string = ",".join(safe_skus)

        return separator.join([self.this_host, str(time.time(
        )), verb, make_kafka_safe(txn_id), make_kafka_safe(
            amount), skus_as_string, make_kafka_safe(detail), make_kafka_safe(
                userid), make_kafka_safe(email), make_kafka_safe(phone)])


def _get_topic_from_vertical(vertical):
    return "_".join(["TRANSACTIONS", vertical])


def make_kafka_safe(raw_data):
    if type(raw_data) != unicode:
        raw_data = str(raw_data)
        raw_data = raw_data.decode('utf-8')
        return raw_data.encode('ascii', 'ignore')
    else:
        return raw_data.encode('ascii', 'ignore')
