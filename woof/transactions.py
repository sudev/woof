from woof.partitioned_producer import PartitionedProducer
import socket
import time
import logging

log = logging.getLogger("kafka")


class TransactionLogger():
    def __init__(self, broker, vertical, host=socket.gethostname(), async=False):
        self.broker = broker
        self.this_host = host
        self.vertical = vertical
        self.producer = PartitionedProducer(broker,async=async)
        self.topic = _get_topic_from_vertical(vertical)
        print self.topic

    def New(self, txn_id, amount, skus, detail="#", userid="#", email="#", phone="#"):
        self._send_log("NEW", txn_id, amount, skus, detail, userid, email, phone)

    def Modify(self, txn_id, amount="#", skus=[], detail="#", userid="#", email="#", phone="#"):
        self._send_log("MODIFY", txn_id, amount, skus, detail, userid, email, phone)

    def Cancel(self, txn_id, amount="#", skus=[], detail="#", userid="#", email="#", phone="#"):
        self._send_log("CANCEL", txn_id, amount, skus, detail, userid, email, phone)

    def Fulfil(self, txn_id, amount="#", skus=[], detail="#", userid="#", email="#", phone="#"):
        self._send_log("FULFIL", txn_id, amount, skus, detail, userid, email, phone)

    def _send_log(self, verb, txn_id, amount, skus, detail="#", userid="#", email="#", phone="#"):
        msg = self._format_message(verb, txn_id, amount, skus, detail, userid, email, phone)
        log.info("[transactions log] topic %s txnid %s msg %s \n", self.topic, txn_id, msg)

        self.producer.send(self.topic, txn_id, msg)

    def _format_message(self, verb, txn_id, amount, skus, detail, userid, email, phone):
        """
        Generates log message.
        """
        separator = '\t'
        skus_as_string = ",".join(skus)
        return "%s" % (separator.join([self.this_host, str(time.time()), verb, txn_id,
                       str(amount), skus_as_string, detail, userid, email, phone]))


def _get_topic_from_vertical(vertical):
    return "_".join(["TRANSACTIONS", vertical])
