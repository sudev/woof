from woof.partitioned_producer import PartitionedProducer

class TransactionLogger():
    def __init__(self, broker, vertical, ):
        self.broker = broker
        self.vertical = vertical
        self.producer = PartitionedProducer(broker)
        self.topic = _get_topic_from_vertical(vertical)

    def New(self, txn_id, amount, skus, detail="#", userid="#", email="#", phone="#"):
        self._send_log("NEW", txn_id, amount, skus, detail, userid, email, phone)

    def Modify(self, txn_id, amount, skus, detail="#", userid="#", email="#", phone="#"):
        self._send_log("MODIFY", txn_id, amount, skus, detail, userid, email, phone)

    def Cancel(self, txn_id, amount, skus, detail="#", userid="#", email="#", phone="#"):
        self._send_log("CANCEL", txn_id, amount, skus, detail, userid, email, phone)

    def Fulfil(self, txn_id, amount, skus, detail="#", userid="#", email="#", phone="#"):
        self._send_log("FULFIL", txn_id, amount, skus, detail, userid, email, phone)

    def _send_log(self, verb, txn_id, amount, skus, detail="#", userid="#", email="#", phone="#"):
        msg = self._format_message(verb, txn_id, amount, skus, detail, userid, email, phone)
        self.producer.send(self.topic, txn_id, msg)

    def _format_message(self, verb, txn_id, amount, skus, detail, userid, email, phone):
        """
        Generates log message.
        """
        separator = '\t'
        return separator.join([verb, txn_id, str(amount), skus, detail, userid, email, phone])


def _get_topic_from_vertical(topic):
    # TODO : fancy stuff
    return topic
