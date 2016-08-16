import threading, logging, time, signal

from kafka import KafkaConsumer
from kafka.errors import KafkaTimeoutError
from .common import WoofNotSupported, CURRENT_PROD_BROKER_VERSION

log = logging.getLogger("woof")


class FeedConsumer(threading.Thread):
    """Threaded gomsg feed consumer
    callbacks are called on a separate thread in the same method
    
    keyword arguments :

    broker (list): List of initial broker nodes the consumer should contact to
    bootstrap initial cluster metadata.  This does not have to be the full node list.
    It just needs to have at least one broker

    group (str): the name of the consumer group to join, Offsets are fetched /
    committed to this group name.

    offset='earliest' : read all msgs from beginning of time;  default read fresh

    async=True : In case of Zk commit offset everytime

    commit_every_t_ms:  How much time (in milliseconds) to before commit to zookeeper

    kwargs : anything else you want to pass to kafka-python

    """
    daemon = True

    def __init__(self,
                 broker,
                 group,
                 offset='latest',
                 commit_every_t_ms=1000,
                 parts=None,
                 kill_signal=signal.SIGTERM,
                 wait_time_before_exit=1,
                 use_zk=False,
                 async_commit=True,
                 handler_timeout_ms=60000,
                 **kwargs):
        self.brokerurl = broker
        self.kill_signal = kill_signal
        self.exit_consumer = False
        self.async_commit = async_commit
        try:
            self.create_kill_signal_handler()
        except Exception as e:
            log.error(
                "[feedconsumer log] exception %s. Skipping signal handler install. ",
                str(e))
            pass

        self.wait_time_before_exit = wait_time_before_exit

        if use_zk:
            kwargs['api_version'] = kwargs.get('api_version', '0.8.1')
            # ZK autocommit does not seem to work reliably
            # TODO
            self.async_commit = False

        else:
            kwargs['api_version'] = kwargs.get('api_version',
                                               CURRENT_PROD_BROKER_VERSION)

        # curb over-optimism
        handler_timeout_ms = min(handler_timeout_ms, 60000)

        try:
            self.cons = KafkaConsumer(
                bootstrap_servers=broker,
                auto_offset_reset=offset,
                enable_auto_commit=self.async_commit,
                auto_commit_interval_ms=commit_every_t_ms,
                group_id=group,
                session_timeout_ms=handler_timeout_ms,
                **kwargs)

        except KafkaTimeoutError as e:
            log.error(
                "[feedconsumer log] INIT KafkaTimeoutError  %s. Please check broker string %s \n",
                str(e), broker)
            raise e
        except Exception as e1:
            log.error("[feedconsumer log] INIT err %s \n", str(e1))
            raise e1

        self.callbacks = {}
        super(FeedConsumer, self).__init__()

    def add_topic(self, topic, todo, parts=None):
        """
        Set the topic/partitions to consume

        todo (callable) : callback for the topic
        NOTE: Callback is for entire topic, if you call this for multiple
        partitions for same topic with diff callbacks, only the last callback
        is retained

        topic : topic to listen to

        parts (list) : tuple of the partitions to listen to

        """
        try:
            self.callbacks[topic] = todo

            if parts is None:
                log.info("[feedconsumer log] : adding topic %s ", topic)
            else:
                raise WoofNotSupported(
                    "manual partition assignement not supported")

            self.cons.subscribe(topics=self.callbacks.keys())
        except Exception as e:
            log.error("[feedconsumer log] add_topic err %s \n", str(e))
            raise e

    def remove_topic(self, topic, parts=None):
        if parts is not None:
            raise WoofNotSupported(
                "manual partition assignement not supported")

        try:
            self.cons.unsubscribe()
            del self.callbacks[topic]
            self.cons.subscribe(topics=self.callbacks.keys())
        except Exception as e:
            log.error("[feedconsumer log] remove_topic err %s \n", str(e))
            raise e

    def create_kill_signal_handler(self):
        def set_stop_signal(signal, frame):
            self.exit_consumer = True

        signal.signal(self.kill_signal, set_stop_signal)

    def check_for_exit_criteria(self):
        if self.exit_consumer:
            self.cons.commit()
            time.sleep(self.wait_time_before_exit)
            exit(0)

    def run(self):
        while True:
            try:
                for m in self.cons:
                    self.callbacks[m.topic](m.key, m.value)
                    # Looks like in kakfa-python,  the generator namely (fetcher._message_generator), stores offset in
                    # the next call to __next__
                    # https://github.com/dpkp/kafka-python/blob/master/kafka/consumer/fetcher.py
                    # TODO verfiy

                    # "extra" safely
                    if not self.async_commit:
                        self.cons.commit()

                    self.check_for_exit_criteria()
                self.check_for_exit_criteria()
            except Exception as e:
                log.error(
                    "[feedconsumer log] thread run  err %s ..continuing..\n",
                    str(e))
                time.sleep(1)
