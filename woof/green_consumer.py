from gevent import monkey

monkey.patch_all()
import logging, gevent, time
import signal
import sys, traceback

from .consumer import FeedConsumer
from .common import WoofNotSupported

log = logging.getLogger("kafka")


class GreenFeedConsumer(FeedConsumer):
    """
    Greenlet based feed consumer
    All callbacks are spawned as greenlets on a background thread


    keyword arguments :

    broker (list): List of initial broker nodes the consumer should contact to
    bootstrap initial cluster metadata.  This does not have to be the full node list.
    It just needs to have at least one broker

    group (str): the name of the consumer group to join, Offsets are fetched /
    committed to this group name.

    offset='smallest' : read all msgs from beginning of time;  default read fresh

    commit_every_t_ms:  How much time (in milliseconds) to before commit to zookeeper
    kill_signal: What is the kill signal to handle exit gracefully.
    wait_time_before_exit: How much time to wait before exiting green threads

    """
    daemon = True

    def __init__(self,
                 broker,
                 group,
                 offset='largest',
                 commit_every_t_ms=1000,
                 parts=None,
                 kill_signal=signal.SIGTERM,
                 wait_time_before_exit=1):

        # TODO
        # getting issues with gevent and kafka-python (select to be specific)
        # https://github.com/dpkp/kafka-python/issues/702
        # unsupported till them
        raise WoofNotSupported("GreenFeedConsumer  not supported")

        super(GreenFeedConsumer, self).__init__(
            broker, group, offset, commit_every_t_ms, parts, kill_signal,
            wait_time_before_exit)

    def wrap(self, callback, mesg):
        # earlier wrap was used to mark task done, but not needed here i guess
        # Looks like in kakfa-python,  the generator namely (fetcher._message_generator), stores offset in
        # the next call to __next__
        # https://github.com/dpkp/kafka-python/blob/master/kafka/consumer/fetcher.py
        # TODO this may make greenlet based callbacks dangerous..as we have commited without callback returning
        callback(mesg.key, mesg.value)

    def run(self):
        while True:
            try:
                for m in self.cons:
                    gevent.spawn(self.wrap, self.callbacks[m.topic], m)
                    self.check_for_exit_criteria()
                self.check_for_exit_criteria()
            except Exception as e:
                print 'EXCEPTION', e
                traceback.print_exc(file=sys.stdout)
                log.error(
                    "[greenconsumer log] thread run  err %s ..continuing../n",
                    str(e))
                time.sleep(1)
