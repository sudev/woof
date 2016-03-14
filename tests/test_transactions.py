import os
import logging
import time
import thread
from woof.transactions import TransactionLogger

logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        filename='/tmp/kafkalog',
        level=logging.INFO
)

logger = logging.getLogger('kafka')
logger.setLevel(logging.INFO)

srv = os.getenv("GOMSG_SRV", "localhost:9092")

stime = time.time()

# Instantiate
# Should be a long lived object
# async would be True for performance, if needed
# but in fringe cases if there is a restart, msg might not be deliverd
tr = TransactionLogger(srv, "dummy_vertical1", async=True)
print "Time taken for connection: ", time.time() - stime



def thread_test():
    stime = time.time()
    tr.New(txn_id="gofld3434",
           amount=3500,
           skus=["vcid_1", "vhid_1"],
           detail="{'foo':'bar'}",
           userid="rohith2506",
           email="r1@gmail.com",
           phone="8984758345345")
    print "Time taken to send one message: ", time.time() - stime

    # Modify
    tr.Modify(txn_id="gofld3434",
              amount=4000,
              detail="{'foo':'bar', 'foo1':'bar1'}",
              phone="8984758345345")

    # Cancel
    tr.Cancel(txn_id="gofld3434",
           phone="8984758345345")

    # Fulfil
    tr.Fulfil(txn_id="gofld3434",
              phone="8984758345345")

for i in range(10):
    thread.start_new_thread(thread_test,())


# sleep to allow msg to go
time.sleep(60)
