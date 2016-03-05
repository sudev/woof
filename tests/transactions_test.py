import sys
import os
import logging
import time
from woof.transactions import TransactionLogger

stime = time.time()
tr = TransactionLogger("localhost:9092", "transaction_dummy")
print "Time taken for connection: ", time.time() - stime

stime = time.time()
tr.New("gofld3434", 3500, "htld864578346578", "{'foo':'bar'}", "rohith2506", "r1@gmail.com", "8984758345345")
print "Time taken to send one message: ", time.time() - stime
