#!/usr/bin/env python
import logging, time, os, sys, argparse
from kazoo.client import KazooClient
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

ZK_OFFSETS_PATH_FMT = "/consumers/%s/offsets/"


def get_offsets(zk, group, topic):
    path = ZK_OFFSETS_PATH_FMT % (group)
    path += topic
    try:
        partitions = zk.get_children(path)
    except:
        print "Check if consumer group name is correct"

    offsets_topic = {}

    for part in partitions:
        path += "/%s" % part
        try:
            offset, meta = zk.get(path)
        except:
            offset = 0
        offsets_topic[part] = offset

    print "Offsets for topic ", topic
    print offsets_topic, "\n"

    return offsets_topic


def commit_offsets_in_kafka(cons, topic, offsets_topic):

    for partition, offset in offsets_topic.items():
        tp = TopicPartition(topic, int(partition))
        cons.assign([tp])
        cons.seek(tp, int(offset))
        # commit it
        cons.commit()


if __name__ == "__main__":
    logging.basicConfig(
        format=
        '%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        filename='/tmp/woofload',
        level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("zk_server", help="host:port to the zookeeper server")
    parser.add_argument("kafka_broker",
                        help="host:port to the kafka broker server")
    parser.add_argument("group", help="consumer group name ")
    parser.add_argument("-t",
                        "--topic",
                        default="all",
                        help="topic to use, all for all topics")
    args = parser.parse_args()

    # TODO
    # handle topic = all by getting topics from zk and doing a for

    zk = KazooClient(hosts=args.zk_server)
    zk.start()

    kafka_cons = KafkaConsumer(bootstrap_servers=args.kafka_broker,
                               group_id=args.group)

    # get offsets from Zk
    offsets_topic = get_offsets(zk, args.group, args.topic)
    #commit them to Kafka
    try:
        commit_offsets_in_kafka(kafka_cons, args.topic, offsets_topic)
    except:
        print "Kafka Storage offset commit failed."
