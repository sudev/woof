import time
import sys
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import json
import logging
"""
Json describing the group, topic, partition map.
Syntax -
{
        "group-name": {
                "topic-name": {
                        "partition-number": "desired-offset-number"
        }
}
}
Example - 
{
        "mygroup": {
                "mytopic": {
                        "1": 100,
                        "2": 2356
                }
        }
}
"""


def commit_offsets_in_kafka(broker, group_name, group_dict):
    cons = KafkaConsumer(bootstrap_servers=broker, group_id=group_name)
    for topic_name, topic_dict in group_dict.iteritems():
        for partition, offset in topic_dict.iteritems():
            logging.info(
                "Commiting {} {} to topic {} and partition number {}".format(
                    group_name, offset, topic_name, partition))
            tp = TopicPartition(topic_name, int(partition))
            cons.assign([tp])
            cons.seek(tp, int(offset))
            # commit it
            cons.commit()
            time.sleep(1)
    cons.close()


def main():
    # Create connection.
    broker = sys.argv[1]
    json_path = sys.argv[2]
    json_dict = json.loads(open(json_path, 'r').read())
    for group_name, group_dict in json_dict.iteritems():
        print(broker, group_name, group_dict)
        commit_offsets_in_kafka(broker, group_name, group_dict)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
