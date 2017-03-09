A python script to commit offsets to Kafka. 
Assumes the offset store to be Kafka, should work with brokers with version > `0.82`.

Requirements: 

```
pip install kafka-python
```

Usage: 

```
python commit_offset.py kafkabrokerip:port sample_json.json
```

The Json describing the group, topic, partition should follow the following syntax. 

### Syntax

```json 
{
        "group-name": {
                "topic-name": {
                        "partition-number": "desired-offset-number"
        }
}
}
```

### Example

```json 
{
        "mygroup": {
                "mytopic": {
                        "1": 100,
                        "2": 2356
                }
        }
}
```
