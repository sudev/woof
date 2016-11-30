Usuage: 

```
python commit_offset.py sample_json.json
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
"""
```
