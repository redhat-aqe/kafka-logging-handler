from kafka import KafkaConsumer
from typing import List
import json

def listen_channel_output(
    topics: List[str], bootstrap_servers: str,
    username: str = None, password: str = None,
    security_protocol: str = 'PLAINTEXT', sasl_mechanism: str = None,
    timeout=200000, **kwargs
):
    """ Listen to log publish in the dweet channel from remote server

    :raises Exception: [description]
    :return: [description]
    :rtype: [type]
    """
    try:
        consumer = KafkaConsumer(
            *topics,
            group_id="%s-consumer" % username,
            bootstrap_servers=bootstrap_servers,
            sasl_plain_username=username,
            sasl_plain_password=password,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
        )
        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            topic, partition, offset, key, value = (message.topic, message.partition,
                                                    message.offset, message.key,
                                                    json.loads(message.value))
            
            yield topic, value
    except Exception as e:
        raise e
