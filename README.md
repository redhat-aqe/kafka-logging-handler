Kafka Logging Handler
=====================
[![Downloads](https://pepy.tech/badge/kafka-logging-handler)](https://pepy.tech/project/kafka-logging-handler)
![Workflow](https://github.com/redhat-aqe/kafka-logging-handler/workflows/release-pipeline/badge.svg)


The following library simplifies the process of forwarding logs to a Kafka consumer.

How to Use
----------

```python
import logging
from kafka_logger.handlers import KafkaLoggingHandler

KAFKA_BOOTSTRAP_SERVER = ('<hostname:port>')
KAFKA_CA = '<path_to_ca_cert>'
TOPIC = '<publish_topic>'

logger = logging.getLogger('MyCoolProject')

# Instantiate your kafka logging handler object
kafka_handler_obj = KafkaLoggingHandler(KAFKA_BOOTSTRAP_SERVER,
                                        TOPIC,
                                        ssl_cafile=KAFKA_CA)

logger.addHandler(kafka_handler_obj)
# Set logging level
logger.setLevel(logging.DEBUG)

logger.info('Happy Logging!')
```

Troubleshooting
----------

If you see the following error when running on macOS:

```
Traceback (most recent call last):
  File "/path/to/kafka-logging-handler/kafka_logger/handlers.py", line 109, in __init__
    'host_ip': socket.gethostbyname(socket.gethostname())
socket.gaierror: [Errno 8] nodename nor servname provided, or not known
```

The issues is that `socket.gethostname()` resolves to a name that is not available in `/etc/hosts`.
First run `python3 -c "import socket; print(socket.gethostname())"` to get the name (e.g. `MacBook-Pro`).
Then fix it with `sudo echo 127.0.0.1 MacBook-Pro >> /etc/hosts`, where `MacBook-Pro` is your computer name.
There won't be OS-specific fix for it in the library.
