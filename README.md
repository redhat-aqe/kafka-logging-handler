Kafka Logging Handler
=====================

The following library simplifies the process of forwarding logs to a Kafka consumer.

## Installation

Installation is easy!

```
$ pip install git+https://github.com/sandyz1000/kafka-logging-handler.git
```


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

Using Setting.ini (Here we are trying to use shell in kaggle notebook), to download colabshell you can clone from 
[here](https://github.com/sandyz1000/colabshell)

-----------------

```python
from kafka_logger import init_kafka_logger, OutputLogger
from colabshell import ColabShell

logger = init_kafka_logger("kafka.logger", "../kafka_logger/settings.ini")

logger.info("Initializing decentralized logging")
with OutputLogger(logger) as redirect:
    print("From redirector")
    shell = ColabShell(port=10001, username='colabshell', password='password')
    shell.run()

```

### Your typical setting file will look like this

```

[DEFAULT]
timeout = 2000000
log_level = DEBUG
log_dir = logs

; General log configuration
[LOG_FORMATTER]
to_json=True
format=%(asctime)s:%(levelname)s:%(name)s:%(message)s
; # format='%(relativeCreated)6.1f %(threadName)12s: %(levelname).1s %(module)8.8s:%(lineno)-4d %(message)s',
log_max_bytes=1024 * 1024 * 10
log_backups=5
relay_stdout=True

; # Kafka Configuration
[KAFKA]
bootstrap_servers = localhost:9094
username =
password =
topics = default,decentlog
timeout=${DEFAULT:timeout}
group_id=default-consumer
sasl_plain_username=
sasl_plain_password=
kafka_retry=5
sasl_mechanism=
security_protocol=PLAINTEXT

```

To access log from command line:
----------

```bash
kafka_logger_cli --settings_ini kafka-settings.ini 
```
**Note**: The settings file should be the same that you have used to start the kafka-logging server

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
