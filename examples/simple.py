"""Simple usage example of kafka-logging-handler."""

import logging
import os
import sys
import time
import configparser
sys.path.append(
    os.path.realpath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)
from kafka_logger.handlers import KafkaLoggingHandler

sasl_mechanism = 'SCRAM-SHA-256'
security_protocol = 'SASL_SSL'
username = ""
password = ""
KAFKA_SERVER = "localhost:9094"
KAFKA_TOPIC = "default_topic"


def main(settings_ini: str):    
    """Setup logger and test logging."""
    global KAFKA_SERVER, KAFKA_TOPIC, username, password, sasl_mechanism, security_protocol
    # validate that Kafka configuration is available
    confg = configparser.ConfigParser(
        allow_no_value=True, interpolation=configparser.ExtendedInterpolation()
    )
    confg.read(settings_ini)
    section = confg['KAFKA']
    topics = section.pop('topics').split(",")
    KAFKA_TOPIC = topics[0]
    KAFKA_SERVER = section['bootstrap_servers']
    username = section['username']
    password = section['password']
    sasl_mechanism, security_protocol = section['sasl_mechanism'], section['security_protocol']

    logger = logging.getLogger("test.logger")
    logger.propagate = False
    log_level = logging.DEBUG

    log_format = logging.Formatter(
        "%(asctime)s %(name)-12s %(levelname)-8s %(message)s", "%Y-%m-%dT%H:%M:%S"
    )

    # create handler to show logs at stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(log_format)
    logger.addHandler(stdout_handler)

    # create Kafka logging handler
    kafka_handler = KafkaLoggingHandler(
        KAFKA_SERVER,
        KAFKA_TOPIC,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=username,
        sasl_plain_password=password,
        # you can configure how often logger will send logs to Kafka
        # flush_buffer_size=3,  # uncomment to see that it works slower
        # flush_interval=3.0,  # interval in seconds
        unhandled_exception_logger=logger,
        kafka_producer_args={
            "api_version_auto_timeout_ms": 1000000,
            "request_timeout_ms": 1000000,
            "retries": 5
        },
        # you can include arbitrary fields to all produced logs
        additional_fields={"service": "test_service"},
    )
    kafka_handler.setFormatter(log_format)
    logger.addHandler(kafka_handler)

    logger.setLevel(log_level)

    # test logging
    logger.debug("Test debug level logs")
    for idx in range(3):
        logger.info("Test log #%d", idx)
        time.sleep(0.5)

    # log unhandled top-level exception logging
    raise Exception("No try/except block here")


if __name__ == "__main__":
    main("kafka-settings.ini")
