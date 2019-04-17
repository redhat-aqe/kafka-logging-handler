"""Simple usage example of kafka-logging-handler."""

import json
import logging
import os
import sys

from kafka_logger.handlers import KafkaLoggingHandler

REQUIRED_ENV_VARS = ['KAFKA_SERVER', 'KAFKA_CERT', 'KAFKA_TOPIC']


class CustomClass:
    """Dummy class to demo logging."""

    def __init__(self, value):
        """Initialize CustomClass object."""
        self._value = value

    def __str__(self):
        """Convert CustomClass to string."""
        return "CustomClass: {}".format(self._value)


def main():
    """Setup logger and test logging."""
    # validate that Kafka configuration is available
    assert all([(key in os.environ) for key in REQUIRED_ENV_VARS])

    logger = logging.getLogger("test.logger")
    logger.propagate = False
    log_level = logging.DEBUG

    log_format = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        '%Y-%m-%dT%H:%M:%S')

    # create handler to show logs at stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(log_format)
    logger.addHandler(stdout_handler)

    # create Kafka logging handler
    kafka_handler = KafkaLoggingHandler(
        os.environ['KAFKA_SERVER'],
        os.environ['KAFKA_TOPIC'],
        security_protocol='SSL',
        ssl_cafile=os.environ['KAFKA_CERT'],
        unhandled_exception_logger=logger,
    )
    kafka_handler.setFormatter(log_format)
    logger.addHandler(kafka_handler)

    logger.setLevel(log_level)

    logger.info("Test log with int parameter: %d", 42)
    logger.info("Test log with multiple parameters: %d %f", 42, 43.2)

    logger.info("Test log with str parameter: %s", "test1")
    logger.info("Test log with multiple str parameters: %s %s",
                "test1", "test2")

    custom_object = CustomClass('test')
    logger.info("Test logging of custom obj: %s", custom_object)
    # log record will contain the following values:
    # args: <__main__.CustomClass object at 0x7f3147041c88>
    # message: Test logging of custom obj: CustomClass: test

    # extra values have to be JSON serializable
    try:
        json.dumps(custom_object)
        # TypeError: Object of type 'CustomClass' is not JSON serializable
    except TypeError:
        logger.exception("Attempt to log non JSON serializable data")
    # please transform extra values to JSON
    logger.info("Test custom objects in extra argument", extra={
        "custom_field_number": 42,
        "custom_field_json": {"a": "test", "b": "test"}
    })


if __name__ == '__main__':
    main()
