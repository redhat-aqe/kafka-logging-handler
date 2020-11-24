"""Simple usage example of kafka-logging-handler."""

import logging
import os
import random
import sys
import time

from kafka_logger.handlers import KafkaLoggingHandler

REQUIRED_ENV_VARS = ["KAFKA_SERVER", "KAFKA_CERT", "KAFKA_TOPIC"]
PASSWORDS = ["qwerty", "12345", "password"]


def main():
    """Setup logger and test logging."""
    # validate that Kafka configuration is available
    assert all([(key in os.environ) for key in REQUIRED_ENV_VARS])

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

    def remove_lineno(log):
        """Example preprocessor, includes lineno to message field."""
        log["message"] += " (at line {})".format(log["lineno"])
        del log["lineno"]
        log["custom_field"] = 42
        return log

    def hide_passwords(log):
        """Example preprocessor, hides passwords."""
        for password in PASSWORDS:
            hidden_password = password[0] + "*" * (len(password) - 2) + password[-1]
            log["message"] = log["message"].replace(password, hidden_password)
        return log

    # create Kafka logging handler
    kafka_handler = KafkaLoggingHandler(
        os.environ["KAFKA_SERVER"],
        os.environ["KAFKA_TOPIC"],
        security_protocol="SSL",
        ssl_cafile=os.environ["KAFKA_CERT"],
        # you can configure how often logger will send logs to Kafka
        # flush_buffer_size=3,  # uncomment to see that it works slower
        # flush_interval=3.0,  # interval in seconds
        unhandled_exception_logger=logger,
        kafka_producer_args={
            "api_version_auto_timeout_ms": 1000000,
            "request_timeout_ms": 1000000,
        },
        # you can include arbitrary fields to all produced logs
        additional_fields={"service": "test_service"},
        log_preprocess=[remove_lineno, hide_passwords],
    )
    kafka_handler.setFormatter(log_format)
    logger.addHandler(kafka_handler)

    logger.setLevel(log_level)

    # test logging
    logger.debug("Test debug level logs")
    for idx in range(3):
        logger.info("Test log #%d: %s", idx, random.choice(PASSWORDS))
        time.sleep(0.5)


if __name__ == "__main__":
    main()
