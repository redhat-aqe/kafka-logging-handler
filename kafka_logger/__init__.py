from kafka_logger.handlers import KafkaLoggerException, KafkaLoggingHandler
from kafka_logger.listeners import listen_channel_output
from kafka_logger.outlog import OutputLogger
import logging
import sys
import configparser


class DefaultPartitioner:
    """Default partitioner.
    Return the first available partition, the purpose of using this configuration
    so that the stream maintain the correct order in the listener
    """
    @classmethod
    def __call__(cls, key, all_partitions, available):
        """
        Get the partition corresponding to key
        :param key: partitioning key
        :param all_partitions: list of all partitions sorted by partition ID
        :param available: list of available partitions in no particular order
        :return: one of the values from all_partitions or available
        """
        return available[0] if available else all_partitions[0]


def init_kafka_logger(name: str, settings_ini: str):
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

    logger = logging.getLogger(name)
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
            "partitioner": DefaultPartitioner(),
            "retries": 5
        },
        # you can include arbitrary fields to all produced logs
        additional_fields={"service": "test_service"},
    )

    kafka_handler.setFormatter(log_format)
    logger.addHandler(kafka_handler)
    logger.setLevel(log_level)

    return logger


__all__ = ['KafkaLoggingHandler', 'KafkaLoggerException', 'OutputLogger', 'listen_channel_output']