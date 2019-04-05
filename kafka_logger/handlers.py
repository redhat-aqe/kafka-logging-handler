"""This module contains logging handler which forwards logs to Kafka."""

import atexit
import json
import logging
import sys
from threading import Lock, Timer

from kafka import KafkaProducer  # pylint: disable=import-error


class KafkaLoggerException(Exception):
    """Exception to identify errors in Kafka Logger."""

    pass


class KafkaLoggingHandler(logging.Handler):
    """
    This handler enables the user to forward logs to Kafka.

    Attributes:
        buffer (list): logs (dict) waiting for a flush to Kafka
        buffer_lock (threading.Lock): multithreading lock for a buffer access
        flush_interval (float): scheduled flush interval in seconds
        kafka_topic_name (str): topic name
        max_buffer_size (int): flush if buffer > max size
        producer (kafka.KafkaProducer): producer object
        timer (threading.Timer): thread with a scheduled logs flush
        unhandled_exception_logger (logging.Logger):
            logger that will be used to log uhandled top-level exception

    """

    def __init__(self,
                 hosts_list,
                 topic,
                 security_protocol='SSL',
                 ssl_cafile=None,
                 kafka_producer_args=None,
                 flush_buffer_size=None,
                 flush_interval=5.0,
                 unhandled_exception_logger=None):
        """
        Initialize the handler.

        Args:
            hosts_list: list of the Kafka hostnames
            topic: kafka consumer topic to where logs are forwarded
            security_protocol (str, optional): KafkaProducer security protocol
            ssl_cafile (None, optional): path to CA file
            kafka_producer_args (None, optional):
                extra arguments to pass to KafkaProducer
            flush_buffer_size (None/int, optional):
                flush if buffer > max size, None means there is no restriction
            flush_interval (int, optional): scheduled flush interval in seconds
            unhandled_exception_logger (None/logging.Logger, optional):
                logger that will be used to log uhandled top-level exception

        Raises:
            KafkaLoggerException: in case of incorrect logger configuration

        """
        logging.Handler.__init__(self)

        if security_protocol == 'SSL' and ssl_cafile is None:
            raise KafkaLoggerException("SSL CA file isn't provided.")

        self.kafka_topic_name = topic
        self.unhandled_exception_logger = unhandled_exception_logger

        self.buffer = []
        self.buffer_lock = Lock()
        self.max_buffer_size = flush_buffer_size \
            if flush_buffer_size is not None else float("inf")
        self.flush_interval = flush_interval
        self.timer = None

        if kafka_producer_args is None:
            kafka_producer_args = {}
        self.producer = KafkaProducer(
            bootstrap_servers=hosts_list,
            security_protocol=security_protocol,
            ssl_cafile=ssl_cafile,
            value_serializer=lambda msg: json.dumps(msg).encode("utf-8"),
            **kafka_producer_args)

        # setup exit hooks
        atexit.register(self.at_exit)
        sys.excepthook = self.unhandled_exception

    def emit(self, record):
        """
        Add a new log to the buffer.

        Args:
            record: Logging message
        """
        # drop Kafka logging to avoid infinite recursion.
        if record.name == 'kafka.client':
            return

        # use default formatting
        # Update the msg dict to include all of the message attributes
        self.format(record)

        # If there's an exception, let's convert it to a string
        if record.exc_info:
            record.msg = repr(record.msg)
            record.exc_info = repr(record.exc_info)

        with self.buffer_lock:
            self.buffer.append(record.__dict__)

        # schedule a flush
        if len(self.buffer) >= self.max_buffer_size:
            self.flush()  # flush oversized buffer
        else:
            self.schedule_flush()

    def flush(self):
        """
        Flush a buffer to Kafka.

        Skip if the buffer is empty.
        Uses multithreading lock to access buffer.
        """
        # clean up the timer (reached max buffer size)
        if self.timer is not None and self.timer.is_alive():
            self.timer.cancel()
        self.timer = None

        if self.buffer:
            # get logs from buffer
            with self.buffer_lock:
                logs_from_buffer = self.buffer
                self.buffer = []
            # send logs in parallel
            for log in logs_from_buffer:
                self.producer.send(self.kafka_topic_name, log)
            self.producer.flush()  # blocks multiple parallel send calls()

    def schedule_flush(self):
        """Run a daemon thread that will flush buffer."""
        if self.timer is None:  # if timer isn't initialized yet
            self.timer = Timer(self.flush_interval, self.flush)
            self.timer.setDaemon(True)
            self.timer.start()

    def at_exit(self):
        """
        Flush logs at exit, close the producer.

        Flush operation will close scheduled flush thread.
        Kafka raises RecordAccumulator in case of flushing in close method.
        """
        # Kafka's RecordAccumulator is still alive here
        if self.timer is not None:
            self.flush()
        self.producer.close()

    def unhandled_exception(self, _, exception, __):
        """
        Log top-level exception to the provided logger.

        Args:
            exception (Exception): exception object from excepthook

        """
        if self.unhandled_exception_logger is not None:
            try:
                raise exception
            except Exception:
                self.unhandled_exception_logger.exception(
                    "Unhandled top-level exception")

    def close(self):
        """Close the handler."""
        logging.Handler.close(self)
