"""This module contains logging handler which forwards logs to Kafka."""

import atexit
import json
import logging
from multiprocessing import Queue
import os
import socket
import sys
from threading import Lock, Thread, Timer
import time

from kafka import KafkaProducer  # pylint: disable=import-error


class KafkaLoggerException(Exception):
    """Exception to identify errors in Kafka Logger."""

    pass


class KafkaLoggingHandler(logging.Handler):
    """
    This handler enables the user to forward logs to Kafka.

    Attributes:
        additional_fields (dict): extra fields attached to logs
        buffer (list): logs (dict) waiting for a flush to Kafka
        buffer_lock (threading.Lock): multithreading lock for a buffer access
        flush_interval (float): scheduled flush interval in seconds
        kafka_topic_name (str): topic name
        main_process_pid (int): pid of the process which initialized logger
        max_buffer_size (int): flush if buffer > max size
        mp_log_handler_flush_lock (threading.Lock):
            locked when mp_log_handler_thread flushes logs
        mp_log_handler_thread (threading.Thread): thread that flushed mp queue
        mp_log_queue (multiprocessing.Queue):
            queue used to redirect logs of child processes
        producer (kafka.KafkaProducer): producer object
        timer (threading.Timer): thread with a scheduled logs flush
        unhandled_exception_logger (logging.Logger):
            logger that will be used to log uhandled top-level exception

    """

    __LOGGING_FILTER_FIELDS = ['msecs',
                               'relativeCreated',
                               'levelno',
                               'created']
    __MULTIPROCESSING_QUEUE_FLUSH_DELAY = 0.2

    def __init__(self,
                 hosts_list,
                 topic,
                 security_protocol='SSL',
                 ssl_cafile=None,
                 kafka_producer_args=None,
                 additional_fields={},
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
            additional_fields (None, optional):
                A dictionary with all the additional fields that you would like
                to add to the logs, such the application, environment, etc.
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
        self.additional_fields = additional_fields.copy()
        self.additional_fields.update({
            'host': socket.gethostname(),
            'host_ip': socket.gethostbyname(socket.gethostname())
        })

        if kafka_producer_args is None:
            kafka_producer_args = {}

        self.producer = KafkaProducer(
            bootstrap_servers=hosts_list,
            security_protocol=security_protocol,
            ssl_cafile=ssl_cafile,
            value_serializer=lambda msg: json.dumps(msg).encode("utf-8"),
            **kafka_producer_args)

        # setup exit hooks
        # exit hooks work only in main process
        # termination of child processes uses os.exit() and ignore any hooks
        atexit.register(self.at_exit)
        sys.excepthook = self.unhandled_exception

        # multiprocessing support
        self.main_process_pid = os.getpid()
        self.mp_log_queue = Queue()
        # main process thread that will flush mp queue
        self.mp_log_handler_flush_lock = Lock()
        self.mp_log_handler_thread = Thread(
            target=self.mp_log_handler,
            name="Kafka Logger Multiprocessing Handler")
        # daemon will terminate with the main process
        self.mp_log_handler_thread.setDaemon(True)
        self.mp_log_handler_thread.start()

    def prepare_record_dict(self, record):
        """
        Prepare a dictionary log item.

        Format a log record and extend dictionary with default values.

        Args:
            record (logging.LogRecord): log record

        Returns:
            dict: log item ready for Kafka
        """
        # use default formatting
        # Update the msg dict to include all of the message attributes
        self.format(record)

        # If there's an exception, let's convert it to a string
        if record.exc_info:
            record.msg = repr(record.msg)
            record.exc_info = repr(record.exc_info)

        # Append additional fields
        rec = self.additional_fields.copy()
        for key, value in record.__dict__.items():
            if key not in KafkaLoggingHandler.__LOGGING_FILTER_FIELDS:
                if key == "args":
                    # convert ALL argument to a str representation
                    # Elasticsearch supports number datatypes
                    # but it is not 1:1 - logging "inf" float
                    # causes _jsonparsefailure error in ELK
                    value = tuple(repr(arg) for arg in value)
                if key == "msg" and not isinstance(value, str):
                    # msg contains custom class object
                    # if there is no formatting in the logging call
                    value = str(value)
                rec[key] = "" if value is None else value

        return rec

    def emit(self, record):
        """
        Add log to the buffer or forward to the main process.

        Args:
            record: Logging message
        """
        # drop Kafka logging to avoid infinite recursion.
        if record.name == 'kafka.client':
            return

        record_dict = self.prepare_record_dict(record)

        if os.getpid() == self.main_process_pid:
            self.append_to_buffer(record_dict)
        else:  # if forked
            self.mp_log_queue.put(record_dict)

    def append_to_buffer(self, record_dict):
        """
        Place log dictionary to the buffer.

        Triggers/schedules a flush of the buffer.

        Args:
            record_dict (dict): log item
        """
        with self.buffer_lock:
            self.buffer.append(record_dict)

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

    def mp_log_handler(self):
        """Emit logs from multiprocessing queue."""
        while True:
            if self.mp_log_handler_flush_lock.locked():
                # second+ iteration
                self.mp_log_handler_flush_lock.release()
            record_dict = self.mp_log_queue.get(block=True)  # wait for logs
            self.mp_log_handler_flush_lock.acquire()
            self.append_to_buffer(record_dict)

    def at_exit(self):
        """
        Flush logs at exit, close the producer.

        Flush operation will close scheduled flush thread.
        Kafka raises RecordAccumulator in case of flushing in close method.
        """
        # Kafka's RecordAccumulator is still alive here
        if self.unhandled_exception_logger is not None:
            # check if there are running subprocesses and log a warning
            try:
                import psutil
                main_process = psutil.Process(pid=self.main_process_pid)
            except ImportError:
                pass
            except psutil.NoSuchProcess:
                pass
            else:
                children = main_process.children(recursive=True)
                if children:
                    self.unhandled_exception_logger.warning(
                        "There are %d child process(es) at the moment of the "
                        "main process termination. This may cause logs loss.",
                        len(children))
        while self.mp_log_queue.qsize() != 0:
            time.sleep(KafkaLoggingHandler.__MULTIPROCESSING_QUEUE_FLUSH_DELAY)
        # wait until everything in multiprocessing queue will be buffered
        self.mp_log_handler_flush_lock.acquire()

        if self.timer is not None:
            self.flush()
        self.producer.close()

    def unhandled_exception(self, exctype, exception, traceback):
        """
        Log top-level exception to the provided logger.

        Args:
            exctype (type): type of the exception
            exception (Exception): exception object from excepthook
            traceback (traceback): traceback object
        """
        if self.unhandled_exception_logger is not None:
            self.unhandled_exception_logger.exception(
                "Unhandled top-level exception",
                exc_info=(exctype, exception, traceback, ))

    def close(self):
        """Close the handler."""
        logging.Handler.close(self)
