"""Usage of kafka-logging-handler with multiprocessing and multithreading."""
# pylint: disable=global-statement
import logging
import multiprocessing
import os
import sys
import threading
import time
from multiprocessing import Process

from kafka_logger.handlers import KafkaLoggingHandler

REQUIRED_ENV_VARS = ["KAFKA_SERVER", "KAFKA_CERT", "KAFKA_TOPIC"]

CHILD_PROCESSES = 5
CHILD_THREADS = 5

LOGGER = None


def get_process_thread():
    """Get string with process and thread names."""
    # you can get PID and thread ID as well:
    # os.getpid(), threading.current_thread().ident
    return "(process: {}, thread: {})".format(
        multiprocessing.current_process().name, threading.current_thread().name
    )


def child_process(index):
    """
    Log a message.

    Args:
        index (int): index of the child process
    """
    LOGGER.info("Hi, I'm child process #%d %s", index, get_process_thread())
    logging.shutdown()


def thread_function(index):
    """Log a message."""
    LOGGER.info(
        "Hi, I'm a thread #%d in the main process %s", index, get_process_thread()
    )
    logging.shutdown()


def main():
    """Setup logger and test logging."""
    global LOGGER

    # validate that Kafka configuration is available
    assert all([(key in os.environ) for key in REQUIRED_ENV_VARS])

    LOGGER = logging.getLogger("test.logger")
    LOGGER.propagate = False
    log_level = logging.DEBUG

    log_format = logging.Formatter(
        "%(asctime)s %(name)-12s %(levelname)-8s %(message)s", "%Y-%m-%dT%H:%M:%S"
    )

    # create handler to show logs at stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(log_format)
    LOGGER.addHandler(stdout_handler)

    # create Kafka logging handler
    kafka_handler = KafkaLoggingHandler(
        os.environ["KAFKA_SERVER"],
        os.environ["KAFKA_TOPIC"],
        security_protocol="SSL",
        ssl_cafile=os.environ["KAFKA_CERT"],
        unhandled_exception_logger=LOGGER,
        additional_fields={"service": "test_service"},
    )
    kafka_handler.setFormatter(log_format)
    LOGGER.addHandler(kafka_handler)

    LOGGER.setLevel(log_level)

    LOGGER.info("Hi there, I'm the main process! %s", get_process_thread())

    # test child processes
    child_processes = []
    for idx in range(CHILD_PROCESSES):
        child = Process(
            target=child_process, name="Child process #{}".format(idx), args=(idx,)
        )
        child_processes.append(child)
        child.start()

    time.sleep(1)  # in the main proc only
    alive = [proc.is_alive() for proc in child_processes]
    assert not any(alive)

    LOGGER.info("Multiprocessing logging.shutdown() works")

    threads = []
    for idx in range(CHILD_THREADS):
        thread = threading.Thread(
            target=thread_function,
            name="Thread of the main process #{}".format(idx),
            args=(idx,),
        )
        threads.append(thread)
        thread.start()
    # wait for threads to finish
    for thread in threads:
        thread.join()

    LOGGER.info("Multithreding logging.shutdown() works")


if __name__ == "__main__":
    main()
