"""Usage of kafka-logging-handler with multiprocessing and multithreading."""
# pylint: disable=global-statement
from concurrent.futures import ThreadPoolExecutor
import logging
import multiprocessing
from multiprocessing import Process
import os
import sys
import threading

from kafka_logger.handlers import KafkaLoggingHandler

REQUIRED_ENV_VARS = ["KAFKA_SERVER", "KAFKA_CERT", "KAFKA_TOPIC"]

MAIN_PROCESS_THREADS = 2
CHILD_PROCESSES = 2
THREAD_POOL_WORKERS = 2
THREAD_POOL_SIZE = 3

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


def grandchild_process():
    """Log a message."""
    LOGGER.info("Hi, I'm sub sub process %s", get_process_thread())


def child_process_with_grandchild():
    """Spawn grandchild process."""
    LOGGER.info("I'm going to spawn another child %s", get_process_thread())
    subprocess = Process(target=grandchild_process, name="Grandchild process")
    subprocess.start()
    subprocess.join()


def thread_worker(index):
    """Log a message."""
    LOGGER.info(
        "Hi, I'm a thread worker #%d in the child process thread pool %s",
        index,
        get_process_thread(),
    )


def child_process_with_threads():
    """Run thread executor pool."""
    LOGGER.info("I'm going to spawn multiple threads %s", get_process_thread())
    with ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS) as executor:
        for thread_idx in range(THREAD_POOL_SIZE):
            executor.submit(thread_worker, thread_idx)


def main_process_thread(index):
    """Log a message."""
    LOGGER.info(
        "Hi, I'm a thread #%d in the main process %s", index, get_process_thread()
    )


def child_process_with_exception():
    """Raise an exception after start."""
    LOGGER.info(
        "Hi, I'm child process that is going to raise exception %s",
        get_process_thread(),
    )
    raise Exception("This exception will not occur in Kafka!")


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

    # testing threads in the main process
    threads = []
    for idx in range(MAIN_PROCESS_THREADS):
        thread = threading.Thread(
            target=main_process_thread,
            name="Thread of the main process #{}".format(idx),
            args=(idx,),
        )
        threads.append(thread)
        thread.start()
    # wait for threads to finish
    for thread in threads:
        thread.join()

    # there is a chance of logs loss
    # if the main process terminates without joining child processes
    for child in child_processes:
        child.join()

    # test if a child of a child process logs correctly
    child_with_subprocess = Process(
        target=child_process_with_grandchild,
        name="Child process that spawns another child",
    )
    child_with_subprocess.start()
    child_with_subprocess.join()

    # test threads in a child process
    child_with_threads = Process(
        target=child_process_with_threads, name="Child process that has a thread pool"
    )
    child_with_threads.start()
    child_with_threads.join()

    # test unhandled exception in a child process
    child_exception = Process(
        target=child_process_with_exception, name="Child process with an exception"
    )
    child_exception.start()
    child_exception.join()

    # top-level exception works only in the main process
    raise Exception("Testing top-level exception!")


if __name__ == "__main__":
    main()
