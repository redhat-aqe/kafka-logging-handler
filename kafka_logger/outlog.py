import io
import contextlib
import logging
from typing import AnyStr
import six


class singleton(type):
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instances[cls]


@six.add_metaclass(singleton)
class OutputLogger(io.StringIO):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """

    def __init__(
        self,
        logger: logging.Logger,
        level: int = logging.INFO, initial_value: str = '', newline: str = '\n',
    ):
        """ Initial configuration to redirect stdout to Logger, should be used with context manager

        :param logger: [description], logging.Logger Object, we need this to redirect output
        :param level: [description], defaults to logging.INFO
        :param initial_value: [description], defaults to ''
        :param newline: [description], defaults to '\n'
        """
        super(OutputLogger, self).__init__(initial_value=initial_value, newline=newline)
        self.logger = logger
        self.level = level
        self.linebuf = ''
        self.name = self.logger.name
        self._redirector = contextlib.redirect_stdout(self)

    def write(self, buf: AnyStr):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line)

    def __enter__(self):
        self._redirector.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._redirector.__exit__(exc_type, exc_value, traceback)
