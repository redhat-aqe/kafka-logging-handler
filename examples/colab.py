# !pip install git+https://github.com/sandyz1000/kafka-logging-handler.git@devel
# !pip install git+https://github.com/sandyz1000/colabshell.git

# %%
import os
import sys
sys.path.append(
    os.path.realpath(os.path.join(os.path.dirname(__name__), os.path.pardir))
)
from kafka_logger import init_kafka_logger, OutputLogger
from colabshell import ColabShell

# %%
logger = init_kafka_logger("kafka.logger", "../kafka_logger/settings.ini")
# %%
logger.info("Initializing decentralized logging")
with OutputLogger(logger) as redirect:
    print("From redirector")
    shell = ColabShell(port=10001, username='colabshell', password='password')
    shell.run()

# %%
