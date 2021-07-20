import os

from setuptools import setup, find_packages

PROJECT_ROOT, _ = os.path.split(__file__)

NAME = "kafka-logging-handler"
EMAILS = "mbirger@redhat.com, mjahudko@readhat.com, rmonegro@redhat.com"
AUTHORS = "Mark Birger, Michaela Jahudkova, Robert Monegro"
VERSION = "0.2.5"

URL = "https://github.com/redhat-aqe/kafka-logging-handler"
LICENSE = "GPLv3"


SHORT_DESCRIPTION = "A Python logging handler library for Kafka consumers."

try:
    import pypandoc

    DESCRIPTION = pypandoc.convert(os.path.join(PROJECT_ROOT, "README.md"), "rst")
except (IOError, ImportError):
    DESCRIPTION = SHORT_DESCRIPTION

INSTALL_REQUIRES = (
    open(os.path.join(PROJECT_ROOT, "requirements.txt")).read().splitlines()
)

setup(
    name=NAME,
    version=VERSION,
    author=AUTHORS,
    author_email=EMAILS,
    entry_points={
        'console_script': ["kafka_logger_cli=cli:main"],
    },
    packages=find_packages(".", exclude=("tests")),
    install_requires=INSTALL_REQUIRES,
    url=URL,
    download_url="https://github.com/redhat-aqe/kafka-logging-handler/archive/{0}.tar.gz".format(
        VERSION
    ),
    description=SHORT_DESCRIPTION,
    long_description=DESCRIPTION,
    license=LICENSE,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Topic :: System :: Logging",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
