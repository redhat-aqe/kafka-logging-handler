#! /usr/bin/env python
import argparse
from kafka_logger import listen_channel_output
import configparser
from pprint import pprint


def main():
    parser = argparse.ArgumentParser(description="Decentralized logging using dweet server")

    parser.add_argument(
        "--settings_ini",
        type=str,
        required=True,
        help="Path to settings.ini, all the configuration should be written in the ini file",
    )

    args = parser.parse_args()
    try:
        confg = configparser.ConfigParser(
            allow_no_value=True,
            interpolation=configparser.ExtendedInterpolation()
        )
        confg.read(args.settings_ini)
        section = confg['KAFKA']
        topics = section.pop('topics').split(",")
        for topic, value in listen_channel_output(topics, **section):
            print(value['message'])
            if value['exc_info'] and value['exc_text']:
                print("------------------------------\n")
                print(value['exc_info'])
                print(value['exc_text'])
                print("\n\n")
            # pprint(message)
    except (KeyboardInterrupt, AttributeError) as e:
        print("Closing .....")


if __name__ == "__main__":
    main()
