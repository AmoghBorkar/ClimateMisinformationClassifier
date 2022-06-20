# This script connects to twitter for listening to global tweets and saves a large number into a json

import sys
import os
import tweepy
import numpy as np
import time
import json
from datetime import date, timedelta
import csv
import yaml

# Define keywords to filter on
filter_keywords = set(
    [
        "climate",
        "carbon",
        "global warming",
        "emissions",
        "carbon dioxide",
        "carbon monoxide",
        "net zero",
        "greenhouse",
        "methane",
        "air pollution",
        "air quality",
    ]
)

# Read the params YAML file
with open("twitter_scraper_params.yaml", "r") as yamlfile:
    data = yaml.load(yamlfile, Loader=yaml.FullLoader)
    print("Config Params Read successful")


# Define the twitter keys
consumer_key = data[0]["twitter_keys"]["consumer_key"]
consumer_secret = data[0]["twitter_keys"]["consumer_secret"]
access_token = data[0]["twitter_keys"]["access_token"]
access_token_secret = data[0]["twitter_keys"]["access_token_secret"]

# Define the bounding box & Path
BOUNDING_BOX = data[0]["bounding_box"]
JSON_FILE_PATH = data[0]["file_paths"]["json_file_path"]

temp_status = {}


class CustomStreamListener(tweepy.Stream):
    def on_status(self, status):
        if (
            len(status.text.encode("utf-8")) > 15
            and status.lang is not None
            and status.place is not None
        ):
            # Filter
            if any(word in status.text for word in filter_keywords):
                print(status.text)
                with open("tweet_data_" + str(status.id) + ".json", "w") as outfile:
                    json_str = json.dumps(status._json)
                    outfile.write(json_str)


def on_error(self, status_code):
    print(sys.stderr, "Encountered error with status code:", status_code)
    print("Sleeping for 20 seconds before proceeding.")
    time.sleep(20)
    return True


def on_timeout(self):
    print(sys.stderr, "Timeout...")
    print("Sleeping for 2 minutes before proceeding.")
    time.sleep(120)
    return True


def start_stream():
    os.chdir(JSON_FILE_PATH)
    while True:
        try:
            sapi = CustomStreamListener(
                consumer_key, consumer_secret, access_token, access_token_secret
            )
            sapi.filter(locations=BOUNDING_BOX)
        except KeyboardInterrupt:
            raise
        except Exception as ex:
            template = "An exception of type {0} occured. Arguments: \n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            print("re-setting the lists.")
            print("Other exception - Sleeping for 10 seconds before proceeding.")
            time.sleep(10)
            continue


if __name__ == "__main__":
    start_stream()

