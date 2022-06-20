# This script generated the config file for using the script locally later
import yaml

# Define the parameters - create a twitter developer account, create an app and get the keys below.
consumer_key = ""
consumer_secret = ""
access_key = ""
access_secret = ""


# Use this to define the box for country - https://gist.github.com/graydon/11198540
# bounding_box = [-7.57216793459, 49.959999905, 1.68153079591, 58.6350001085]
bounding_box = [-179.903322, -58.747096, 177.609363, 74.279311]
# GEBOX_Indonesia = [05.82,-6.68,119.86,-4.76]
JSON_FILE_PATH = "./Data/JSON"
CSV_FILE_PATH = "./Data/CSV"


# Create the YAML file
twitter_params = [
    {
        "twitter_keys": {
            "consumer_key": consumer_key,
            "consumer_secret": consumer_secret,
            "access_token": access_key,
            "access_token_secret": access_secret,
        },
        "bounding_box": bounding_box,
        "file_paths": {
            "json_file_path": JSON_FILE_PATH,
            "csv_file_path": CSV_FILE_PATH,
        },
    }
]

with open("twitter_scraper_params.yaml", "w") as yamlfile:
    data = yaml.dump(twitter_params, yamlfile)
    print("Write successful")
