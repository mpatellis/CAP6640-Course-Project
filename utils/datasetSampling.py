#
# This script will walk through all the tweet id files and
# hydrate them with twarc. The line oriented JSON files will
# be placed right next to each tweet id file.
#
# Note: you will need to install twarc, tqdm, and run twarc configure
# from the command line to tell it your Twitter API keys.
#
# Special thanks to Github users edsu and SamSamhuns for contributing to this file. This file was repurposed from our other
# data repository on COVID-19 related tweets : https://github.com/echen102/COVID-19-TweetIDs
#

import gzip
import json
import os
from random import sample
import time
from tqdm import tqdm
from twarc import Twarc
from pathlib import Path

#Twitter API Credentials
TWITTER_KEY = 'czYBxNJbhE8CxzjRyICkBYemy' 
TWITTER_SECRET_KEY = 'EV0AmVQ5aYhw4OB9oFFQb6UQfIrLH2fhuRUWPs4EqBquAp83T5' 

ACCESS_TOKEN = TWITTER_KEY 
ACCESS_TOKEN_SECRET = TWITTER_SECRET_KEY

data_dirs = ['C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-01',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-02',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-03',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-04',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-05',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-06',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-07',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-08',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-09',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-10',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-11',
'C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\us-pres-elections-2020\\2020-12']

# Global id samples
sample_set = []

def main():
    #for data_dir in data_dirs:
    #    for path in Path(data_dir).iterdir():
    #        print("Sampling from: " + path.name)
    #        if path.name.endswith('.txt'):
    #            hydrate(path)

    #with open('C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\full_test_set100.txt', mode='wt', encoding='utf-8') as f:
    #    for line in sample_set:
    #        f.write(str(line))
    #        f.write('\n')

    hydrate_ids()

def hydrate(id_file):
    #print('hydrating {}'.format(id_file))

    with open(id_file) as f:
        content = f.readlines()
    # Remove new line characters
    content = [x.strip() for x in content]

    # Randomly sample 100 id's
    random_ids = sample(content, min(100, len(content)))

    sample_set.extend(random_ids)

    #with gzip.open(gzip_path, 'w') as output:
    #    with tqdm(total=100) as pbar:
    #        for tweet in random_ids:
    #            hydrated_tweet = twarc.hydrate(tweet)
    #            print(hydrated_tweet)
    #            output.write(json.dumps(hydrated_tweet).encode('utf8') + b"\n")
    #            pbar.update(1)

def ids():
    for id in open('C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\full_test_set100.txt'):
        yield id

def hydrate_ids():
    now = time.time()
    with tqdm(total=876000) as pbar:       
        with gzip.open('C:\\Users\\jorda\Desktop\\CAP6640-Course-Project\\data\\full_tweets100.jsonl.gz', 'w') as output:
            for tweet in twarc.hydrate(ids()):
                output.write(json.dumps(tweet).encode('utf8') + b"\n")
                if (time.time() - now) >= 600:
                    time.sleep(900)
                    now = time.time()
                pbar.update(1)

API_KEY = "fE9yYjZeFwQm3u01RjQMOj185"
API_SECRET_KEY = "gyBxrwWExHVnh8dg5usyDqR54x74NlSTRyuVrhTAQ0HQ13cF8n"
bearer_token = "AAAAAAAAAAAAAAAAAAAAABhVMgEAAAAAQPOVpqeAaWOYYijnplo32mxfA%2BY%3Dz9akGBVSlP9Bv5FcOA2ZEGFHygQ82xXd2mwLFWk8OLY5RLPTKY"
ACCESS_TOKEN = "2861672845-wla1CvSnNnbHFdIMWwPS11L4s9CSeQoORlfl6Kb"
ACCESS_TOKEN_SECRET = "JcqW0vOZdWdn4DzMhMlx9vnNivyEXXcdIafViAIzGT7yX"

if __name__ == "__main__":
    twarc = Twarc(API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    main()

