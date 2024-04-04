import re
import ast
import sys
import json
import time
import random
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from dateutil.parser import parse as parseDate
from dateutil.parser import ParserError

PATH_TO_UTILS = "../"  # change based on your directory structure
sys.path.append(PATH_TO_UTILS)

from utils import check_mf_formatting
wd_entries = pd.read_csv("wikidata_entries.tsv", delimiter="\t", low_memory=False)
wd_entries.tail()
def make_request(url, max_retries=3, initial_backoff=2, multiplier=2, max_backoff=16, **request_params):
    retries = 0
    backoff = initial_backoff

    while retries < max_retries:
        try:
            response = requests.get(url, **request_params)
            if response.status_code != 404:
                response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            retries += 1
            if retries < max_retries:
                print(f"Retrying in {backoff} seconds (retry {retries}/{max_retries})")
                time.sleep(backoff)
                backoff = min(backoff * multiplier, max_backoff)
    
    raise Exception(f"Max retries reached, could not complete request for {url}")

# test
# make_request("http://foobar.com/")
# Adapted from https://www.zenrows.com/blog/user-agent-web-scraping
# More here https://useragentstring.com/pages/useragentstring.php
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15"
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15"
]
base_url = "https://www.findagrave.com/memorial/"
try:
    acc = pd.read_csv('findagrave_entries.csv').to_dict(orient="records")
except:
    acc = []
i = len(acc) - 1

def construct_row(id, newId=None):
    headers = {'User-Agent': random.choice(user_agents)}
    res = make_request(base_url + str(newId if newId else id), headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    if res.status_code == 404:
        a = soup.find("a", string=" See Merged Memorial")
        if a is None:
            # Memorial has been removed
            return {
                "?findAGraveID": id,
                "!newId": ""
            }
        return construct_row(id, a.get("href").split('/')[2])
    s = soup.find_all(attrs={'aria-labelledby': 'siblingLabel'})
    try:
        birthdays = parseDate(soup.find(id="birthDateLabel").text).isoformat() + 'Z'
    except ParserError:
        print("could not parse", soup.find(id="birthDateLabel").text)
        birthdays = np.NaN
    except AttributeError as e:
        print(res.text)
        raise e
    try:
        deathdays = parseDate(re.sub(r"\([^)]+\)", "", soup.find(id="deathDateLabel").text)).isoformat() + 'Z'
    except ParserError:
        print("could not parse", soup.find(id="deathDateLabel").text)
        deathdays = np.NaN
    return {
        "?findAGraveID": id,
        "!newId": newId,
        "?name": soup.find(id="bio-name").find(string=True).strip(),
        "?birthdays": birthdays,
        "?birthplaces": soup.find(id="birthLocationLabel").text.strip() if soup.find(id="birthLocationLabel") else np.NaN,
        "?deathdays": deathdays,
        "?deathplaces": soup.find(id="deathLocationLabel").text.strip() if soup.find(id="deathLocationLabel") else np.NaN,
        "?burials": soup.find(id="cemeteryNameLabel").text.strip() if soup.find(id="cemeteryNameLabel") else 
            (re.sub("[ \n]+", " ", soup.find(id="cemeteryCountryName").parent.text.strip()) if soup.find(id="cemeteryCountryName") else np.NaN),
        "?plots": soup.find(id="plotValueLabel").text.strip() if soup.find(id="plotValueLabel") is not None else np.NaN,
        "?siblings": ';'.join(list(map(lambda elem: re.sub(" +", " ", elem.find("h3", recursive=True).text.strip()), soup.find_all(attrs={'aria-labelledby': 'siblingLabel'}))))
    }

for id in tqdm(wd_entries["?findAGraveID"][len(acc):]):
    i += 1
    try:
        acc.append(construct_row(id))
    except Exception as e:
        print(base_url + str(id), "idx", i)
        raise e
    finally:
        pd.DataFrame(acc).to_csv("findagrave_entries.csv", index=False)

acc[0]
