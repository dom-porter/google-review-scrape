import concurrent.futures
import os
import sys
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import csv
import logging
import logging.handlers
from datetime import datetime
from time import sleep, gmtime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from google import Business

load_dotenv()  # take environment variables from .env.

# all_store_targets = [(207, 'Zara store UK E20 1EJ')] # 2550 reviews
# all_store_targets = [(202, 'Zara store UK BS1 3BX')] # 824 reviews
# all_store_targets = [(205, 'Zara  store UK NW4 3FP')] # 1074 reviews

all_targets = [(207, 'Zara store UK E20 1EJ'), (202, 'Zara store UK BS1 3BX'), (205, 'Zara store UK NW4 3FP'),
               (206, 'Zara store UK WD17 2TB')]

test = Business(201, "Zara store UK BS1 3BX")


# print(test.getBusinessDetails())

# print("Get reviews:")
# print(test.getReviews())
# print("Get business info:")
# print(test.getBusinessDetails())
# print("Get reviews:")
# test2 = pd.DataFrame(test.get_reviews())
# test2.to_csv("test" + ".csv", index=False)
# print("Get popular times:")
# print(test.get_popular_times())

# test = Business("Strumpet store UK HP34 789P")

# results = []
# for store in all_stores:
# ref, address = store
# g_maps_details = Business(ref, address)
# pd.DataFrame(g_maps_details.get_popular_times())
# results.append(pd.DataFrame(g_maps_details.get_popular_times()))

# stores_pt = pd.concat(results, ignore_index=True)
# stores_pt.to_csv("popular_times.csv", index=False)


def scrape_business(_business: tuple):
    ref, address = _business
    print(f"Scrape: {ref} - {address}")
    g_maps_details = Business(ref, address)
    return pd.DataFrame(g_maps_details.get_reviews())


all_results = []
# scrape and crawl
with ThreadPoolExecutor(max_workers=4) as executor:
    results_futures = {executor.submit(scrape_business, target): target for target in
                       all_targets}
    for future in concurrent.futures.as_completed(results_futures):
        result = results_futures[future]
        try:
            data = future.result()
            all_results.append(data)
        except Exception as exc:
            print(exc)

stores_pt = pd.concat(all_results, ignore_index=True)
stores_pt.to_csv(f"popular_times.csv", index=False)
