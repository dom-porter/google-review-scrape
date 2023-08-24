import concurrent.futures
import logging.handlers
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from time import gmtime, time
import asyncio
from asyncio import Task
from typing import List

import pandas as pd
from dotenv import load_dotenv
from selenium.webdriver.chrome.service import Service as ChromeService, Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver

from google import Business

load_dotenv()  # take environment variables from .env.

if str(os.environ['G_MAPS_LOG_DEBUG']).upper() == "TRUE":
    __DEBUG__ = True
    log_level = logging.DEBUG
else:
    __DEBUG__ = False
    log_level = logging.INFO

# Configure logging
handler = logging.handlers.RotatingFileHandler(str(os.environ['G_MAPS_LOG_NAME']),
                                               maxBytes=int(os.environ['G_MAPS_LOG_SIZE']),
                                               backupCount=int(os.environ['G_MAPS_LOG_COUNT']),
                                               encoding="utf-8")

formatter = logging.Formatter('%(asctime)s %(pathname)s %(name)-15s [%(process)s] [%(thread)d] [%(levelname)s] %(message)s')
formatter.converter = gmtime
handler.setFormatter(formatter)

# set the root logger level to error
logging.basicConfig(handlers=[handler],
                    level=logging.ERROR)

logger = logging.getLogger(str(os.environ['APP_NAME']))
logger.setLevel(log_level)


def scrape_business(_business: str, chrome_service):
    ref, address = _business.split(",")
    logger.debug(f"Scrape: {ref} - {address}")
    if ref and address:
        g_maps_details = Business(ref, address, chrome_service)
        return_details = pd.DataFrame(g_maps_details.get_business_details(), index=[0])
        return_times = pd.DataFrame(g_maps_details.get_popular_times())
        return_reviews = pd.DataFrame(g_maps_details.get_reviews())
        return return_details, return_times, return_reviews
    else:
        return None


def main(_input_csv: str, _output_prefix: str, _mode: int):
    # chrome_service =ChromeService(ChromeDriverManager().install())
    all_targets = read_file(_input_csv)
    start_time = time()
    all_details = []
    all_times = []
    all_reviews = []
    chrome_service = ChromeService(ChromeDriverManager().install())
    with ThreadPoolExecutor(max_workers=4) as executor:
        results_futures = {executor.submit(scrape_business, target, chrome_service): target for target in
                           all_targets}
        for future in concurrent.futures.as_completed(results_futures):
            try:
                data = future.result()
                business_details, popular_times, reviews = data
                if business_details is not None:
                    all_details.append(business_details)
                if popular_times is not None:
                    all_times.append(popular_times)
                if reviews is not None:
                    all_reviews.append(reviews)
            except Exception as exc:
                logger.exception("Error while processing futures")
                logger.debug(exc)


    output_details_pt = pd.concat(all_details, ignore_index=True)
    output_times_pt = pd.concat(all_times, ignore_index=True)
    output_reviews_pt = pd.concat(all_reviews, ignore_index=True)

    output_details_pt.to_csv(f"{_output_prefix}_details.csv", index=False)
    output_times_pt.to_csv(f"{_output_prefix}_popular_times.csv", index=False)
    output_reviews_pt.to_csv(f"{_output_prefix}_reviews.csv", index=False)

    end_time = time()
    elapsed_time = end_time - start_time
    logger.info(f"Elapsed run time: {round(elapsed_time / 60, 2)} minutes")


def read_file(_filename: str) -> list:
    with open(_filename, newline='') as csvfile:
        all_lines = [stripped for line in csvfile if (stripped := line.strip())]
    return all_lines


if __name__ == '__main__':

    if len(sys.argv) != 4:
        print(sys.argv)
        print(
            'Enter the expected arguments <input.csv> <output.csv <mode>\n\nexample "python3 main.py target_details.csv reviews.csv 3"\n\n')
        # exit()

    try:
        # input_csv = sys.argv[1]
        # output_csv = sys.argv[2]
        # mode = int(sys.argv[3])

        input_csv = "test.csv"
        output_csv = "output"

        # main(input_csv, output_csv, mode)
        logger.info("==================== Google Business Scrape 1.0 ====================")
        logger.info(f"Input file: {input_csv}")
        logger.info(f"Output file prefix: {output_csv}")
        main(input_csv, output_csv, 1)

    except Exception as e:
        print(e)
        exit()

# 202,Zara store UK BS1 3BX
# 205,Zara store UK NW4 3FP
# 206,Zara store UK WD17 2TB
# 207,Zara store UK E20 1EJ