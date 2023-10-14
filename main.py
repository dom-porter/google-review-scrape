import concurrent.futures
import logging.handlers
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from time import gmtime, time

import pandas as pd
from dotenv import load_dotenv
from webdriver_manager.chrome import ChromeDriverManager

from google import Business

load_dotenv()  # take environment variables from .env.

MAX_THREADS = int(os.environ['G_MAP_THREADS'])
LOG_NAME = str(os.environ['G_MAPS_LOG_NAME'])
LOG_MAX_SIZE = int(os.environ['G_MAPS_LOG_SIZE'])
LOG_COUNT = int(os.environ['G_MAPS_LOG_COUNT'])
APP_NAME = "google.business.scrape"

# Configure logging
handler = logging.handlers.RotatingFileHandler(LOG_NAME,
                                               maxBytes=LOG_MAX_SIZE,
                                               backupCount=LOG_COUNT,
                                               encoding="utf-8")

formatter = logging.Formatter('%(asctime)s %(pathname)s %(name)-15s [%(process)s] [%(thread)d] [%(levelname)s] %(message)s')
formatter.converter = gmtime
handler.setFormatter(formatter)

# set the root logger level to error
logging.basicConfig(handlers=[handler],
                    level=logging.ERROR)

logger = logging.getLogger(APP_NAME)

if str(os.environ['G_MAPS_LOG_DEBUG']).upper() == "TRUE":
    logger.info("Debug logging enabled.")
    DEBUG = True
    log_level = logging.DEBUG
else:
    DEBUG = False
    log_level = logging.INFO

logger.setLevel(log_level)


def scrape_business(business_details: str, driver_path: str):
    ref, address = business_details.split(",")
    logger.debug(f"Scrape: {ref} - {address}")
    if ref and address:
        g_maps_details = Business(ref, address, driver_path)
        return_details = pd.DataFrame(g_maps_details.get_business_details(), index=[0])
        return_times = pd.DataFrame(g_maps_details.get_popular_times())
        return_reviews = pd.DataFrame(g_maps_details.get_reviews())
        return return_details, return_times, return_reviews
    else:
        return None


def main(input_filename: str, prefix: str):
    all_targets = read_file(input_filename)
    start_time = time()
    all_details = []
    all_times = []
    all_reviews = []

    # Do this to get round permissions error when installing the driver + spawn a new instance of the webdriver in each thread
    chrome_driver_path = ChromeDriverManager().install()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        results_futures = {executor.submit(scrape_business, target, chrome_driver_path): target for target in
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
            except Exception:
                logger.exception("Error while processing futures")

    output_details_pt = pd.concat(all_details, ignore_index=True)
    output_times_pt = pd.concat(all_times, ignore_index=True)
    output_reviews_pt = pd.concat(all_reviews, ignore_index=True)

    output_details_pt.to_csv(f"{prefix}_details.csv", index=False)
    output_times_pt.to_csv(f"{prefix}_popular_times.csv", index=False)
    output_reviews_pt.to_csv(f"{prefix}_reviews.csv", index=False)

    end_time = time()
    elapsed_time = end_time - start_time
    logger.info(f"Elapsed run time: {round(elapsed_time / 60, 2)} minutes")


def read_file(_filename: str) -> list:
    with open(_filename, newline='') as csvfile:
        all_lines = [stripped for line in csvfile if (stripped := line.strip())]
    return all_lines


if __name__ == '__main__':

    if len(sys.argv) != 3:
        print(sys.argv)
        print(
            'Enter the expected arguments <input.csv> <output file prefix>\n\nexample:\n\n python3 main.py target_details.csv 01_01_2023\n\n')
        exit()

    try:
        input_csv = sys.argv[1]
        output_prefix = sys.argv[2]

        logger.info("==================== Google Business Scrape 2.4 ====================")
        logger.info(f"Input file: {input_csv}")
        logger.info(f"Output file prefix: {output_prefix}")
        main(input_csv, output_prefix)

    except Exception as e:
        print(e)
        exit()
