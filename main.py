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
from time import sleep, gmtime, time
from dotenv import load_dotenv
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
                                               backupCount=int(os.environ['G_MAPS_LOG_COUNT']))

formatter = logging.Formatter('%(asctime)s %(pathname)s %(name)-15s [%(process)s] [%(levelname)s] %(message)s')
formatter.converter = gmtime
handler.setFormatter(formatter)

# set the root logger level to error
logging.basicConfig(handlers=[handler],
                    level=logging.ERROR)

logger = logging.getLogger(str(os.environ['APP_NAME']))
logger.setLevel(log_level)


def scrape_business(_business: list):
    ref, address = _business
    logger.debug(f"Scrape: {ref} - {address}")
    g_maps_details = Business(ref, address)
    return pd.DataFrame(g_maps_details.get_reviews())


def main(_input_csv: str, _output_csv: str, _mode: int):

    with open(_input_csv, newline='') as csvfile:
        all_targets = csv.reader(csvfile, delimiter=',', quotechar='|')

        start_time = time()
        all_results = []
        # scrape and crawl
        with ThreadPoolExecutor(max_workers=4) as executor:
            results_futures = {executor.submit(scrape_business, target): target for target in
                               all_targets}
            for future in concurrent.futures.as_completed(results_futures):
                try:
                    data = future.result()
                    all_results.append(data)
                except Exception as exc:
                    print(exc)

        stores_pt = pd.concat(all_results, ignore_index=True)
        stores_pt.to_csv(_output_csv, index=False)

        end_time = time()
        elapsed_time = end_time - start_time
        logger.info(f"Elapsed run time: {round(elapsed_time/60, 2)} minutes")


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

        # main(input_csv, output_csv, mode)
        main("test.csv", "output.csv", 1)

    except Exception as e:
        print(e)
        exit()
