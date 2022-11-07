import os
import sys
import MySQLdb
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


load_dotenv()  # take environment variables from .env.

# Constants
GOOGLE_URL = "https://www.google.com"
STORED_PROCEDURE = "sp_ll_store_scrape_reviews_get"
STORE_OUTPUT_FILE_PREFIX = "stores_"
POPULAR_TIMES_OUTPUT_FILE_PREFIX = "popular_times_"
REVIEWS_OUTPUT_FILE_PREFIX = "reviews_"
REVIEW_COUNT_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[2]/div/div[2]/div[2]'
SORT_ORDER_BUTTON_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div[7]/div[2]/button'
REVIEW_SCROLL_DIV = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]'

# For more log messages enable debug
if str(os.environ['G_REVIEWS_LOG_DEBUG']).upper() == "TRUE":
    __DEBUG__ = True
    log_level = logging.DEBUG
else:
    __DEBUG__ = False
    log_level = logging.INFO

# restrict to a single target store use single store
__SINGLE_STORE__ = False

connection = MySQLdb.connect(
    host=os.environ['DB_MYSQL_HOST'],
    user=os.environ['DB_MYSQL_USER'],
    passwd=os.environ['DB_MYSQL_PASS'],
    db=os.environ['DB_MYSQL_DATABASE'],
    port=int(os.environ['DB_MYSQL_PORT']),
    local_infile=1,
    autocommit=True,
)

# Configure logging
handler = logging.handlers.RotatingFileHandler(str(os.environ['G_REVIEWS_LOG_NAME']),
                                               maxBytes=int(os.environ['G_REVIEWS_LOG_SIZE']),
                                               backupCount=int(os.environ['G_REVIEWS_LOG_COUNT']))

formatter = logging.Formatter('%(asctime)s %(pathname)s %(name)-15s [%(process)s] [%(levelname)s] %(message)s')
formatter.converter = gmtime
handler.setFormatter(formatter)

# set the basic logging level to error so selenium doesn't fill up the logs
logging.basicConfig(handlers=[handler],
                    level=logging.ERROR)

logger = logging.getLogger(str(os.environ['APP_NAME']))
logger.setLevel(log_level)

# Globals
cursor = connection.cursor()
rowCount_Stores = 0
rowCount_Times = 0
rowCount_Reviews = 0
filename_Stores = ""
filename_Times = ""
filename_Reviews = ""


def deleteFile(_filename: str):
    try:
        if os.path.exists(_filename):
            os.remove(_filename)
    except:
        logger.exception(f"Unable to delete {_filename}")


def checkStoreDetailsUpload(_batch_no):
    global cursor

    check_query = f"CALL rawData.`sp_load_complete`('store_details',{_batch_no},1,{rowCount_Stores})"
    cursor.execute(check_query)
    fetchall_value = cursor.fetchall()
    if not fetchall_value[0][0] == 0:
        deleteFile(filename_Stores)
        logger.error(f"Failed database row count check for {filename_Stores}")
    else:
        logger.debug(f"Success database row count check for {filename_Stores}")


def checkTimesUpload(_batch_no: int):
    global cursor

    check_query = f"CALL rawData.`sp_load_complete`('store_busy',{_batch_no},1,{rowCount_Times})"
    cursor.execute(check_query)
    fetchall_value = cursor.fetchall()
    if not fetchall_value[0][0] == 0:
        deleteFile(filename_Times)
        logger.error(f"Failed database row count check for {filename_Times}")
    else:
        logger.debug(f"Success database row count check for {filename_Times}")


def checkReviewsUpLoad(_batch_no: int):
    global cursor

    check_query = f"CALL rawData.`sp_load_complete`('store_reviews',{_batch_no},1,{rowCount_Reviews})"
    cursor.execute(check_query)
    fetchall_value = cursor.fetchall()
    if not fetchall_value[0][0] == 0:
        deleteFile(filename_Reviews)
        logger.error(f"Failed database row count check for {filename_Reviews}")
    else:
        logger.debug(f"Success database row count check for {filename_Reviews}")


def getStoreDetails(_store_id: int, _driver: webdriver.Chrome) -> pd.DataFrame:
    response = BeautifulSoup(_driver.page_source, 'html.parser')

    # Get Service Options
    Service_options_parent = response.find('c-wiz', class_='u1M3kd W2lMue').text
    Service_options = ", ".join(
        Service_options_parent.replace(response.find('span', class_='d2aWRb').text, "").replace("\xa0",
                                                                                                "").split(
            u'\u00B7')),
    store = {
        'store_id': _store_id,
        # Get Store Name
        'store_name': response.find('h2', class_='qrShPb kno-ecr-pt PZPZlf q8U8x').span.text,
        # Get Address
        'address': response.find('span', class_='LrzXr').text,
        # Get Average Score
        'avg_rating': response.find('span', class_='Aq14fc').text.split()[0],
        # Get total Review
        'total_reviews': response.find('span', class_='hqzQac').text.split()[0],
        # Get Service Options

        'Service_options': Service_options,
        # People Spending time
        'avg_time_spent': response.find('span', class_='ffc9Ud').text
    }
    sleep(2)
    return pd.DataFrame(store, index=[0])


def getPopularTimes(_store_id: int, _driver: webdriver.Chrome) -> pd.DataFrame:
    # Store Popular time data
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    popular_times = []

    # Get the known fixed h2 element that contains the text "Popular times"
    # get the parent of the element and perform a search for the drop-down button
    # this should account for structure changes in the page
    popular_times_heading = _driver.find_element(By.XPATH, "//h2[contains(text(), 'Popular times')]")
    parent = popular_times_heading.parent
    drop_down = parent.find_element(By.CLASS_NAME, "goog-menu-button-dropdown")

    for day in days:
        # Find Drop down class
        logger.debug(f"Processing: {day}")

        drop_down.click()
        sleep(2)
        # Find option
        option = _driver.find_element(By.ID, ':' + str(days.index(day)))
        option.click()

        response = BeautifulSoup(_driver.page_source, 'html.parser')
        # Find Graph/Image Div
        graph_div = response.find('div', class_='g2BVhd eoFzo')
        # Find all hour data div
        hour_data = graph_div.find_all("div", class_="dpoVLd")
        # Get percent of busy for each hour
        for each_hour in hour_data:
            text = each_hour.get('aria-label').split()
            logger.debug(f"Processing: {text}")
            if text[0] != 'Currently':
                popular_time_day = {
                    'store_id': _store_id,
                    'perc_busy': text[0].replace("%", ''),
                    'hour_no': int(text[3]) if (text[4] == "am." or int(text[3]) == 12) else int(text[3]) + 12,
                    'each_hour': each_hour.get('aria-label'),
                    'day_of_week': day
                }
                popular_times.append(popular_time_day)
            sleep(2)

    return pd.DataFrame(popular_times)


def getReviews(_store_id: int, _driver: webdriver.Chrome) -> pd.DataFrame:
    count_parent = _driver.find_element(By.CLASS_NAME, "jANrlb")
    review_count = count_parent.find_element(By.XPATH, "//div[contains(text(), 'reviews')]").text.split(" ")[0]
    review_count = review_count.replace(",", "")

    # Adjust the sort order of the reviews to most recent
    _driver.find_element(By.XPATH, "//button[@aria-label='Sort reviews']").click()
    WebDriverWait(_driver, 10).until(EC.visibility_of_all_elements_located((By.XPATH, "//li[@role='menuitemradio']")))
    _driver.find_element(By.XPATH, "(//li[@role='menuitemradio'])[2]").click()
    WebDriverWait(_driver, 10).until(EC.visibility_of_all_elements_located((By.XPATH, REVIEW_SCROLL_DIV)))

    # ===============================================================================================================
    #  ISSUE: Expanding the reviews scrollable div when the number of reviews exceeds apx 1000 the browser
    #         will return an error "call stack size exceeded" and stop expanding the div.
    #         It looks like the amount of data is too much for the browser.
    #         This link appears to show a way to do it, but it requires a 3rd party api:
    #         https://stackoverflow.com/questions/53749984/selenium-python-unable-to-scroll-down-while-fetching-google-reviews
    #
    # Workaround: Limit the reviews returned to 1000 amd only return the most recent.

    scrollable_div = _driver.find_element(By.XPATH, REVIEW_SCROLL_DIV)
    logger.debug(f"Total reviews: {review_count}")
    if int(review_count) >= 1000:
        scroll_end = 1000
        logger.debug(f"Total reviews exceeds 1000, script is limiting the scrape to 1000 reviews")
    else:
        scroll_end = int(review_count)

    all_items = _driver.find_elements(By.CLASS_NAME, 'jftiEf.fontBodyMedium')
    while len(all_items) < scroll_end:
        _driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
        WebDriverWait(_driver, 10).until(EC.visibility_of_all_elements_located((By.XPATH, REVIEW_SCROLL_DIV)))
        all_items = _driver.find_elements(By.CLASS_NAME, 'jftiEf.fontBodyMedium')

    process_count = 1
    rev_dict = {
        'store_id': [],
        'reviewer_name': [],
        'rating': [],
        'reviewed_dt': [],
        'review': []}

    for item in all_items:
        logger.debug(f"Processing: {item.accessible_name} {process_count} of {len(all_items)}")
        # Check if review has been shortened then click the More button
        more_buttons = item.find_elements(By.CLASS_NAME, 'w8nwRe.kyuRq')
        for button in more_buttons:
            try:
                button.click()
                sleep(1)
            except:
                logger.error(f"Unable to expand shortened review for {item.accessible_name}")

        html_item = item.get_attribute("outerHTML")
        bs_item = BeautifulSoup(html_item, 'html.parser')
        Reviewer_name = bs_item.find('div', class_='d4r55').text
        review_rate = bs_item.find('span', class_='kvMYJc')["aria-label"]
        review_time = bs_item.find('span', class_='rsqaWe').text
        review_text = bs_item.find('span', class_='wiI7pd').text
        rev_dict['store_id'].append(_store_id)
        rev_dict['reviewer_name'].append(Reviewer_name)
        rev_dict['rating'].append(review_rate)
        rev_dict['reviewed_dt'].append(review_time)
        rev_dict['review'].append(review_text)
        process_count += 1

    return pd.DataFrame(rev_dict)


def sp_glg_store_scrape(_batch_no: int, _mode: int):
    global cursor
    global rowCount_Stores
    global rowCount_Times
    global rowCount_Reviews
    global filename_Stores
    global filename_Times
    global filename_Reviews

    dfListStores = []
    dfListPopularTimes = []
    dfListReviews = []
    store_id = ""
    store_address = ""

    logger.info(f"Google store review scrape, batch no. {_batch_no}, mode {_mode}")
    if __DEBUG__:
        logger.debug("Debug logging enabled")

    try:
        # For debug purposes just use a single store details
        if __SINGLE_STORE__:
            all_store_targets = [(207, 'Zara store UK E20 1EJ')] # 2550 reviews
            # all_store_targets = [(202, 'Zara store UK BS1 3BX')] # 824 reviews
            # all_store_targets = [(205, 'Zara  store UK NW4 3FP')] # 1074 reviews
        else:
            # get the store info to scrape
            cursor.callproc(STORED_PROCEDURE, [_batch_no])
            all_store_targets = cursor.fetchall()

        logger.info(f"Fetching details for {len(all_store_targets)} store(s)")
        for store_details in all_store_targets:

            # instantiate a chrome options object, so you can set the size and headless preference
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--window-size=1920x1080")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-extensions")

            # For The Headless
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

            try:
                store_id = store_details[0]
                store_address = store_details[1]
                logger.info(f"Processing: [{store_id}] {store_address}")

                Google_place = store_address
                # Open Google chrome
                driver.get(GOOGLE_URL)

                # Test for the Google consent popup for cookies and click "Accept All"
                try:
                    consent = driver.find_element(By.ID, "L2AGLb")
                    if consent is not None:
                        consent.click()
                except:
                    logger.debug("No Google consent popup")

                # identify search box
                search_box = driver.find_element("name", "q")
                # enter search text
                search_box.send_keys(Google_place)
                sleep(2)
                # perform Google search with Keys.ENTER
                search_box.send_keys(Keys.ENTER)

                # mode
                # 1 = Store details + Reviews + Popular times
                # 2 = Popular times
                # 3 = Reviews

                if _mode == 1:
                    logger.info(f"Getting store details...")
                    dfListStores.append(getStoreDetails(store_id, driver))

                    # Switch to Google Maps
                    google_map = driver.find_element(By.CLASS_NAME, "lu-fs")
                    google_map.click()
                    sleep(2)

                if _mode == 1 or _mode == 2:
                    logger.info(f"Getting popular times...")

                    if not _mode == 1:
                        # Switch to Google Maps
                        google_map = driver.find_element(By.CLASS_NAME, "lu-fs")
                        google_map.click()
                        sleep(2)
                    dfListPopularTimes.append(getPopularTimes(store_id, driver))

                if _mode == 1 or _mode == 3:
                    logger.info(f"Getting reviews...")

                    if not _mode == 1:
                        # Switch to Google Maps
                        google_map = driver.find_element(By.CLASS_NAME, "lu-fs")
                        google_map.click()
                        sleep(2)

                    # Click the reviews link at the top of the left pane to go to the review details pane
                    review = driver.find_element(By.CLASS_NAME, "DkEaL")
                    review.click()
                    sleep(2)
                    dfListReviews.append(getReviews(store_id, driver))

            except Exception as e:
                logger.exception(f"Unable to fetch details for the store - ({store_id}) {store_address}")
            finally:
                driver.close()
                driver.quit()

        if len(dfListStores) > 0:
            rowCount_Stores = len(dfListStores)
            filename_Stores = STORE_OUTPUT_FILE_PREFIX + str(_batch_no) + ".csv"
            stores_df = pd.concat(dfListStores, ignore_index=True)
            stores_df.to_csv(filename_Stores, index=False)
            try:
                logger.debug(f"Uploading {filename_Stores} to database")
                upload_query = f"""LOAD DATA LOCAL INFILE '{filename_Stores}' INTO TABLE rawData.imp_stores FIELDS TERMINATED BY ',' ESCAPED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES (store_id, store_name, address, avg_rating, total_reviews, service_options, avg_time_spent) SET batch_no = {_batch_no};"""
                cursor.execute(upload_query)
            except Exception as e:
                logger.exception(f"Unable to upload {filename_Stores} to database")
            checkStoreDetailsUpload(_batch_no)

        if len(dfListPopularTimes) > 0:
            rowCount_Times = len(dfListPopularTimes)
            filename_Times = POPULAR_TIMES_OUTPUT_FILE_PREFIX + str(_batch_no) + ".csv"
            times_df = pd.concat(dfListPopularTimes, ignore_index=True)
            times_df.to_csv(POPULAR_TIMES_OUTPUT_FILE_PREFIX + str(_batch_no) + ".csv", index=False)
            try:
                logger.debug(f"Uploading {filename_Times} to database")
                upload_query = f"""LOAD DATA LOCAL INFILE '{filename_Times}' INTO TABLE rawData.imp_store_busy_times FIELDS TERMINATED BY ',' ESCAPED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES (store_id, perc_busy, hour_no, each_hour, day_of_week) SET batch_no = {_batch_no};"""
                cursor.execute(upload_query)
            except Exception as e:
                logger.exception(f"Unable to upload {filename_Times} to database")
            checkTimesUpload(_batch_no)

        if len(dfListReviews) > 0:
            rowCount_Reviews = len(dfListReviews)
            filename_Reviews = REVIEWS_OUTPUT_FILE_PREFIX + str(_batch_no) + ".csv"
            dfReviews = pd.concat(dfListReviews, ignore_index=True)
            dfReviews.to_csv(REVIEWS_OUTPUT_FILE_PREFIX + str(_batch_no) + ".csv", index=False)
            try:
                logger.debug(f"Uploading {filename_Reviews} to database")
                upload_query = f"""LOAD DATA LOCAL INFILE '{filename_Reviews}' INTO TABLE rawData.imp_store_reviews FIELDS TERMINATED BY ',' ESCAPED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES (store_id, reviewer_name, rating, reviewed_dt, review ) SET batch_no = {_batch_no};"""
                cursor.execute(upload_query)
            except Exception as e:
                logger.exception(f"Unable to upload {filename_Reviews} to database")
            checkReviewsUpLoad(_batch_no)

    except Exception as e:
        logger.exception("Uncaught exception during script")
    finally:
        cursor.close()
        cursor = connection.cursor()

    logger.info(f"Completed running of batch no. {_batch_no}")


def truncate_csv(file):
    f = open(file, "w+")
    f.close()


def write_csv(file, row):
    f = open(file, 'a', newline='')
    writer = csv.writer(f)
    if os.stat(file).st_size == 0:
        writer.writerow(['run_datetime', 'app_name', 'instance_id', 'status'])
    writer.writerow(row)
    f.close()


def sp_process_logging(instance_id, status, error_message):
    cursor.callproc("adminProcessLog.sp_process_logging", [instance_id, status, error_message])


def dump_logs_to_db():
    query = f"LOAD DATA LOCAL INFILE '{os.environ['LOG_FILE_NAME']}'  INTO TABLE adminProcessLog.ad_app_recovery FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (run_datetime, app_name, instance_id, status) ;"
    cursor.execute(query)


if len(sys.argv) != 4:
    print(sys.argv)
    print(
        'Instance Id should be passed asa integer command line parameter, an instance id -1 dumps logs to database, and truncates logs \n\nexample "python script.py 1 599 1"\n\n')
    exit()

try:
    instance_id = int(sys.argv[1])
    batch_no = int(sys.argv[2])
    mode_no = int(sys.argv[3])
except Exception as e:
    print(e)
    exit()

status = 1
error = None
row = [str(datetime.utcnow()), os.environ['APP_NAME'], instance_id, "started"]
write_csv(os.environ['LOG_FILE_NAME'], row)

if int(instance_id) == -1:
    try:
        dump_logs_to_db()
        truncate_csv(os.environ['LOG_FILE_NAME'])
        sp_process_logging(instance_id, status, "recovery")
    except Exception as e:
        row = [str(datetime.utcnow()), os.environ['APP_NAME'], instance_id, "error"]
        write_csv(os.environ['LOG_FILE_NAME'], row)
        status = 0
        error = "error"
else:
    try:
        # -------------------------------------------------------------
        # add application code here.
        sp_glg_store_scrape(batch_no, mode_no)
        # -------------------------------------------------------------
    except Exception as e:
        row = [str(datetime.utcnow()), os.environ['APP_NAME'], instance_id, "error"]
        write_csv(os.environ['LOG_FILE_NAME'], row)
        status = 0
        error = "error"

try:
    row = [str(datetime.utcnow()), os.environ['APP_NAME'], instance_id, "complete"]
    write_csv(os.environ['LOG_FILE_NAME'], row)
    sp_process_logging(instance_id, status, "none")

except Exception as e:
    row = [str(datetime.utcnow()), os.environ['APP_NAME'], instance_id, "error"]
    write_csv(os.environ['LOG_FILE_NAME'], row)
    sp_process_logging(instance_id, status, "error")
