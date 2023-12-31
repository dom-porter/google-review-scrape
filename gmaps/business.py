import logging
import os
import re
from time import sleep

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from gmaps.exceptions import EmptyBusinessError, FactoryError, BrowserError

GOOGLE_MAPS_URL = "https://www.google.com/maps?q="
MAPS_SUMMARY = 1
MAPS_REVIEWS = 2
REVIEW_SCROLL_DIV = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]'
REVIEW_ITEM_CLASS = 'jftiEf.fontBodyMedium'

load_dotenv()  # take environment variables from .env.

if str(os.environ['G_MAPS_LOG_DEBUG']).upper() == "TRUE":
    DEBUG = True
    log_level = logging.DEBUG
else:
    DEBUG = False
    log_level = logging.INFO

logger = logging.getLogger("gmaps.business")
logger.setLevel(log_level)


class GoogleBusiness:

    def __init__(self):
        self.no_match = True
        self.ref = None
        self.address = None
        self._chrome_driver = None
        self._focus = MAPS_SUMMARY

    def __del__(self):
        if self._chrome_driver:
            self._chrome_driver.close()

    def get_details(self) -> dict:
        if self.no_match:
            raise EmptyBusinessError("Unable to return details when no match returned from Google maps.")

        self._switch_to_summary()
        log_info(f"[{self.ref}] Getting business information")

        business_details = {
            'business_ref': self.ref,
            'business_name': self._get_business_name(),
            'address': self._get_address(),
            'avg_rating': self._get_rating(),
            'total_reviews': self._get_review_total(),
            'service_options': self._get_service_options(),
        }
        return business_details

    def get_popular_times(self):
        if self.no_match:
            raise EmptyBusinessError("Unable to return popular times when no match returned from Google maps.")

        log_info(f"[{self.ref}] Getting popular times")
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        popular_times = []

        try:
            # Scroll down to make the popular times graph visible
            ActionChains(self._chrome_driver).move_to_element(
                self._chrome_driver.find_element(By.CLASS_NAME, "C7xf8b")).perform()
            popular_times_heading = self._chrome_driver.find_element(By.XPATH,
                                                                     "//h2[contains(text(), 'Popular times')]")
            parent = popular_times_heading.parent
            drop_down = parent.find_element(By.CLASS_NAME, "goog-menu-button-dropdown")

            for day in days:
                popular_hours = self._get_day_times(day, str(days.index(day)), drop_down)
                for hour in popular_hours:
                    popular_times.append(hour)
            return popular_times
        except Exception as e:
            log_exception(f"[{self.ref}] Unable to get popular times")
            return {'business_ref': self.ref, 'percent_busy': "none", 'hour_no': "none", 'each_hour': "none",
                    'day_of_week': "none"}

    def get_reviews(self):
        if self.no_match:
            raise EmptyBusinessError("Unable to return popular times when no match returned from Google maps.")

        log_info(f"[{self.ref}] Getting reviews")
        process_count = 0

        self._switch_to_review()
        review_count = self._get_review_count()
        self._adjust_sort_order()
        all_items = self._scroll_div_bottom(review_count)

        rev_dict = {
            'business_ref': [],
            'reviewer_name': [],
            'rating': [],
            'reviewed_dt': [],
            'review': []}

        sleep(1)
        log_info(f"[{self.ref}] Processing reviews")
        for item in all_items:
            # Check if review has been shortened then click the More button
            more_buttons = item.find_elements(By.CLASS_NAME, 'w8nwRe.kyuRq')
            for button in more_buttons:
                try:
                    button.click()
                    sleep(1)
                except Exception as e:
                    log_exception(f"Unable to expand shortened review for {item.accessible_name}")

            html_item = item.get_attribute("outerHTML")
            bs_item = BeautifulSoup(html_item, 'html.parser')
            rev_dict['business_ref'].append(self.ref)

            try:
                name_test = bs_item.find('div', class_='d4r55').text.strip()
                log_debug(f"Reviewer {name_test}")
                rev_dict['reviewer_name'].append(bs_item.find('div', class_='d4r55').text.strip())
                rev_dict['rating'].append(bs_item.find('span', class_='kvMYJc')["aria-label"])
                rev_dict['reviewed_dt'].append(bs_item.find('span', class_='rsqaWe').text)
                review = bs_item.find('span', class_='wiI7pd')
                if review is None:
                    review = ""
                else:
                    review = review.text
                rev_dict['review'].append(review)
                process_count += 1

            except Exception as e:
                log_exception("Error getting review data")

        log_debug(f"{process_count} reviews processed")
        return rev_dict

    def _scroll_div_bottom(self, review_count: int):
        try:
            log_debug(f"[{self.ref}] Scrolling reviews div")
            scrollable_div = self._chrome_driver.find_element(By.XPATH, REVIEW_SCROLL_DIV)

            if review_count >= 1000:
                scroll_end = 1000
                log_info(
                    f"[{self.ref}] Total reviews exceeds 1000, script is limiting the scrape to 1000 reviews")
            else:
                scroll_end = review_count

            scroll_end = scroll_end
            all_items = self._chrome_driver.find_elements(By.CLASS_NAME, REVIEW_ITEM_CLASS)
            loop_count = 0
            current_count = 0
            while len(all_items) < scroll_end:
                self._chrome_driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
                try:
                    WebDriverWait(self._chrome_driver, 10).until(
                        EC.visibility_of_all_elements_located((By.XPATH, REVIEW_SCROLL_DIV)))
                except TimeoutException:
                    log_exception("Timeout while scrolling review div")

                all_items = self._chrome_driver.find_elements(By.CLASS_NAME, REVIEW_ITEM_CLASS)

                # there are instances of review total on the page being more than the
                # returned reviews (due to browser limitations) which causes this scroll to be infinite. This should stop it.
                if len(all_items) == current_count:
                    loop_count += 1
                    if loop_count == 100:
                        log_error(
                            f"[{self.ref}] Error, unable to load additional reviews. Expected {scroll_end} but returned {len(all_items)}")
                        break
                else:
                    loop_count = 0
                current_count = len(all_items)
            log_debug(f"[{self.ref}] Finished scrolling")
            return all_items
        except Exception as e:
            log_exception("Error whilst fetching reviews")
            return None

    def _adjust_sort_order(self):
        try:
            # Adjust the sort order of the reviews to most recent
            self._chrome_driver.find_element(By.XPATH, "//button[@aria-label='Sort reviews']").click()
            WebDriverWait(self._chrome_driver, 10).until(
                EC.visibility_of_all_elements_located((By.XPATH, "//div[@role='menuitemradio']")))
            self._chrome_driver.find_element(By.XPATH, "(//div[@role='menuitemradio' and @data-index='1'])").click()
            WebDriverWait(self._chrome_driver, 10).until(
                EC.visibility_of_all_elements_located((By.XPATH, REVIEW_SCROLL_DIV)))
            log_debug(f"[{self.ref}] Adjusted sort order")
        except Exception as e:
            log_exception("Error whilst changing sort oder of reviews")

    def _switch_to_review(self):
        if self._focus != MAPS_REVIEWS:
            self._chrome_driver.refresh()
            ActionChains(self._chrome_driver).move_to_element(
                self._chrome_driver.find_element(By.CLASS_NAME, "RWPxGd")).perform()
            try:
                reviews_button = self._chrome_driver.find_element(By.XPATH, "//button[contains(@aria-label, 'Reviews')]")
                if reviews_button is not None:
                    reviews_button.click()
            except Exception as e:
                log_exception("Unable to click reviews button")

            try:
                WebDriverWait(self._chrome_driver, 10).until(
                    EC.visibility_of_all_elements_located((By.XPATH, "//div[@role='radiogroup']")))
                self._focus = MAPS_REVIEWS

            except TimeoutException:
                log_exception("Timeout while loading review page")
                # self._webdriver.save_screenshot(f"{self._business_ref}_review_screenshot.png")

    def _get_review_count(self):
        try:
            count_parent = self._chrome_driver.find_element(By.CLASS_NAME, "jANrlb")
            parent_text = count_parent.text
            parent_text = parent_text.split("\n")[1].split(" ")[0]
            review_count = parent_text.replace(",", "")
            log_debug(f"[{self.ref}] Review count: {review_count}")
            return int(review_count)
        except Exception as e:
            log_exception("Error whilst reading review count")
            return 0

    def _get_day_times(self, day: str, day_index: str, drop_down: WebElement):
        popular_times = []
        log_debug(f"[{self.ref}] Getting {day} hours.")
        drop_down.click()
        WebDriverWait(self._chrome_driver, timeout=3).until(
            EC.visibility_of_all_elements_located((By.CLASS_NAME, "goog-menuitem")))
        option = self._chrome_driver.find_element(By.ID, ':' + day_index)
        option.click()
        graph_parent = self._chrome_driver.find_element(By.CLASS_NAME, "C7xf8b")
        all_hours = graph_parent.find_elements(By.CLASS_NAME, "dpoVLd")
        for each_hour in all_hours:
            label_text = each_hour.get_attribute("aria-label").split()
            if label_text[0] != 'Currently':
                popular_time_day = {
                    'business_ref': self.ref,
                    'percent_busy': label_text[0].replace("%", ''),
                    'hour_no': int(label_text[3]) if (
                            label_text[4].upper() == "AM." or int(label_text[3]) == 12) else int(
                        label_text[3]) + 12,
                    'each_hour': each_hour.get_attribute('aria-label'),
                    'day_of_week': day
                }
                popular_times.append(popular_time_day)
        return popular_times

    def _get_business_name(self) -> str:
        parent_div = self._chrome_driver.find_element(By.CLASS_NAME, "tAiQdd")
        return parent_div.find_element(By.XPATH, "//h1").text

    def _get_address(self) -> str:
        try:
            label = self._chrome_driver.find_element(By.XPATH,
                                                     "//button[contains(@aria-label, 'Address')]").get_attribute(
                "aria-label")
            return label.split(":")[1].strip()
        except NoSuchElementException as e:
            log_exception(f"[{self._chrome_driver}] - Unable to get address for {self.ref}")
            return "No Address"

    def _get_rating(self) -> str:
        try:
            parent_div = self._chrome_driver.find_element(By.CLASS_NAME, "F7nice")
            rating = parent_div.find_element(By.XPATH, "//span[contains(@role, 'img')]").get_attribute(
                "aria-label").strip()
            return rating
        except NoSuchElementException as e:
            log_exception(f"[{self.ref}] Unable to get rating for {self.ref}")
            return "No rating"

    def _get_review_total(self) -> str:
        try:
            review_count = self._chrome_driver.find_element(By.XPATH, "//span[contains(@aria-label, 'reviews')]").text
            return re.sub('[()]', '', review_count)
        except NoSuchElementException as e:
            log_exception(f"[{self._chrome_driver}] Unable to get review count")
            return "0"

    def _get_service_options(self) -> str:
        return_str = []
        all_options = self._chrome_driver.find_elements(By.CLASS_NAME, "LTs0Rc")
        for option in all_options:
            return_str.append(option.get_attribute("aria-label"))
        return ", ".join(return_str)

    def _switch_to_summary(self):
        if self._focus != MAPS_SUMMARY:
            self._chrome_driver.back()
            try:
                WebDriverWait(self._chrome_driver, timeout=10).until(
                    EC.presence_of_element_located((By.XPATH, "//h2[contains(text(), 'Photos')]")))
            except TimeoutException:
                log_exception("Timeout while loading summary page")
                # self._webdriver.save_screenshot(f"{self._business_ref}_summary_screenshot.png")

            # Needed as menu item number changes to letters moving from reviews back to summary
            self._chrome_driver.refresh()
            self._focus = MAPS_SUMMARY


def business_factory(ref: str, address: str, driver_path: str) -> GoogleBusiness:
    """
    Creates an instance of GoogleBusiness.
    Factory does not manage instances after creation.
    """

    chrome_driver = webdriver.Chrome(service=ChromeService(driver_path), options=get_options())
    chrome_driver.get(GOOGLE_MAPS_URL + address.replace(" ", "+"))
    WebDriverWait(chrome_driver, timeout=10).until(EC.visibility_of_all_elements_located((By.ID, "searchboxinput")))
    consent_check(chrome_driver)

    new_business = GoogleBusiness()

    try:
        WebDriverWait(chrome_driver, timeout=5).until(
            EC.presence_of_element_located((By.XPATH, "//h2[contains(text(), 'Photos')]")))

        new_business.no_match = False
        new_business.ref = ref
        new_business.address = address
        new_business._chrome_driver = chrome_driver
        return new_business

    except TimeoutException:
        if chrome_driver is not None:
            chrome_driver.close()
        raise FactoryError(
            f"Timeout waiting for browser returning info for {ref}, possible no match found in Google maps.") from None

    except NoSuchElementException:
        if chrome_driver is not None:
            chrome_driver.close()
        raise FactoryError(f"Google maps returned no match for {ref}") from None


def get_options() -> Options:
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--locale=en")
    chrome_options.add_argument("force-device-scale-factor=0.5")
    return chrome_options


def consent_check(chrome_drive: webdriver.Chrome):
    """
    Checks for the chrome consent dialog for new browser instances and
    clicks the accept all button.
    """

    if len(chrome_drive.find_elements(By.ID, "L2AGLb")) > 0:
        try:
            consent = chrome_drive.find_element(By.ID, "L2AGLb")
            consent.click()
        except Exception as e:
            raise BrowserError("Unable to click consent accept all button")


def log_exception(message: str):
    """ Logs an exception message to the log file but also prints it to the terminal """
    print(message)
    logger.exception(message)


def log_info(message: str):
    """ Logs an information message to the log file but also prints it to the terminal """
    print(message)
    logger.info(message)


def log_debug(message: str):
    """ Logs a debug message to the log file but also prints it to the terminal """
    if DEBUG:
        print(message)
        logger.debug(message)


def log_error(message: str):
    """ Logs a debug message to the log file but also prints it to the terminal """
    print(message)
    logger.error(message)
