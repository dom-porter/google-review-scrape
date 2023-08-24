import logging
import os
import re
from time import sleep

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv()  # take environment variables from .env.

if str(os.environ['G_MAPS_LOG_DEBUG']).upper() == "TRUE":
    __DEBUG__ = True
    log_level = logging.DEBUG
else:
    __DEBUG__ = False
    log_level = logging.INFO

logger = logging.getLogger("google.Business")
logger.setLevel(log_level)

MAPS_SUMMARY = 1
MAPS_REVIEWS = 2


class Business:
    # GOOGLE_SEARCH_URL = "https://www.google.com/search?q="
    GOOGLE_MAPS_URL = "https://www.google.com/maps?q="
    REVIEW_SCROLL_DIV = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]'
    REVIEW_ITEM_CLASS = 'jftiEf.fontBodyMedium'

    def __init__(self, business_ref, address: str):
        if business_ref and address:
            # self._chrome_service = ChromeService(ChromeDriverManager().install())
            self._webdriver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=self._get_options())
            self._business_ref = business_ref
            self._address = address
            # self._set_maps_driver(address)
            self._maps_focus = MAPS_SUMMARY
            self._partial = self._check_partial_match()
            self._webdriver.get(self.GOOGLE_MAPS_URL + address.replace(" ", "+"))
            WebDriverWait(self._webdriver, 10).until(EC.visibility_of_all_elements_located((By.ID, "searchboxinput")))
            self._consent_check(self._webdriver)

    def __del__(self):
        if self._webdriver is not None:
            self._webdriver.close()


    # def _set_maps_driver(self, address: str):
        # logger.debug(f"Installing and initializing maps browser for {self._business_ref}..")
        # self._maps_driver = webdriver.Chrome(service=self._chrome_service) # USED FOR TESTING. NOT HEADLESS
        # self._maps_driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=self._get_options())

        # self._maps_driver = webdriver.Chrome(service=self._chrome_service, options=self._get_options())
        # self._maps_driver.get(self.GOOGLE_MAPS_URL + address.replace(" ", "+"))
        # WebDriverWait(self._maps_driver, 10).until(EC.visibility_of_all_elements_located((By.ID, "searchboxinput")))
        # self._consent_check(self._maps_driver)
        # logger.debug(f"Initialization of browser for {self._business_ref} complete")

    # def _set_search_driver(self, address: str):
        # self._search_driver = webdriver.Chrome(service=self._chrome_service) # USED FOR TESTING. NOT HEADLESS
        # self._search_driver = webdriver.Chrome(service=self._chrome_service, options=self._get_options())
        # self._search_driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=self._get_options())

        # self._search_driver.get(self.GOOGLE_SEARCH_URL + address.replace(" ", "+"))
        # WebDriverWait(self._maps_driver, 10).until(EC.visibility_of_all_elements_located((By.NAME, "q")))
        # self._consent_check(self._search_driver)

    def _check_partial_match(self):
        parent_div = self._webdriver.find_elements(By.XPATH, "//*[text()='Partial match']")
        if len(parent_div) > 0:
            logger.error(f"Partial match for {self._business_ref} - {self._address}")
            return True
        return False

    def get_business_details(self) -> dict:
        self._switch_to_summary()
        if self._partial:
            return None

        logger.info(f"[{self._business_ref}] Getting business information...")

        business_details = {
            'business_ref': self._business_ref,
            'business_name': self._get_business_name(),
            'address': self._get_address(),
            'avg_rating': self._get_rating(),
            'total_reviews': self._get_review_total(),
            'service_options': self._get_service_options(),
        }
        return business_details

    def _get_business_name(self) -> str:
        parent_div = self._webdriver.find_element(By.CLASS_NAME, "tAiQdd")
        return parent_div.find_element(By.XPATH, "//h1").text

    def _get_address(self) -> str:
        try:
            label = self._webdriver.find_element(By.XPATH,
                                                   "//button[contains(@aria-label, 'Address')]").get_attribute(
                "aria-label")
            return label.split(":")[1].strip()
        except Exception as e:
            logger.error(f"[{self._business_ref}] Unable to get address")
            logger.error(e)
            return "No Address"

    def _get_rating(self) -> str:
        try:
            parent_div = self._webdriver.find_element(By.CLASS_NAME, "F7nice")
            rating = parent_div.find_element(By.XPATH, "//span[contains(@role, 'img')]").get_attribute(
                "aria-label").strip()
            return rating
        except Exception as e:
            logger.error(f"[{self._business_ref}] Unable to get rating")
            logger.error(e)
            return "No rating"

    def _get_review_total(self) -> str:
        # count_parent = self._maps_driver.find_element(By.CLASS_NAME, "jANrlb")
        # review_count = count_parent.find_element(By.XPATH, "//button[contains(text(), 'Reviews')]").text.split(" ")[0]
        try:
            review_count = self._webdriver.find_element(By.XPATH, "//span[contains(@aria-label, 'reviews')]").text
            return re.sub('[()]', '', review_count)
        except Exception as e:
            logger.error(f"[{self._business_ref}] Unable to get review count")
            logger.error(e)
            return "0"

    def _get_service_options(self) -> str:
        return_str = []
        all_options = self._webdriver.find_elements(By.CLASS_NAME, "LTs0Rc")
        for option in all_options:
            return_str.append(option.get_attribute("aria-label"))
        return ", ".join(return_str)


    def get_popular_times(self):
        logger.info(f"[{self._business_ref}] Getting popular times...")

        self._switch_to_summary()
        if self._partial:
            return None

        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        popular_times = []

        try:
            # Scroll down to make the popular times graph visible
            ActionChains(self._webdriver).move_to_element(
                self._webdriver.find_element(By.CLASS_NAME, "C7xf8b")).perform()
            popular_times_heading = self._webdriver.find_element(By.XPATH, "//h2[contains(text(), 'Popular times')]")
            parent = popular_times_heading.parent
            drop_down = parent.find_element(By.CLASS_NAME, "goog-menu-button-dropdown")

            for day in days:
                drop_down.click()
                WebDriverWait(self._webdriver, 3).until(
                    EC.visibility_of_all_elements_located((By.CLASS_NAME, "goog-menuitem")))
                option = self._webdriver.find_element(By.ID, ':' + str(days.index(day)))
                option.click()
                graph_parent = self._webdriver.find_element(By.CLASS_NAME, "C7xf8b")
                all_hours = graph_parent.find_elements(By.CLASS_NAME, "dpoVLd")
                for each_hour in all_hours:
                    label_text = each_hour.get_attribute("aria-label").split()
                    if label_text[0] != 'Currently':
                        popular_time_day = {
                            'business_ref': self._business_ref,
                            'percent_busy': label_text[0].replace("%", ''),
                            'hour_no': int(label_text[3]) if (
                                    label_text[4].upper() == "AM." or int(label_text[3]) == 12) else int(
                                label_text[3]) + 12,
                            'each_hour': each_hour.get_attribute('aria-label'),
                            'day_of_week': day
                        }
                        popular_times.append(popular_time_day)
            return popular_times
        except Exception as exp:
            logger.info(f"[{self._business_ref}] Unable to get popular times")
            return ["No data available"]

    def get_reviews(self):
        logger.info(f"[{self._business_ref}] Getting reviews...")

        self._switch_to_review()
        if self._partial:
            return None

        review_count = 0
        try:
            count_parent = self._webdriver.find_element(By.CLASS_NAME, "jANrlb")
            parent_text = count_parent.text
            parent_text = parent_text.split("\n")[1].split(" ")[0]
            review_count = parent_text.replace(",", "")
            # review_count = count_parent.find_element(By.XPATH, "//div[contains(text(), 'reviews')]").text.split(" ")[0]
            # review_count = count_parent.find_element(By.XPATH, "//div[contains(text(), ' reviews')]")
            # review_count = count_parent.find_element(By.CLASS_NAME, "fontBodySmall")
            # review_count = review_count.replace(",", "")
            logger.debug(f"[{self._business_ref}] Review count: {review_count}")
        except Exception as exp:
            logger.exception("Error whilst reading review count")

        try:
            # Adjust the sort order of the reviews to most recent
            self._webdriver.find_element(By.XPATH, "//button[@aria-label='Sort reviews']").click()
            WebDriverWait(self._webdriver, 10).until(
                EC.visibility_of_all_elements_located((By.XPATH, "//div[@role='menuitemradio']")))
            self._webdriver.find_element(By.XPATH, "(//div[@role='menuitemradio' and @data-index='1'])").click()
            WebDriverWait(self._webdriver, 10).until(
                EC.visibility_of_all_elements_located((By.XPATH, self.REVIEW_SCROLL_DIV)))
            logger.debug(f"[{self._business_ref}] Adjusted sort order")
        except Exception as exp:
            logger.exception("Error whilst changing sort oder of reviews")

        all_items = None
        try:
            logger.debug(f"[{self._business_ref}] Scrolling reviews div")
            scrollable_div = self._webdriver.find_element(By.XPATH, self.REVIEW_SCROLL_DIV)

            if int(review_count) >= 1000:
                scroll_end = 1000
                logger.info(
                    f"[{self._business_ref}] Total reviews exceeds 1000, script is limiting the scrape to 1000 reviews")
            else:
                scroll_end = int(review_count)

            scroll_end = scroll_end
            all_items = self._webdriver.find_elements(By.CLASS_NAME, self.REVIEW_ITEM_CLASS)
            loop_count = 0
            current_count = 0
            while len(all_items) < scroll_end:
                self._webdriver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
                try:
                    WebDriverWait(self._webdriver, 10).until(
                        EC.visibility_of_all_elements_located((By.XPATH, self.REVIEW_SCROLL_DIV)))
                except TimeoutException as exp:
                    logger.exception("Timeout while scrolling review div")

                all_items = self._webdriver.find_elements(By.CLASS_NAME, self.REVIEW_ITEM_CLASS)

                # there are instances of review total on the page being more than the
                # returned reviews which causes this scroll to be infinite. This should stop it.
                if len(all_items) == current_count:
                    loop_count += 1
                    if loop_count == 100:
                        logger.error(
                            f"[{self._business_ref}] Error, unable to load additional reviews. Expected {scroll_end} but returned {len(all_items)}")
                        break
                else:
                    loop_count = 0
                current_count = len(all_items)
            logger.debug(f"[{self._business_ref}] Finished scrolling")
        except Exception as exp:
            logger.exception("Error whilst fetching reviews")

        process_count = 0
        rev_dict = {
            'business_ref': [],
            'reviewer_name': [],
            'rating': [],
            'reviewed_dt': [],
            'review': []}

        sleep(2)
        logger.info(f"[{self._business_ref}] Processing reviews")
        for item in all_items:
            # Check if review has been shortened then click the More button
            more_buttons = item.find_elements(By.CLASS_NAME, 'w8nwRe.kyuRq')
            for button in more_buttons:
                try:
                    button.click()
                    sleep(1)
                except BaseException as e:
                    logger.error(f"Unable to expand shortened review for {item.accessible_name}")
                    raise e

            html_item = item.get_attribute("outerHTML")
            bs_item = BeautifulSoup(html_item, 'html.parser')
            rev_dict['business_ref'].append(self._business_ref)

            try:
                name_test = bs_item.find('div', class_='d4r55').text.strip()
                logger.debug(f"Reviewer {name_test}")
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

            except BaseException as e:
                logger.error("Error getting review data")
                logger.error(e)

        logger.debug(f"{process_count} reviews processed")
        return rev_dict

    def _switch_to_review(self):
        if self._maps_focus != MAPS_REVIEWS:
            self._webdriver.refresh()
            ActionChains(self._webdriver).move_to_element(
                self._webdriver.find_element(By.CLASS_NAME, "RWPxGd")).perform()
            # google_reviews_link = self._maps_driver.find_element(By.CLASS_NAME, "F7nice.mmu3tf")
            # google_reviews_link.click()
            try:
                reviews_button = self._webdriver.find_element(By.XPATH, "//button[contains(@aria-label, 'Reviews')]")
                if reviews_button is not None:
                    reviews_button.click()
            except Exception as e:
                logger.error("Unable to click reviews button")
                logger.error(e)

            try:
                WebDriverWait(self._webdriver, 10).until(
                    EC.visibility_of_all_elements_located((By.XPATH, "//div[@role='radiogroup']")))
                self._maps_focus = MAPS_REVIEWS

            except TimeoutException as exp:
                logger.exception("Timeout while loading review page")
                self._webdriver.save_screenshot(f"{self._business_ref}_review_screenshot.png")

    def _switch_to_summary(self):
        if self._maps_focus != MAPS_SUMMARY:
            self._webdriver.back()
            try:
                WebDriverWait(self._webdriver, 100).until(
                    EC.presence_of_element_located((By.XPATH, "//h2[contains(text(), 'Photos')]")))
            except TimeoutException as exp:
                logger.exception("Timeout while loading summary page")
                self._webdriver.save_screenshot(f"{self._business_ref}_summary_screenshot.png")

            # Needed as menu item number changes to letters moving from reviews back to summary
            self._webdriver.refresh()
            self._maps_focus = MAPS_SUMMARY

    @staticmethod
    def _get_options() -> Options:
        prefs = {}
        chrome_options = Options()

        prefs["intl.accept_languages"] = "en_us"
        # chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--headless=new")
        # chrome_options.add_argument("window-size=1920x1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--incognito")
        # chrome_options.add_argument("--lang=en")
        chrome_options.add_argument("--locale=en")
        # chrome_options.add_argument("--accept-lang=en")
        chrome_options.add_argument("force-device-scale-factor=0.5")
        return chrome_options

    @staticmethod
    def _consent_check(_driver: webdriver.Chrome):
        if len(_driver.find_elements(By.ID, "L2AGLb")) > 0:
            try:
                consent = _driver.find_element(By.ID, "L2AGLb")
                consent.click()
            except BaseException as e:
                logger.debug("Unable to click consent accept all button")
                raise e
