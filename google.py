import logging

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from time import sleep, gmtime
from bs4 import BeautifulSoup

logger = logging.getLogger("google.Business")

MAPS_SUMMARY = 1
MAPS_REVIEWS = 2


class Business:
    GOOGLE_SEARCH_URL = "https://www.google.com/search?q="
    GOOGLE_MAPS_URL = "https://www.google.com/maps?q="
    REVIEW_SCROLL_DIV = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]'
    REVIEW_ITEM_CLASS = 'jftiEf.fontBodyMedium'

    def __init__(self, address: str):
        self._set_maps_driver(address)
        self._set_search_driver(address)
        self._maps_focus = MAPS_SUMMARY

    def __del__(self):
        self._maps_driver.close()
        # self._maps_driver.quit()
        self._search_driver.close()
        # self._search_driver.quit()

    def _set_maps_driver(self, address: str):
        self._maps_driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                             options=self._get_options())
        self._maps_driver.get(self.GOOGLE_MAPS_URL + address.replace(" ", "+"))
        WebDriverWait(self._maps_driver, 10).until(EC.visibility_of_all_elements_located((By.ID, "searchboxinput")))
        self._consent_check(self._maps_driver)

    def _set_search_driver(self, address: str):
        self._search_driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                               options=self._get_options())
        self._search_driver.get(self.GOOGLE_SEARCH_URL + address.replace(" ", "+"))
        WebDriverWait(self._maps_driver, 10).until(EC.visibility_of_all_elements_located((By.NAME, "q")))
        self._consent_check(self._search_driver)

    def get_business_details(self) -> dict:
        self._switch_to_summary()

        business_details = {
            'store_name': self._get_business_name(),
            'address': self._get_address(),
            'avg_rating': self._get_rating(),
            'total_reviews': self._get_review_total(),
            'Service_options': self._get_service_options(),
            'avg_time_spent': self._get_av_time_spent()
        }
        return business_details

    def _get_business_name(self) -> str:
        parent_div = self._maps_driver.find_element(By.CLASS_NAME, "tAiQdd")
        return parent_div.find_element(By.XPATH, "//h1").text

    def _get_address(self) -> str:
        label = self._maps_driver.find_element(By.XPATH, "//button[contains(@aria-label, 'Address')]").get_attribute(
            "aria-label")
        return label.split(":")[1].strip()

    def _get_rating(self) -> str:
        return self._maps_driver.find_element(By.XPATH, "//span[contains(@aria-label, 'stars')]").get_attribute(
            "aria-label").strip()

    def _get_review_total(self) -> str:
        count_parent = self._maps_driver.find_element(By.CLASS_NAME, "jANrlb")
        review_count = count_parent.find_element(By.XPATH, "//button[contains(text(), 'reviews')]").text.split(" ")[0]
        return review_count.replace(",", "")

    def _get_service_options(self) -> str:
        return_str = []
        all_options = self._maps_driver.find_elements(By.CLASS_NAME, "LTs0Rc")
        for option in all_options:
            return_str.append(option.get_attribute("aria-label"))
        return ", ".join(return_str)

    def _get_av_time_spent(self):
        return self._search_driver.find_element(By.CLASS_NAME, "ffc9Ud").text

    def get_popular_times(self):
        self._switch_to_summary()

        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        popular_times = []

        # Scroll down to make the popular times graph visible
        ActionChains(self._maps_driver).move_to_element(
            self._maps_driver.find_element(By.CLASS_NAME, "C7xf8b")).perform()
        popular_times_heading = self._maps_driver.find_element(By.XPATH, "//h2[contains(text(), 'Popular times')]")
        parent = popular_times_heading.parent
        drop_down = parent.find_element(By.CLASS_NAME, "goog-menu-button-dropdown")

        for day in days:
            drop_down.click()
            WebDriverWait(self._maps_driver, 3).until(
                EC.visibility_of_all_elements_located((By.CLASS_NAME, "goog-menuitem")))
            option = self._maps_driver.find_element(By.ID, ':' + str(days.index(day)))
            option.click()
            graph_parent = self._maps_driver.find_element(By.CLASS_NAME, "C7xf8b")
            all_hours = graph_parent.find_elements(By.CLASS_NAME, "dpoVLd")
            for each_hour in all_hours:
                label_text = each_hour.get_attribute("aria-label").split()
                if label_text[0] != 'Currently':
                    popular_time_day = {
                        'perc_busy': label_text[0].replace("%", ''),
                        'hour_no': int(label_text[3]) if (
                                    label_text[4].upper() == "AM." or int(label_text[3]) == 12) else int(
                            label_text[3]) + 12,
                        'each_hour': each_hour.get_attribute('aria-label'),
                        'day_of_week': day
                    }
                    popular_times.append(popular_time_day)
        return popular_times

    def get_reviews(self):
        self._switch_to_review()

        count_parent = self._maps_driver.find_element(By.CLASS_NAME, "jANrlb")
        review_count = count_parent.find_element(By.XPATH, "//div[contains(text(), 'reviews')]").text.split(" ")[0]
        review_count = review_count.replace(",", "")

        # Adjust the sort order of the reviews to most recent
        self._maps_driver.find_element(By.XPATH, "//button[@aria-label='Sort reviews']").click()
        WebDriverWait(self._maps_driver, 10).until(
            EC.visibility_of_all_elements_located((By.XPATH, "//li[@role='menuitemradio']")))
        self._maps_driver.find_element(By.XPATH, "(//li[@role='menuitemradio'])[2]").click()
        WebDriverWait(self._maps_driver, 10).until(
            EC.visibility_of_all_elements_located((By.XPATH, self.REVIEW_SCROLL_DIV)))

        scrollable_div = self._maps_driver.find_element(By.XPATH, self.REVIEW_SCROLL_DIV)

        if int(review_count) >= 1000:
            scroll_end = 1000
            logger.debug(f"Total reviews exceeds 1000, script is limiting the scrape to 1000 reviews")
        else:
            scroll_end = int(review_count)

        all_items = self._maps_driver.find_elements(By.CLASS_NAME, self.REVIEW_ITEM_CLASS)

        while len(all_items) < scroll_end:
            self._maps_driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
            WebDriverWait(self._maps_driver, 10).until(
                EC.visibility_of_all_elements_located((By.XPATH, self.REVIEW_SCROLL_DIV)))
            all_items = self._maps_driver.find_elements(By.CLASS_NAME, self.REVIEW_ITEM_CLASS)

        process_count = 1
        rev_dict = {
            'reviewer_name': [],
            'rating': [],
            'reviewed_dt': [],
            'review': []}

        sleep(2)
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
            reviewer_name = bs_item.find('div', class_='d4r55').text.strip()
            review_rate = bs_item.find('span', class_='kvMYJc')["aria-label"]
            review_time = bs_item.find('span', class_='rsqaWe').text
            review_text = bs_item.find('span', class_='wiI7pd').text
            rev_dict['reviewer_name'].append(reviewer_name)
            rev_dict['rating'].append(review_rate)
            rev_dict['reviewed_dt'].append(review_time)
            rev_dict['review'].append(review_text)
            process_count += 1
        logger.debug(f"{process_count} reviews processed")
        return rev_dict

    def _switch_to_review(self):
        if self._maps_focus != MAPS_REVIEWS:
            ActionChains(self._maps_driver).move_to_element(
                self._maps_driver.find_element(By.CLASS_NAME, "DkEaL")).perform()
            google_reviews_link = self._maps_driver.find_element(By.CLASS_NAME, "DkEaL")
            google_reviews_link.click()
            WebDriverWait(self._maps_driver, 100).until(
                EC.presence_of_element_located((By.XPATH, "//span[contains(text(), 'All reviews')]")))
            WebDriverWait(self._maps_driver, 100).until(
                EC.presence_of_element_located((By.CLASS_NAME, self.REVIEW_ITEM_CLASS)))
            self._maps_focus = MAPS_REVIEWS

    def _switch_to_summary(self):
        if self._maps_focus != MAPS_SUMMARY:
            self._maps_driver.back()
            WebDriverWait(self._maps_driver, 100).until(
                EC.presence_of_element_located((By.XPATH, "//h2[contains(text(), 'Photos')]")))

            # Needed as menu item number change to letters moving from reviews back to summary
            self._maps_driver.refresh()
            self._maps_focus = MAPS_SUMMARY

    @staticmethod
    def _get_options() -> Options:
        chrome_options = Options()
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-extensions")
        # chrome_options.add_argument("--start-maximized")
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
