import os
import sys
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

test = Business("Zara store UK BS1 3BX")
# print(test.getBusinessDetails())

# print("Get reviews:")
# print(test.getReviews())
# print("Get business info:")
# print(test.getBusinessDetails())
print("Get reviews:")
print(test.get_reviews())
print("Get popular times:")
print(test.get_popular_times())

# test = Business("Strumpet store UK HP34 789P")


