import copy
import csv
import logging
import math
import os
import re
import smtplib
import sys
import time
import traceback
from datetime import date
from pprint import pformat
from random import randint
from types import SimpleNamespace

import boto3
import numpy as np
import pandas as pd
import requests
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from yaml import Loader, load

CONFIG = 'config/config.yaml'

# Logger -----------------------------------------------------------------------------------------------
try:
    import colorlog
    HAVE_COLORLOG = True
except ImportError:
    HAVE_COLORLOG = False

def create_logger():
    """
        Setup the logging environment
    """
    log = logging.getLogger()  # root logger
    log.setLevel(logging.INFO)
    format_str = '%(asctime)s - %(levelname)-8s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    if HAVE_COLORLOG and os.isatty(2):
        cformat = '%(log_color)s' + format_str
        colors = {'DEBUG': 'reset',
                  'INFO': 'reset',
                  'WARNING': 'bold_yellow',
                  'ERROR': 'bold_red',
                  'CRITICAL': 'bold_red'}
        formatter = colorlog.ColoredFormatter(cformat, date_format,
                                              log_colors=colors)
    else:
        formatter = logging.Formatter(format_str, date_format)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    log.addHandler(stream_handler)
    return logging.getLogger(__name__)

logger = create_logger()

# Main Method --------------------------------------------------------------------------------------------
def main():
    starttime = time.time()

    config = load(open(CONFIG), Loader=Loader)
    full_scraping = gebiz_scraping(**config)
    if full_scraping:
        logger.info("Process is fully completed.")

    endtime = time.time()
    duration = endtime - starttime

    df = pd.read_csv(config['csvname'], header=0).set_index(config['csvheaders']['refno'])
    df_dict = df.T.to_dict('dict')

    # Upload the CSV File - If you are not planning to upload to S3, simply comment this line.
    upload_csv(config['s3bucket'], config['s3path'], config['csvname'])

    # Remove CSV file from Local Directory
    deleted = delete_csv(config['csvname'])
    if deleted:
        logger.info("Local CSV Deleted.")

    
    logger.info("Final Length of Projects Scraped: %s" % len(df_dict))
    logger.info("Task Took %s min" % (duration/60))

    # # Print the dataset for visuals
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #     logger.info(df)

# Gebiz Scraping Method ----------------------------------------------------------------------------------
def gebiz_scraping(url, csvname, csvheaders, s3bucket, s3path):
    """
    Scraping Workflow will be placed here with necessary headers and url link.
    Exporting to CSV will be done regularly in this function.
    Return True or False instead, to signify the complete scrape of the websites or not.
    """
    # Create NameSpace to call Headers
    headertuple = SimpleNamespace(**csvheaders)

    # Start Browser
    options = Options()
    # options.headless = True
    options.add_argument("--window-size=1920,1920")
    options.add_argument("--incognito")
    options.add_argument('--headless')
    # The following arguments are for usage in docker containers, if not it is not necessary.
    # options.add_argument('--no-sandbox')
    # options.add_argument("--disable-setuid-sandbox")
    driver = webdriver.Chrome(
        options=options, executable_path=r'/usr/local/bin/chromedriver')
    driver.get(url)

    # Make script wait for page to load first
    wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located(
        (By.XPATH, "//input[@class='selectManyMenu_BUTTON' and @value='All' and @type='button']")))

    # Click to the menu page
    menu_button = driver.find_elements_by_xpath(
        "//input[@class='selectManyMenu_BUTTON' and @value='All' and @type='button']")[2]
    menu_button.click()

    # Make script wait for page to load for the filter options to appear
    wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located(
        (By.XPATH, "//input[contains(@name, 'GroupOption_Construction') and @value='Construction']")))

    # Activate the Construction Filter
    filter_button = driver.find_element_by_xpath(
        "//input[contains(@name, 'GroupOption_Construction') and @value='Construction']")
    filter_button.click()

    # Activate the Facilities Management Filters - 1.Building, M&E Maintenance 2.Cleaning Services 3.Pest Management Services 4.Security Services
    filter_button = driver.find_element_by_xpath(
        "//input[contains(@name, 'Facilities Management_Option_100049') and @value='100049']")
    filter_button.click()
    filter_button = driver.find_element_by_xpath(
        "//input[contains(@name, 'Facilities Management_Option_100046') and @value='100046']")
    filter_button.click()
    filter_button = driver.find_element_by_xpath(
        "//input[contains(@name, 'Facilities Management_Option_100047') and @value='100047']")
    filter_button.click()
    filter_button = driver.find_element_by_xpath(
        "//input[contains(@name, 'Facilities Management_Option_100044') and @value='100044']")
    filter_button.click()

    # Go to the Closed Tab
    closed_button = driver.find_element_by_xpath(
        "//input[@class='formTabBar_TAB-BUTTON' and contains(@value, 'Closed')]")
    closed_button.click()

    # Make script wait for page to load
    wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and contains(@value, 'Closed') and \
        @disabled='disabled']")))

    # Make script wait for page to load for the filter to work
    wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located(
        (By.XPATH, "//div[@class='selectManyMenu_SELECTED-ITEM-TR' and @title='Construction &#8658; Others']")))

    # This statement here downloads the csv file from S3. If you rather not do that, simply comment this line out.
    download = download_csv(s3bucket, s3path, csvname)
    # Full Dataset
    # If there is an existing csv, simply open the whole thing in pandas
    # It is actually better to save the results as a dictionary of dictionaries and then convert to df back later for faster processing
    if download and os.path.exists(csvname):
        df = pd.read_csv(csvname, header=0).set_index(headertuple.refno)
        df_dict = df.T.to_dict('dict')
    else:
        df_dict = {}

    logger.info('Existing Rows in CSV: %s' % len(df_dict))

    # Do a loop here to click Next after all links are clicked
    try:
        while True:
            # First I have to collect the links for each button to be clicked.
            # Click to link -> Grab information --> Click to Back Search Results --> Go to next button to click.
            # The link can become stale. So, lets maintain a counter instead.
            lenlinks = len(driver.find_elements_by_xpath(
                "//a[contains(@class, 'commandLink_TITLE-') and @href='#' and @type='link']"))

            # Grab all Reference Numbers from the navigation page
            navigation_soup = BeautifulSoup(driver.page_source, 'html.parser')
            referencenumbers = find_values_in_navigation(navigation_soup, 'div', 'formSectionHeader6_TEXT')
            # Grab all Statuses from the navigation page
            current_statuses = find_values_in_navigation(navigation_soup, 'div', 'label_MAIN label_WHITE-ON-.*')

            for index in range(lenlinks):
                # Project Type and Reference Number
                referencenumber = referencenumbers[index]
                projecttype, referencenumber = referencenumber.split('-', 1)
                projecttype = projecttype.strip()
                referencenumber = referencenumber.strip()
                logger.info("Reference Number %s" % referencenumber)

                # Current Status
                current_status = current_statuses[index]

                # Skip those who are already scraped and there is no change
                if duplicate_entry(referencenumber, projecttype, current_status, df_dict, headertuple):
                    logger.info("Project has been scraped. Moving onto next project... \n")
                    continue

                # Append into a row dictionary
                # New row to be built
                row = {key: None for key in csvheaders.values()}
                row.pop(headertuple.refno, None)

                # Append Status and Project Type
                row[headertuple.curstatus] = current_status
                row[headertuple.projtype] = projecttype

                # Rest of the attributes
                result = individual_page_scraping(index, row, headertuple, \
                    referencenumber, projecttype, current_status, driver)

                # If result is False, end the whole program
                if not result:
                    driver.quit()
                    return False

                # If result is 'skip', simply go the next link
                if result != 'skip':
                    # Append Today's Date
                    today = date.today()
                    row[headertuple.lastupdated] = str(today)

                    # Update the dictionary and export as well.
                    # Append it back to Dataset and export it continuously - This slows down the process a lot but it is necessary to maintain all the rows so we don't do extra work.
                    df_dict[referencenumber] = row
                    export_csv(df_dict, csvname, headertuple)

                    # Print the rows being appended
                    logger.info('')
                    logger.info('Page Link Number: %s' % index)
                    for line in pformat(row).split('\n'):
                        logger.info(line)
                    logger.info('\n')

                # Sleep here to prevent suspicious bot activities
                time.sleep(randint(1, 3))

                # Go back to Search Results
                back_button = driver.find_element_by_xpath(
                    "//input[contains(@class, 'commandButton_BACK-') and @value='Back to Search Results' and @type='submit']")
                back_button.click()

                # Sleep here to prevent suspicious bot activities
                time.sleep(randint(1, 2))

            # When next button is unclickable, simply break the loop
            wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located(
                (By.XPATH, "//input[@class='formRepeatPagination2_NAVIGATION-BUTTON' and @value='Next']")))
            end = False
            try:
                next_button = driver.find_element_by_xpath(
                    "//input[@class='formRepeatPagination2_NAVIGATION-BUTTON' and @value='Next']")
                next_button.click()
                logger.info('Going Next Page \n')

                # Before scraping all information from the next page, update the csv on S3 if there exist a bucket for it.
                upload = upload_csv(s3bucket, s3path, csvname)
                time.sleep(3)

                # Make sure the page is reloaded with new links, then go next
                next_counter = 0
                while True:
                    # Grab all Reference Numbers from the navigation page
                    new_navigation = BeautifulSoup(
                        driver.page_source, 'html.parser')
                    newnumbers = new_navigation.find_all(
                        name='div', class_='formSectionHeader6_TEXT')
                    newnumbers = list(map(lambda x: x.text, newnumbers))

                    # If it is the last page, check if last button is disabled. Then End Search.
                    end_disabled_button = new_navigation.find('input', attrs={'type': 'submit',
                                                                            'disabled': 'disabled',
                                                                            'value': 'Last'})
                    if end_disabled_button:
                        logger.info("Reached The End of The Search Results.")
                        end = True
                        break
                    
                    if next_counter == 3:
                        logger.info('Iterating too many times. Breaking loop.')
                        driver.quit()
                        return False

                    if newnumbers[0] == referencenumbers[0]:
                        logger.warning(
                            'The page is not loaded properly yet after pressing Next. Wait a moment.')
                        next_counter += 1
                        time.sleep(5)
                    else:
                        break

                # Break the loop when the end has reached.
                if end:
                    break
                
            except Exception as e:
                logger.info(e)
                logger.info(traceback.print_tb(e.__traceback__))
                logger.info("Error in Going to the Next Page.")
                driver.quit()
                return False

    except Exception as e:
        driver.quit()
        logger.info(e)
        logger.info(traceback.print_tb(e.__traceback__))
        return False

    # Stop Driver
    driver.quit()
    export_csv(df_dict, csvname, headertuple)
    return True


# Individual Page Scraping -------------------------------------------------------------------------------
def individual_page_scraping(index, row, headertuple, referencenumber, projecttype, current_status, driver):
    """
    For BS4 Scraping for each page
    """
    # Wait for page to come back to filtered results
    wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located(
        (By.XPATH, "//div[@class='selectManyMenu_SELECTED-ITEM-TR' and @title='Construction &#8658; Others']")))

    # Click onto the current link
    link = driver.find_elements_by_xpath(
        "//a[contains(@class, 'commandLink_TITLE-') and @href='#' and @type='link']")[index]
    driver.execute_script("arguments[0].click();", link)

    # Wait for link to load
    wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located(
        (By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and @value='Overview' and @type='submit']")))

    # Start BeautifulSoup
    content_soup = BeautifulSoup(driver.page_source, 'html.parser')

    # If it is a second stage tender, it means that the information has already been scraped. THe award details will be scraped from the link.
    # Thus, we should simply skip this row.
    section_check = content_soup.find('span', text='Agency')
    if not section_check:
        tender_information = 'This is a Two-Stage Tender. No overview information will be available.'
        tender_info_section = content_soup.find(text=tender_information)
        logger.info(tender_info_section)
        if tender_info_section:
            logger.info('This row has been scraped previously as it is a second stage tendor project. \n')
            return "skip"

    # Project Name
    projectname_regex = re.compile(".*formOutputText_.* outputText_TITLE-.*")
    projectname = content_soup.find(
        'div', attrs={'class': projectname_regex}).text

    # Agency
    agency = find_value_via_row(content_soup, 'Agency')

    # Published Date
    published_date = find_value_via_row(content_soup, 'Published')

    # Procurement Category
    procurement_category = find_value_via_row(
        content_soup, 'Procurement Category')

    # Closed Date
    closed_text = content_soup.find(text='Closed')
    closed_section = closed_text.find_parent(
        'div', attrs={'class': 'formColumns_COLUMN-TABLE'})
    closed_regex = re.compile('formOutputText_HIDDEN-LABEL outputText_NAME-.*')
    closed_date = closed_section.find(
        'div', attrs={'class': closed_regex}).text
    closed_date = '%s %s' % (closed_date[:-7], closed_date[-7:])

    # # Current Status
    # status_regex = re.compile('label_MAIN label_.*')
    # current_status = content_soup.find(
    #     'div', attrs={'class': status_regex}).text

    # Qualification Criteria
    # This only applies if the project type is qualification.
    if projecttype.lower() == 'qualification':
        qualification_criteria = find_value_via_row(
            content_soup, 'Qualification Criteria')

    else:
        qualification_criteria = 'Not Applicable'

    # Respondents
    respondents_string = ""

    try:
        # Click to next tab
        respondent_button = driver.find_element_by_xpath(
            "//input[contains(@value, 'Respondents') and @class='formTabBar_TAB-BUTTON' and @type='submit']")
        respondent_button.click()
        # Wait for it to load to the next tab
        wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located(
            (By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and contains(@value, 'Respondents') and @type='submit' and @disabled='disabled']")))

    except:
        respondents_string = 'No Respondents'

    if not respondents_string:
        # Loop till you find all the respondents
        respondent_counter = 0
        while True:
            # Sleep here to prevent suspicious bot activities
            time.sleep(randint(1, 2))

            respondent_content = BeautifulSoup(
                driver.page_source, 'html.parser')

            # Grab the total number of respondents who responded to compare with the number of respondents we have
            try:
                respondent_number_regex = re.compile('.*outputText_TITLE-.*')
                respondent_number = respondent_content.find(
                    'span', attrs={'class': respondent_number_regex}).text
            except:
                respondent_number_regex = re.compile(
                    'formOutputText_HIDDEN-LABEL outputText_TITLE-.*')
                respondent_number = respondent_content.find_all(
                    'div', attrs={'class': respondent_number_regex})[1].text
            # logger.info("Respondent Statement %s" % respondent_number)

            # Stop everything when there is no supplier responded
            if respondent_number == "No supplier responded.":
                respondents_string = 'No Respondents'
                break

            try:
                # If there is no respondent cos the page has not loaded, this will throw an error to restart the loop
                respondent_number = int(
                    re.sub("[^0-9]", "", respondent_number))
                # logger.info("Respondent Number: %s" % respondent_number)
            except:
                if respondent_counter == 3:
                    logger.info('Iterating too many times. Breaking loop.')
                    driver.quit()
                    return False

                respondent_counter += 1
                logger.warning(
                    'Respondent Page not loaded properly. Wait a moment.')
                continue

            # There are different format for different status (I believe) thus must handle differently
            if current_status.lower() == 'closed':
                # logger.info('Respondents - Status is closed')
                respondent_section = respondent_content.find(
                    'table', attrs={'class': 'formContainer_TABLE'})
                # logger.info(respondent_section)
                respondents_divs = respondent_section.find_all(
                    'div', attrs={'class': 'formRow_HIDDEN-LABEL'})
                # logger.info(respondents_divs)
                respondents = list(
                    map(lambda x: x.find('span'), respondents_divs))
                # logger.info(respondents)
                respondents = list(map(lambda x: x.text, respondents))
                # logger.info(respondents)
            else:
                # respondent_section_regex = re.compile(
                #     'accordion2_formAccordionGroup_BODY-CONTENT')
                # logger.info('Respondents - Status is not closed')
                # respondent_section = respondent_content.find('div', attrs={'id': respondent_section_regex, 'class': 'formAccordionGroup_BODY-CONTENT-DIV'})
                # logger.info(respondent_section)
                respondent_regex = re.compile('.*_title-text')
                respondents = respondent_content.find_all(
                    'div', attrs={'id': respondent_regex, 'class': 'formAccordion_TITLE-TEXT'})
                # logger.info(respondents)
                respondents = list(map(lambda x: x.text, respondents))
                # logger.info(respondents)

            # Convert list to string
            respondents_string = '; '.join(respondents)

            # If the number of respondents matches the number of actual respondents we scrape, we can continue
            if respondent_number == int(len(respondents)):
                break
            else:
                logger.warning(
                    "Numbers do not match. Actual Number is: %s" % int(len(respondents)))
                logger.info(referencenumber)
                respondent_counter += 1
                time.sleep(3)

    if current_status.lower() == 'awarded':
        # Click to next tab
        award_button = driver.find_element_by_xpath(
            "//input[contains(@value, 'Award') and @class='formTabBar_TAB-BUTTON' and @type='submit']")
        award_button.click()

        # Wait for it to load to the next tab
        wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located(
            (By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and contains(@value, 'Award') and @type='submit' and @disabled='disabled']")))

        # Ensure there is a value no matter what.
        awarded_counter = 0
        while True:
            # Sleep here to prevent suspicious bot activities
            time.sleep(randint(1, 2))

            award_content = BeautifulSoup(driver.page_source, 'html.parser')

            # There is a 2nd Stage Award? If have, open this link to access the award section.
            award_section = award_content.find(text='Awarded to')
            if not award_section:
                logger.warning("Need to access second stage award.")
                award_link = driver.find_element_by_xpath("//a[contains(@class, 'commandLink_SECONDARY-') and @href='#' and @type='link']")
                driver.execute_script("arguments[0].click();", award_link)
                # Wait for it to load to the next tab
                wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.XPATH, "//div[@class='formSectionHeader4_TEXT' and contains(text(),'Awarding')]")))
                # Need to rescrape the page
                award_content = BeautifulSoup(driver.page_source, 'html.parser')

            # Awarded To
            award_section = award_content.find(text='Awarded to')
            award_parent = award_section.find_parent('table')
            award_parent = award_parent.find_parent('table')
            awarded_regex = re.compile(
                'formOutputText_HIDDEN-LABEL outputText_TITLE-.*')
            awarded_to = award_parent.find(
                'div', attrs={'class': awarded_regex}).text
            # logger.info('Award To Text %s' % awarded_to)

            # Awarded Value
            value_regex = re.compile('formOutputText_VALUE-DIV.*')
            awarded_value = award_parent.find(
                'div', attrs={'class': value_regex}).text
            # logger.info('Award Value Text %s' % awarded_value)
            # Convert to Float
            awarded_value = float(re.sub(r'[^\d\-.]', '', awarded_value))

            # Somehow it is possible to be awarded 0.00 SGD... e.g.PUB000ETT19300085
            if awarded_to and (awarded_value or awarded_value == 0):
                break
            else:
                logger.info('Award value (%s) or Awarded To (%s) is / are not present.' %
                            (awarded_value, awarded_to))
                if awarded_counter == 3:
                    logger.info('Iterating too many times. Breaking loop.')
                    driver.quit()
                    return False

                awarded_counter += 1
                logger.warning(
                    'Awarded Page not loaded properly. Wait a moment.')

    else:
        awarded_to = 'Not Applicable'
        awarded_value = 'Not Applicable'

    row[headertuple.projname] = projectname
    row[headertuple.agency] = agency
    row[headertuple.pubdate] = published_date
    row[headertuple.procurecat] = procurement_category
    row[headertuple.closeddate] = closed_date
    # row[headertuple.curstatus] = current_status
    row[headertuple.qualstatus] = qualification_criteria
    row[headertuple.resp] = respondents_string
    row[headertuple.awardedto] = awarded_to
    row[headertuple.awardedval] = awarded_value

    return True


# Helper Methods -----------------------------------------------------------------------------------------
def find_value_via_row(soup, header_text):
    """
    Helper Method to find specific content
    """
    section = soup.find('span', text=header_text)
    regex = re.compile('row.*form2_ROW-TABLE.*')
    parent = section.find_parent("div", attrs={'class': regex})
    value = parent.find('div', class_='formOutputText_VALUE-DIV').text
    return value

def find_values_in_navigation(soup, filter_type, filter_text):
    """
    Search via _class, but we can change it to XPATH if needed.
    Returns a list of all the values we found as this is scraping at the navigation page.
    """
    regex = re.compile(filter_text)
    results = soup.find_all(filter_type, attrs={'class': regex})
    results = list(map(lambda x: x.text, results))

    return results

def skip_rows(skip_counter, driver):
    """
    Skip rows depending on how many is already scraped.
    Return True when the skip is done. 
    """
    # Open the current csvfile, skip to the latest row and start from there by selenium in mulitples of 10 or whatever length each view has.
    try:
        lenlinks = len(driver.find_elements_by_xpath(
            "//a[contains(@class, 'commandLink_TITLE-') and @href='#' and @type='link']"))
        logger.info('Page Link Numbers: %s' % lenlinks)

        while skip_counter >= lenlinks and skip_counter > 0:
            # Grab all Reference Numbers from the navigation page
            navigation_soup = BeautifulSoup(driver.page_source, 'html.parser')
            referencenumbers = navigation_soup.find_all(
                name='div', class_='formSectionHeader6_TEXT')
            referencenumbers = list(map(lambda x: x.text, referencenumbers))

            # When next button is unclickable, simply break the loop
            try:
                next_button = driver.find_element_by_xpath(
                    "//input[@class='formRepeatPagination2_NAVIGATION-BUTTON' and @value='Next']")
                next_button.click()
                logger.info('Going Next Page cos it has already been scraped.')
                time.sleep(3)

                # Make sure the page is reloaded with new links, then go next
                wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located(
                    (By.XPATH, "//input[@class='formRepeatPagination2_NAVIGATION-BUTTON' and @value='Next']")))
                next_counter = 0
                while True:
                    # Grab all Reference Numbers from the navigation page
                    new_navigation = BeautifulSoup(
                        driver.page_source, 'html.parser')
                    newnumbers = new_navigation.find_all(
                        name='div', class_='formSectionHeader6_TEXT')
                    newnumbers = list(map(lambda x: x.text, newnumbers))
                    if next_counter == 3:
                        logger.info('Iterating too many times. Breaking loop.')
                        driver.quit()
                        return False

                    if newnumbers[0] == referencenumbers[0]:
                        logger.warning(
                            'The page is not loaded properly yet after pressing Next. Wait a moment.')
                        next_counter += 1
                        time.sleep(5)
                    else:
                        break

            # If next button cannot be pressed here, the whole script should stop.
            except Exception as e:
                logger.info(e)
                logger.info("Reached The End of The Search Results.")
                driver.quit()
                return False

            # Reduce by the number of links skipped
            skip_counter -= lenlinks
            logger.info("Skip Row Count left: %s" % skip_counter)

    except Exception as e:
        logger.info(e)

    finally:
        return True

def duplicate_entry(referencenumber, projecttype, status, df_dict, headertuple):
    """
    Determine if the row needs to be scraped again or not. 
    This can be done by checking the reference number, projecttype and the status of the project.
    """
    if referencenumber in df_dict:

        if df_dict[referencenumber][headertuple.curstatus] == status and \
            df_dict[referencenumber][headertuple.projtype] == projecttype:

            if headertuple.lastupdated in df_dict[referencenumber]:
                if df_dict[referencenumber][headertuple.lastupdated]:
                     if str(df_dict[referencenumber][headertuple.lastupdated]).lower() != 'nan':
                        return True
    
    return False

def download_csv(s3bucket, s3path, csvname):
    """
    Download CSV from S3 Bucket.
    If cannot download, just return False.
    """
    try:
        s3 = boto3.resource('s3', region_name='ap-southeast-1')
        bucket = s3.Bucket(s3bucket)
        if bucket.objects.filter(Prefix=s3path):
            bucket.download_file(s3path, csvname)
            return True
        else:
            return False
    except (ClientError, NoCredentialsError, BotoCoreError):
        return False

def upload_csv(s3bucket, s3path, csvname):
    """
    Upload CSV from local folder to S3 Bucket
    If cannot download, just return False.
    """
    try:
        s3 = boto3.resource('s3', region_name='ap-southeast-1')
        bucket = s3.Bucket(s3bucket)
        s3.meta.client.upload_file(csvname, s3bucket, s3path)
        if bucket.objects.filter(Prefix=s3path):
            return True
        else:
            return False
    except (ClientError, NoCredentialsError, BotoCoreError):
        return False

def export_csv(df_dict, csvname, headertuple):
    """
    Export CSV to local directory.
    """
    df = pd.DataFrame(df_dict).T
    df.index.names = [headertuple.refno]
    # Set default update to 2019-10-04
    # df[headertuple.lastupdated].fillna('2019-10-04')
    # Replace all "Not Applicable" to nan instead
    # df = df.replace('Not Applicable', float('NaN'))
    df = df.sort_values(by=[headertuple.lastupdated, headertuple.awardedval], ascending=[False, False])
    df.to_csv(csvname, index=True)
    return True

def delete_csv(csvname):
    """
    Delete CSV from local directory
    """
    os.remove(csvname)
    if os.path.exists(csvname):
        return False
    else:
        return True


# Script to Run
if __name__ == '__main__':
    main()
