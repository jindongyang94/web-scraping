from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from bs4 import BeautifulSoup
import requests

import re, sys, os, traceback, logging
import time
import csv
from pprint import pformat
from random import randint
import numpy as np
import pandas as pd

## Logger --------------------------------------------------------------------------------------
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

## Main Method ---------------------------------------------------------------------------------
def gebiz_scraping(url, headers, csvname):
    """
    Scraping Workflow will be placed here with necessary headers and url link.
    Exporting to CSV will be done outside of the function.
    """
    options = Options()
    options.headless = True
    options.add_argument("--window-size=1920,1920")
    options.add_argument("--incognito")
    driver = webdriver.Chrome(options=options, executable_path=r'/usr/local/bin/chromedriver')
    driver.get(url)

    # @name='contentForm:j_idt202_selectManyMenu_BUTTON'
    menu_button = driver.find_elements_by_xpath("//input[@class='selectManyMenu_BUTTON' and @value='All' and @type='button']")[2]
    menu_button.click()

    # Make script wait for page to load for the filter options to appear
    wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.XPATH, "//input[contains(@name, 'GroupOption_Construction') and @value='Construction']")))

    filter_button = driver.find_element_by_xpath("//input[contains(@name, 'GroupOption_Construction') and @value='Construction']")
    filter_button.click()

    closed_button = driver.find_element_by_xpath("//input[@class='formTabBar_TAB-BUTTON' and contains(@value, 'Closed')]")
    closed_button.click()

    # Make script wait for page to load
    wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and contains(@value, 'Closed') and \
        @disabled='disabled']")))

    # Make script wait for page to load for the filter to work
    wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.XPATH, "//tr[@class='selectManyMenu_SELECTED-ITEM-TR' and @title='Construction &#8658; Civil Engineering']")))

    # Full Dataset 
    # If there is an existing csv, simply open the whole thing in pandas
    # It is actually better to save the results as a list of dictionaries and then convert ot df back later for faster processing
    if os.path.exists(csvname):
        df = pd.read_csv(csvname, header=0, index_col=[0])
        df_dict = df.to_dict('records')
    else:
        df_dict = []
        
    # Do a skip_counter to skip and go back to whereever it is stopped in the first place.
    # Do not count headers, but include one extra cos when the rows hit 10, it should go to the next page. 
    skip_counter = len(df_dict)
    logger.info('Existing Rows in CSV: %s'  % skip_counter)

    # Open the current csvfile, skip to the latest row and start from there by selenium in mulitples of 10 or whatever length each view has.
    try:
        lenlinks = len(driver.find_elements_by_xpath("//a[contains(@class, 'commandLink_TITLE-') and @href='#' and @type='link']"))
        logger.info('Page Link Numbers: %s' % lenlinks)

        while skip_counter >= lenlinks and skip_counter > 0:
            # Grab all Reference Numbers from the navigation page
            navigation_soup = BeautifulSoup(driver.page_source, 'html.parser')
            referencenumbers = navigation_soup.find_all(name='div', class_='formSectionHeader6_TEXT')
            referencenumbers = list(map(lambda x: x.text, referencenumbers))

            # When next button is unclickable, simply break the loop
            try:
                next_button = driver.find_element_by_xpath("//input[@class='formRepeatPagination2_NAVIGATION-BUTTON' and @value='Next']")
                next_button.click()
                logger.info('Going Next Page cos it has already been scraped.')
                time.sleep(3)

                # Make sure the page is reloaded with new links, then go next
                wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located((By.XPATH, "//input[@class='formRepeatPagination2_NAVIGATION-BUTTON' and @value='Next']")))
                next_counter=0
                while True:
                    # Grab all Reference Numbers from the navigation page
                    new_navigation = BeautifulSoup(driver.page_source, 'html.parser')
                    newnumbers = new_navigation.find_all(name='div', class_='formSectionHeader6_TEXT')
                    newnumbers = list(map(lambda x: x.text, newnumbers))
                    if next_counter == 3:
                        logger.info('Iterating too many times. Breaking loop.')
                        driver.quit()
                        return df_dict

                    if newnumbers[0] == referencenumbers[0]:
                        logger.warning('The page is not loaded properly yet after pressing Next. Wait a moment.')
                        next_counter+=1
                        time.sleep(5)
                    else:
                        break

            # If next button cannot be pressed here, the whole script should stop.
            except Exception as e:
                logger.info(e)
                logger.info("Reached The End of The Search Results.")
                driver.quit()
                return df_dict

            # Reduce by the number of links skipped
            skip_counter -= lenlinks
            logger.info("Skip Row Count left: %s" % skip_counter)

    except Exception as e:
        logger.info(e)

    # Do a loop here to click Next after all links are clicked
    try:
        while True:
            # First I have to collect the links for each button to be clicked.
            # Click to link -> Grab information --> Click to Back Search Results --> Go to next button to click.
            # The link can become stale. So, lets maintain a counter instead.
            lenlinks = len(driver.find_elements_by_xpath("//a[contains(@class, 'commandLink_TITLE-') and @href='#' and @type='link']"))
            
            # Grab all Reference Numbers from the navigation page
            navigation_soup = BeautifulSoup(driver.page_source, 'html.parser')
            referencenumbers = navigation_soup.find_all(name='div', class_='formSectionHeader6_TEXT')
            referencenumbers = list(map(lambda x: x.text, referencenumbers))

            for index in range(lenlinks):
                if skip_counter > 0:
                    skip_counter -= 1
                    logger.info('Skipping current link.')
                    logger.info('Skip Row Count Left: %s' % skip_counter)
                    continue

                # Wait for page to come back to filtered results
                wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located((By.XPATH, "//tr[@class='selectManyMenu_SELECTED-ITEM-TR' and @title='Construction &#8658; Civil Engineering']")))
                
                link = driver.find_elements_by_xpath("//a[contains(@class, 'commandLink_TITLE-') and @href='#' and @type='link']")[index]
                driver.execute_script("arguments[0].click();", link)

                # Wait for link to load
                wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located((By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and @value='Overview' and @type='submit']")))

                # Start BeautifulSoup
                content_soup = BeautifulSoup(driver.page_source, 'html.parser')

                # Project Type and Reference Number
                referencenumber = referencenumbers[index]
                projecttype, referencenumber = referencenumber.split('-', 1)
                projecttype = projecttype.strip()
                referencenumber = referencenumber.strip()
                logger.info("Reference Number %s" % referencenumber)

                # Append into a row dictionary
                # New row to be built
                row = {key: None for key in headers}
                row['Project Type'] = projecttype
                row['Reference Number'] = referencenumber
                
                # Rest of the attributes
                result = individual_page_scraping(row, referencenumber, projecttype, content_soup, driver)

                # If result is False, end the whole program
                if not result:
                    driver.quit()
                    return df_dict

                logger.info('')
                logger.info('Page Link Number: %s' % index)
                for line in pformat(row).split('\n'):
                    logging.info(line)
                logger.info('\n')

                # # Append it back to Dataset and export it continuously - This slows down the process a lot but it is necessary to maintain all the rows so we don't do extra work.
                df_dict.append(row)
                df = pd.DataFrame(df_dict)
                df.to_csv(csvname, index=True)

                # Sleep here to prevent suspicious bot activities
                time.sleep(randint(1,4))

                # Go back to Search Results
                back_button = driver.find_element_by_xpath("//input[contains(@class, 'commandButton_BACK-') and @value='Back to Search Results' and @type='submit']")
                back_button.click()
                
                # Sleep here to prevent suspicious bot activities
                time.sleep(randint(1,2))
            
            # When next button is unclickable, simply break the loop
            wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located((By.XPATH, "//input[@class='formRepeatPagination2_NAVIGATION-BUTTON' and @value='Next']")))
            try:
                next_button = driver.find_element_by_xpath("//input[@class='formRepeatPagination2_NAVIGATION-BUTTON' and @value='Next']")
                next_button.click()
                logger.info('Going Next Page \n')
                time.sleep(3)

                # Make sure the page is reloaded with new links, then go next
                next_counter = 0
                while True:
                    # Grab all Reference Numbers from the navigation page
                    new_navigation = BeautifulSoup(driver.page_source, 'html.parser')
                    newnumbers = new_navigation.find_all(name='div', class_='formSectionHeader6_TEXT')
                    newnumbers = list(map(lambda x: x.text, newnumbers))
                    if next_counter == 3:
                        logger.info('Iterating too many times. Breaking loop.')
                        driver.quit()
                        return df_dict

                    if newnumbers[0] == referencenumbers[0]:
                        logger.warning('The page is not loaded properly yet after pressing Next. Wait a moment.')
                        next_counter+=1
                        time.sleep(5)
                    else:
                        break
            except:
                logger.info("Reached The End of The Search Results.")
                break
    
    except Exception as e:
        driver.quit()
        logger.info(e)
        logger.info(traceback.print_tb(e.__traceback__))
        return df_dict

    # Stop Driver
    driver.quit()
    return df_dict

## Helper Methods ------------------------------------------------------------------------------
def individual_page_scraping(row, referencenumber, projecttype, content_soup, driver):
    """
    For BS4 Scraping for each page
    """
    # Project Name 
    projectname_regex = re.compile(".*formOutputText_.* outputText_TITLE-.*")
    projectname = content_soup.find('div', attrs={'class': projectname_regex}).text

    # Agency
    agency = find_value_via_row(content_soup, 'Agency')

    # Published Date
    published_date = find_value_via_row(content_soup, 'Published')

    # Procurement Category
    procurement_category = find_value_via_row(content_soup, 'Procurement Category')

    # Closed Date
    closed_text = content_soup.find(text='Closed')
    closed_section = closed_text.find_parent('table', attrs={'class': 'form2_ROW-TABLE'})
    closed_section = closed_section.find_parent('tbody')
    closed_regex = re.compile('formOutputText_HIDDEN-LABEL outputText_NAME-.*')
    closed_date = closed_section.find('div', attrs={'class': closed_regex}).text
    closed_date = '%s %s' % (closed_date[:-7], closed_date[-7:])

    # Current Status
    status_regex = re.compile('label_MAIN label_.*')
    current_status = content_soup.find('div', attrs={'class': status_regex}).text

    # Qualification Criteria
    # This only applies if the project type is qualification.
    if projecttype.lower() == 'qualification':
        qualification_criteria = find_value_via_row(content_soup, 'Qualification Criteria')

    else:
        qualification_criteria = 'Not Applicable'

    # Respondents
    respondents_string = ""

    try:
        # Click to next tab
        respondent_button = driver.find_element_by_xpath("//input[contains(@value, 'Respondents') and @class='formTabBar_TAB-BUTTON' and @type='submit']")
        respondent_button.click()
        # Wait for it to load to the next tab
        wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and contains(@value, 'Respondents') and @type='submit' and @disabled='disabled']")))

    except:
        respondents_string = 'No Respondents'

    if not respondents_string:
        # Loop till you find all the respondents
        respondent_counter = 0
        while True:
            # Sleep here to prevent suspicious bot activities
            time.sleep(randint(1,2))

            respondent_content = BeautifulSoup(driver.page_source, 'html.parser')

            # Grab the total number of respondents who responded to compare with the number of respondents we have
            try:
                respondent_number_regex = re.compile('.*outputText_TITLE-.*')
                respondent_number = respondent_content.find('span', attrs={'class': respondent_number_regex}).text
            except:
                respondent_number_regex = re.compile('formOutputText_HIDDEN-LABEL outputText_TITLE-.*')
                respondent_number = respondent_content.find_all('div', attrs={'class': respondent_number_regex})[1].text
            # logger.info("Respondent Statement %s" % respondent_number)

            # Stop everything when there is no supplier responded
            if respondent_number == "No supplier responded.":
                respondents_string = 'No Respondents'
                break

            try:
                # If there is no respondent cos the page has not loaded, this will throw an error to restart the loop
                respondent_number = int(re.sub("[^0-9]", "", respondent_number))
                # logger.info("Respondent Number: %s" % respondent_number)
            except:
                if respondent_counter == 3:
                    logger.info('Iterating too many times. Breaking loop.')
                    driver.quit()
                    return False

                respondent_counter+=1
                logger.warning('Respondent Page not loaded properly. Wait a moment.')
                continue
            
            # There are different format for different status (I believe) thus must handle differently
            if current_status.lower() == 'closed':
                # logger.info('Respondents - Status is closed')
                respondent_section = respondent_content.find('table', attrs={'class': 'formContainer_TABLE'})
                # logger.info(respondent_section)
                respondents_divs = respondent_section.find_all('div', attrs={'class': 'formRow_HIDDEN-LABEL'})
                # logger.info(respondents_divs)
                respondents = list(map(lambda x: x.find('span'), respondents_divs))
                # logger.info(respondents)
                respondents = list(map(lambda x: x.text, respondents))
                # logger.info(respondents)
            else:
                respondent_section_regex = re.compile('accordion2_formAccordionGroup_BODY-CONTENT')
                # logger.info('Respondents - Status is not closed')
                # respondent_section = respondent_content.find('div', attrs={'id': respondent_section_regex, 'class': 'formAccordionGroup_BODY-CONTENT-DIV'})
                # logger.info(respondent_section)
                respondent_regex = re.compile('.*_title-text')
                respondents = respondent_content.find_all('div', attrs={'id': respondent_regex, 'class': 'formAccordion_TITLE-TEXT'})
                # logger.info(respondents)
                respondents = list(map(lambda x: x.text, respondents))
                # logger.info(respondents)
            
            # Convert list to string
            respondents_string = '; '.join(respondents)

            # If the number of respondents matches the number of actual respondents we scrape, we can continue
            if respondent_number == int(len(respondents)):
                break
            else:
                logger.warning("Numbers do not match. Actual Number is: %s" % int(len(respondents)))
                logger.info(referencenumber)
                respondent_counter+=1
                time.sleep(3)

    if current_status.lower() == 'awarded':
        # Click to next tab
        award_button = driver.find_element_by_xpath("//input[contains(@value, 'Award') and @class='formTabBar_TAB-BUTTON' and @type='submit']")
        award_button.click()

        # Wait for it to load to the next tab
        wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and contains(@value, 'Award') and @type='submit' and @disabled='disabled']")))

        # Ensure there is a value no matter what.
        awarded_counter = 0
        while True:
            # Sleep here to prevent suspicious bot activities
            time.sleep(randint(1,2))

            award_content = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Awarded To
            award_section = award_content.find(text='Awarded to')
            award_parent = award_section.find_parent('table')
            award_parent = award_parent.find_parent('table')
            awarded_regex = re.compile('formOutputText_HIDDEN-LABEL outputText_TITLE-.*')
            awarded_to = award_parent.find('div', attrs={'class': awarded_regex}).text
            # logger.info('Award To Text %s' % awarded_to)

            # Awarded Value
            value_regex = re.compile('formOutputText_VALUE-DIV.*')
            awarded_value = award_parent.find('div', attrs={'class': value_regex}).text
            # logger.info('Award Value Text %s' % awarded_value)
            # Convert to Float
            awarded_value = float(re.sub(r'[^\d\-.]', '', awarded_value))

            # Somehow it is possible to be awarded 0.00 SGD... e.g.PUB000ETT19300085
            if awarded_to and (awarded_value or awarded_value == 0):
                break
            else:
                logger.info('Award value (%s) or Awarded To (%s) is / are not present.' % (awarded_value, awarded_to))
                if awarded_counter == 3:
                    logger.info('Iterating too many times. Breaking loop.')
                    driver.quit()
                    return False

                awarded_counter+=1
                logger.warning('Awarded Page not loaded properly. Wait a moment.')

    else:
        awarded_to = 'Not Applicable'
        awarded_value = 'Not Applicable'

    row['Project Name'] = projectname
    row['Agency'] = agency
    row['Published Date'] = published_date
    row['Procurement Category'] = procurement_category
    row['Closed Date'] = closed_date
    row['Current Status'] = current_status
    row['Qualification Criteria'] = qualification_criteria
    row['Respondents'] = respondents_string
    row['Awarded To'] = awarded_to
    row['Awarded Value (SGD)'] = awarded_value

    return True

def find_value_via_row(soup, header_text):
    """
    Helper Method to find specific content
    """
    section = soup.find('span', text=header_text)
    parent = section.find_parent("tr")
    value = parent.find('div', class_='formOutputText_VALUE-DIV').text
    return value

## Script to Run
if __name__ == '__main__':

    url = 'https://www.gebiz.gov.sg/ptn/opportunity/BOListing.xhtml?origin=search'
    
    headers = ['Project Type', 'Reference Number', 'Project Name', 'Agency', 'Published Date', 'Procurement Category', 'Closed Date', 'Current Status', \
        'Qualification Criteria', 'Respondents', 'Awarded To', 'Awarded Value (SGD)']

    csvname = 'data.csv'

    df_dict = gebiz_scraping(url, headers, csvname)

    df = pd.DataFrame(df_dict)
    df.to_csv(csvname, index=True)

    # # Print the dataset for visuals
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #     logger.info(df)
