from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from bs4 import BeautifulSoup
import requests

import re
import time
from pprint import pprint
from random import randint
import numpy as np
import pandas as pd

url = 'https://www.gebiz.gov.sg/ptn/opportunity/BOListing.xhtml?origin=search'


options = Options()
options.headless = True
# options.add_argument("--window-size=1920,1200")
options.add_argument("--incognito")
driver = webdriver.Chrome(options=options, executable_path=r'/usr/local/bin/chromedriver')
# driver.implicitly_wait(30)
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

# Full Dataset --> Convert to np later
full_dataset = []

# # For testing
# counter = 0

# Do a loop here to click Next after all links are clicked
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
        # New row to be built
        row = []

        # Wait for page to come back to filtered results
        wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located((By.XPATH, "//tr[@class='selectManyMenu_SELECTED-ITEM-TR' and @title='Construction &#8658; Civil Engineering']")))
        
        link = driver.find_elements_by_xpath("//a[contains(@class, 'commandLink_TITLE-') and @href='#' and @type='link']")[index]
        driver.execute_script("arguments[0].click();", link)

        # Wait for link to load
        wait = WebDriverWait(driver, 10).until(ec.presence_of_element_located((By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and @value='Overview' and @type='submit']")))


        content_soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Project Type and Reference Number
        referencenumber = referencenumbers[index]
        projecttype, referencenumber = referencenumber.split('-', 1)
        projecttype = projecttype.strip()
        referencenumber = referencenumber.strip()
        row.append(projecttype)
        row.append(referencenumber)

        # Project Name 
        projectname_regex = re.compile(".*formOutputText_.* outputText_TITLE-.*")
        projectname = content_soup.find('div', attrs={'class': projectname_regex}).text
        row.append(projectname)

        # As the method to find each content 
        def find_value_via_row(soup, header_text):
            section = soup.find('span', text=header_text)
            parent = section.find_parent("tr")
            value = parent.find('div', class_='formOutputText_VALUE-DIV').text
            return value

        # Agency
        agency = find_value_via_row(content_soup, 'Agency')
        row.append(agency)

        # Published Date
        published_date = find_value_via_row(content_soup, 'Published')
        row.append(published_date)

        # Procurement Category
        procurement_category = find_value_via_row(content_soup, 'Procurement Category')
        row.append(procurement_category)

        # Closed Date
        closed_text = content_soup.find(text='Closed')
        closed_section = closed_text.find_parent('table', attrs={'class': 'form2_ROW-TABLE'})
        closed_section = closed_section.find_parent('tbody')
        closed_regex = re.compile('formOutputText_HIDDEN-LABEL outputText_NAME-.*')
        closed_date = closed_section.find('div', attrs={'class': closed_regex}).text
        closed_date = '%s %s' % (closed_date[:-7], closed_date[-7:])
        row.append(closed_date)

        # Current Status
        status_regex = re.compile('label_MAIN label_.*')
        current_status = content_soup.find('div', attrs={'class': status_regex}).text
        row.append(current_status)

        # Qualification Criteria
        # This only applies if the project type is qualification.
        if projecttype.lower() == 'qualification':
            qualification_criteria = find_value_via_row(content_soup, 'Qualification Criteria')
        else:
            qualification_criteria = 'Not Applicable'
        row.append((qualification_criteria))

        # Respondents
        if current_status.lower() in ['pending award', 'cancelled']:
            respondents = ''
        else:
            try:
                # Click to next tab
                respondent_button = driver.find_element_by_xpath("//input[contains(@value, 'Respondents') and @class='formTabBar_TAB-BUTTON' and @type='submit']")
                respondent_button.click()
                # Wait for it to load to the next tab
                wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and contains(@value, 'Respondents') and @type='submit' and @disabled='disabled']")))

                respondent_content = BeautifulSoup(driver.page_source, 'html.parser')
                respondent_section_regex = re.compile(".*formColumns_COLUMN-TD")
                respondent_section = respondent_content.find('td', attrs={'class': respondent_section_regex})
                respondents = respondent_section.find_all('span')
                respondents = ', '.join(respondents)
            except:
                respondents = 'No Respondents Despite Closed/Awarded'
        
        row.append(respondents)

        if current_status.lower() == 'awarded':
            # Click to next tab
            award_button = driver.find_element_by_xpath("//input[contains(@value, 'Award') and @class='formTabBar_TAB-BUTTON' and @type='submit']")
            award_button.click()

            # Wait for it to load to the next tab
            wait = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.XPATH, "//input[@class='formTabBar_TAB-BUTTON-ACTIVE' and contains(@value, 'Award') and @type='submit' and @disabled='disabled']")))

            award_content = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Awarded To
            award_section = award_content.find(text='Awarded to')
            award_parent = award_section.find_parent('tbody')
            awarded_regex = re.compile('formOutputText_HIDDEN-LABEL outputText_TITLE-.*')
            awarded_to = award_parent.find('div', attrs={'class': awarded_regex})

            # Awarded Value
            value_regex = re.compile('formOutputText_VALUE-DIV.*')
            awarded_value = award_parent.find('div', attrs={'class': value_regex})

        else:
            awarded_to = 'Not Applicable'
            awarded_value = 'Not Applicable'

        row.append(awarded_to)
        row.append(awarded_value)

        pprint(row)
        print()

        # Append it back to Dataset
        full_dataset.append(row)

        # Sleep here to prevent suspicious bot activities
        time.sleep(randint(1,4))

        # Go back to Search Results
        back_search = driver.find_element_by_xpath("//input[contains(@class, 'commandButton_BACK-') and @value='Back to Search Results' and @type='submit']")
        back_search.click()

        # Sleep here to prevent suspicious bot activities
        time.sleep(randint(1,2))
    
    # # For testing
    # if counter == 3:
    #     break
    # counter += 1

    # When next button is unclickable, simply break the loop
    try:
        next_button = driver.find_element_by_xpath("//input[@class='formRepeatPagination2_NAVIGATION-BUTTON' and @value='Next']")
        next_button.click()
        print ('Going Next Page')
        time.sleep(3)

        # Make sure the page is reloaded with new links, then go next
        while True:
            # Grab all Reference Numbers from the navigation page
            new_navigation = BeautifulSoup(driver.page_source, 'html.parser')
            newnumbers = new_navigation.find_all(name='div', class_='formSectionHeader6_TEXT')
            newnumbers = list(map(lambda x: x.text, newnumbers))
            if newnumbers[0] == referencenumbers[0]:
                time.sleep(3)
            else:
                break
    except:
        break

# Stop Driver
driver.quit()

# Compile the results to Pandas
headers = ['Project Type', 'Reference Number', 'Project Name', 'Agency', 'Published Date', 'Procurement Category', 'Closed Date', 'Current Status', 'Qualification Criteria', 'Respondents', 'Awarded To', 'Awarded Value']
full_dataset = np.array(full_dataset)
dataset = pd.DataFrame(data = full_dataset, columns = headers)

# Print the dataset for visuals
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(dataset)

# Export to CSV
dataset.to_csv('data.csv')