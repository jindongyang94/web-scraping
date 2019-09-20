# Side Scraping Project

.
├── README.md
├── data.csv
├── gebiz_scraping.py
├── web_scraping.ipynb
└── web_scraping.py

The aim of this project is to use selenium to interact with websites who have require clicking and navigations that are hidden due to AJAX or Javascript.  
However, after the navigation is complete, we will complement the scripting with beautifulsoup4 to do the actual parsing of the website.

This is script can be uploaded to an automation orchestration framework such as Airflow on Kubernetes to be run periodically.  
It is also possible to allow the automated upload of the csv to some user friendly source such as Google Sheets.

Note: if you want to witness the interaction so as to debug, comment out the headless option. However, to use the headless option properly, please uncomment out the display specifics in the next line.
