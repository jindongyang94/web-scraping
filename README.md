# Web Scraping Project - with Automated Googlesheets Upload

```bash
.
├── Makefile
├── README.md
├── config
│   ├── config.yaml
│   └── google-credentials.json ("Not Available. Create your own if you want to export to Googlesheets")  
├── data
│   └── data.csv ("Not Available as file is too big. This is where you can store the data locally in this repo.")
├── notebooks
│   └── web_scraping.ipynb
└── scripts
    ├── gebiz_scraping.py
    └── googlesheets.py
```

The aim of this project is to use selenium to interact with websites who have require clicking and navigations that are hidden due to AJAX or Javascript.  
However, after the navigation is complete, we will continue the function with beautifulsoup4 to do the actual parsing of the website.

## Scrape website using given example

```bash
make scrape
```

## Upload using googlesheets.py

Simply follow instructions online to download credentials: <https://gspread.readthedocs.io/en/latest/oauth2.html>  
Run the script *googlesheets.py* afterwards.

## Deployment

This is script can be uploaded to an automation orchestration framework such as Airflow on Kubernetes to be run periodically.  
It is also possible to allow the automated upload of the csv to some user friendly source such as Google Sheets.  

**Note: if you want to witness the interaction so as to debug, comment out the headless option. However, to use the headless option properly, please uncomment out the display specifics in the next line.**
