

import time
import requests #import requests module allows us to make HTTP GET requests to desired site
from bs4 import BeautifulSoup #Beautiful soup enables parsing of HTML returned by requests
import scrapy #import scrapy module allows us to call web-scraping from the terminal and sets up response/request structure for this functionality
from scrapy import signals #signals enables us to run spider_closed when the scraper completes
import re # re lets us parse inconsistent strings with consistent patterns
from establish_db_connection import mydb #mydb lets us access our database created in establish_db_connection
from sql_query_builder import create_table, add_nonduplicate_row, move_columns_to_end, get_first_n_rows #need these for dynamic SQL queris; functions' precise descriptions can be found in sql_query_builder
from msvldtar import combined_check
from establish_log import add_log, flag_toggle
from datetime import datetime, timezone

class scrapermultiplelinksfromclassicSpider(scrapy.Spider): #necessary formatting to run scraper through terminal (see above)
    name = "scrapermultiplelinksfromclassic" 
    extract_table_name = 'Sites'
    extract_table_col = 'Link'
    number_of_links_to_scrape = 5
    links = get_first_n_rows(extract_table_name, extract_table_col, number_of_links_to_scrape) #getting list of urls from first 5 entries of 'Links' column in Sites table
    allowed_domains = list(set(re.search(r'[^.]+\.[^.]+$', re.search(r'https?://([^/]+)', link).group(1)).group(0) for link in links if re.search(r'https?://([^/]+)', link))) #remove the http:// or https:// prefix from each link and remove everything after .com
    insert_table_name = 'Cars'
    def start_requests(self):
        links = self.links
        allowed_domains = self.allowed_domains
        urls = links
        # print(f"EEEEEEEEEE {urls}")
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'} #header used to bypass robots.txt of the websites scraped
        print(f"EEE {allowed_domains}")
        print(f"GGG {urls}")
        for url in urls:
            created_time = datetime.now(timezone.utc)
            yield scrapy.Request(url=url, headers=headers, meta={'created_time': created_time, 'url' : url}, callback=self.parse)
    
    def parse(self, response): 
        extract_table_name = self.extract_table_name
        updated_time = datetime.now(timezone.utc)
        created_time = response.meta['created_time']
        add_log(response.request.url, response.body, response.status, created_time, updated_time)
        
        if response.status != 200:
            print(f"\n {response.status} \n")

        if response.status == 200:
            print(f"\n HELPPPP {response.meta['url']} \n ")
            flag_toggle(extract_table_name, response.meta['url'], 'Link')

    