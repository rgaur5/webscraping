

import time
import requests #import requests module allows us to make HTTP GET requests to desired site
from bs4 import BeautifulSoup #Beautiful soup enables parsing of HTML returned by requests
import scrapy #import scrapy module allows us to call web-scraping from the terminal and sets up response/request structure for this functionality
from scrapy import signals #signals enables us to run spider_closed when the scraper completes
import re # re lets us parse inconsistent strings with consistent patterns
from establish_db_connection import mydb #mydb lets us access our database created in establish_db_connection
from sql_query_builder import create_table, add_nonduplicate_row, move_columns_to_end, get_first_n_rows #need these for dynamic SQL queris; functions' precise descriptions can be found in sql_query_builder

from establish_log import add_log, flag_toggle
from datetime import datetime, timezone
from scraper5links import baronsauctionsscrape1, barrettjacksonscrape2
class scrapermultiplelinksfromclassicSpider(scrapy.Spider): #necessary formatting to run scraper through terminal (see above)
    name = "scrapermultiplelinksfromclassic"

    custom_settings = {
        'HTTPERROR_ALLOWED_CODES': [200, 403, 404],  # Add other status codes you want to handle
    }

    def __init__(self, *args, **kwargs):
        self.extract_table_name = 'Sites'
        self.extract_table_col = 'Link'
        self.exclusive_upper_limit_on_number_of_links_to_scrape = 6
        self.current_id = 1
        super(scrapermultiplelinksfromclassicSpider, self).__init__(*args, **kwargs)
        self.links = get_first_n_rows(self.extract_table_name, self.extract_table_col, self.exclusive_upper_limit_on_number_of_links_to_scrape - 1)
        self.allowed_domains = list(set(re.search(r'https?://(?:www\.)?([^/]+)', link).group(1) for link in self.links if re.search(r'https?://(?:www\.)?([^/]+)', link)))
        print(f"\n EEE {self.allowed_domains} \n " )
        self.insert_table_name = 'Cars'
        create_table(self.insert_table_name, ['Name'], ['VARCHAR(255)']) #creates table with only one column: Name
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'}

    def start_requests(self):
        if self.current_id < len(self.links):
            url = self.links[self.current_id-1]
            created_time = datetime.now(timezone.utc)
            print(f"\n {url} \n " )
            yield scrapy.Request(url=url, headers=self.headers, meta={'created_time': created_time, 'url': url}, callback=self.parse)
        else:
            raise ValueError("current id is greater than or equal to our exclusive_upper_limit_on_number_of_links_to_scrape")
    
    def parse(self, response): 
        extract_table_name = self.extract_table_name
        updated_time = datetime.now(timezone.utc)
        created_time = response.meta['created_time']
        add_log(response.request.url, response.body, response.status, created_time, updated_time) 
        
        if response.status != 200:
            print(f" \n {response.meta['url']} raised 403 error. Unable to access link. \n")

        if response.status == 200:
            print(f"\n {response.meta['url']} raised 200 status. \n")

            #handle scraping for each url
            # if (response.meta['url'] == "http://barons-auctions.com"):
            #     print('running parse1')
            #     baronsauctionsscrape1(response.text, self.insert_table_name)

            if (response.meta['url'] == "http://barrett-jackson.com"):
                print('running parse2')
                barrettjacksonscrape2(response.text, self.insert_table_name)
            




            flag_toggle(extract_table_name, response.meta['url'], 'Link')
            self.current_id += 1
            if self.current_id < self.exclusive_upper_limit_on_number_of_links_to_scrape:
                next_url = self.links[self.current_id - 1]
                created_time = datetime.now(timezone.utc)
                yield scrapy.Request(url=next_url, headers=self.headers, meta={'created_time': created_time, 'url': next_url}, callback=self.parse)