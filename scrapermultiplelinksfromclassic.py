"""
 * @category : Functionality 
 * @author : Rishabh Gaur
 * @created date : May 30, 2024
 * @updated date : June 4, 2024
 * @company : Birbals Inc
 * @description : Collects car data from the first x links of the Sites table using static scrapers. Ensures each scraper is only called when necessary.
"""

#STEPS ON RUNNING THE WEB SCRAPERS
"""
Run scraperclassic BEFORE running this file. Sites table must be full before running.
All flags (scraping_status) must be 0 in Sites table in order for all the scrapers to run.
ENSURE YOU HAVE DOWNLOADED chromedriver and chrome version for your device from here: https://googlechromelabs.github.io/chrome-for-testing/
pip install ALL NECESSARY LIBRARIES
1) Open your computer's terminal. Copy and paste the path to your chromedriver and run. The terminal should say 'ChromeDriver was started successfully.' 
2) Open 'New Terminal' 
3) Type scrapy runspider, then drag the path to terminal. Click enter.
"""

import time
import requests #import requests module allows us to make HTTP GET requests to desired site
from bs4 import BeautifulSoup #Beautiful soup enables parsing of HTML returned by requests
import scrapy #import scrapy module allows us to call web-scraping from the terminal and sets up response/request structure for this functionality
from scrapy import signals #signals enables us to run spider_closed when the scraper completes
import re # re lets us parse inconsistent strings with consistent patterns
from establish_db_connection import mydb #mydb lets us access our database created in establish_db_connection
from sql_query_builder import create_table, add_nonduplicate_row, move_columns_to_end, get_first_n_rows #need these for dynamic SQL queris; functions' precise descriptions can be found in sql_query_builder

from establish_log import add_log, flag_toggle,create_log_table #see establish_log for functions' details 
from datetime import datetime, timezone #datetime and timezone let us report time at which some event happens
from scraper5links import baronsauctionsscrape1, barrettjacksonscrape2, garage427scrape3, bavarianscrape4,grautoscrape5 #see scraper5links for details on these functions
class scrapermultiplelinksfromclassicSpider(scrapy.Spider): #necessary formatting to run scraper through terminal (see above)
    name = "scrapermultiplelinksfromclassic"

    custom_settings = {
        'HTTPERROR_ALLOWED_CODES': [200, 403, 404],  #ensures that Scrapy does not reject possible failure codes so that we can handle them explicitly in parse (see #handles non-200 responses)
    }

    def __init__(self, *args, **kwargs):
        self.extract_table_name = 'Sites' #table we are extracting links from
        self.extract_table_col = 'Link' #column which contains links
        self.exclusive_upper_limit_on_number_of_links_to_scrape = 6 # we want to collect exclusive_upper_limit_on_number_of_links_to_scrape-1 links
        self.current_id = 1 #id in the Sites table of current link we are scraping, will increment later in parse
        super(scrapermultiplelinksfromclassicSpider, self).__init__(*args, **kwargs) #necessary formatting
        self.links = get_first_n_rows(self.extract_table_name, self.extract_table_col, self.exclusive_upper_limit_on_number_of_links_to_scrape - 1) #collects  exclusive_upper_limit_on_number_of_links_to_scrape-1 links
        self.flags = get_first_n_rows(self.extract_table_name, 'scraping_status', self.exclusive_upper_limit_on_number_of_links_to_scrape - 1) #collects the flags for each of those links
        self.allowed_domains = list(set(re.search(r'https?://(?:www\.)?([^/]+)', link).group(1) for link in self.links if re.search(r'https?://(?:www\.)?([^/]+)', link))) #simplifies domain names so Scrapy can interpret allowed_domains
        self.insert_table_name = 'Cars' #table we are inserting data into
        create_table(self.insert_table_name, ['Name', 'site_source_id'], ['VARCHAR(255)', 'MEDIUMINT']) #creates table with only two columns: Name (self-explanatory, names of each car) and site_source_id, which notes which id from Sites the Car came from
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'}

    def start_requests(self):#necesarry formatting 
        if self.current_id < len(self.links): #ensures we only scrape the number of links we want to scrape specified by exclusive_upper_limit_on_number_of_links_to_scrape
            url = self.links[self.current_id-1]
            created_time = datetime.now(timezone.utc) # marks when request is made
            print(f"\n {url} \n " )
            yield scrapy.Request(url=url, headers=self.headers, meta={'created_time': created_time, 'url': url}, callback=self.parse) #meta contains data that will be passed and accessed by parse function
        else:
            raise ValueError("current id is greater than or equal to our exclusive_upper_limit_on_number_of_links_to_scrape")
    
    def parse(self, response): 
        extract_table_name = self.extract_table_name 
        updated_time = datetime.now(timezone.utc) #marks time that we get response
        created_time = response.meta['created_time'] #pulling data provided by start_requests

        #formatting request's text to place in log table
        request = response.request 
        request_text = '{}\n{}\n\n{}'.format(
        f'{request.method} {request.url} HTTP/1.1',
        '\n'.join(f'{k}: {v}' for k, v in request.headers.items()),
        request.body or ''
    )

        add_log(request_text, response.body, response.status, created_time, updated_time, self.current_id)  #adds log row to log table
        
        #handles non-200 responses
        if response.status != 200:
            print(f" \n {response.meta['url']} raised 403 error. Unable to access link. \n")
            return #ends program running
        
        #handles 200 responses, progresses the program
        if response.status == 200:
            print(f"\n {response.meta['url']} raised 200 status. \n")
        
        #if we run less than 5 scrapers, this ends the program if current_id is greater than exclusive_upper_limit_on_number_of_links_to_scrape
        if self.current_id >= len(self.flags):
            return

        #handle scraping for each url (see first block for example functionality)
        if(self.flags[self.current_id] == 0): #checks that flag is 0, meaning the link has not already been visited
            print(response.meta['url'], "has flag 0. starting scrape.") #print message
            if (response.meta['url'] == "http://barons-auctions.com"): #checks that the link provided by start_requests is barons_auctions 
                print('running parse1')
                baronsauctionsscrape1(response.text, self.insert_table_name, self.current_id) #runs scraper for barons auctions
                flag_toggle(extract_table_name, response.meta['url'], 'Link') #toggles flag to 1 in extract_table_name table
                mydb.commit() #commits changes
        if(self.flags[self.current_id] == 0):
            print(response.meta['url'], "has flag 0. starting scrape.")
            if (response.meta['url'] == "http://barrett-jackson.com"):
                print('running parse2')
                barrettjacksonscrape2(response.text, self.insert_table_name, self.current_id)
                flag_toggle(extract_table_name, response.meta['url'], 'Link')
                mydb.commit()
        if(self.flags[self.current_id] == 0):
            print(response.meta['url'], "has flag 0. starting scrape.")
            if (response.meta['url'] == "https://427garage.com/"):
                print('running parse3')
                garage427scrape3(self.insert_table_name, self.current_id)
                flag_toggle(extract_table_name, response.meta['url'], 'Link')
                mydb.commit()

        if(self.flags[self.current_id] == 0):
            print(response.meta['url'], "has flag 0. starting scrape.")
            if (response.meta['url'] == "https://bavarianmotorsport.com/"):
                print('running parse4')
                bavarianscrape4(self.insert_table_name, self.current_id)
                flag_toggle(extract_table_name, response.meta['url'], 'Link')
                mydb.commit()
        if(self.flags[self.current_id] == 0):
            print(response.meta['url'], "has flag 0. starting scrape.")
            if (response.meta['url'] == "https://grautogallery.com/"):
                print('running parse5')
                grautoscrape5(self.insert_table_name, self.current_id)  
                flag_toggle(extract_table_name, response.meta['url'], 'Link')
                mydb.commit()
            
            
            self.current_id += 1 #increments current id by 1
            if self.current_id < self.exclusive_upper_limit_on_number_of_links_to_scrape: #ensures we are below limit of number of links we want to scrape
                next_url = self.links[self.current_id - 1] #gathers next url
                created_time = datetime.now(timezone.utc) #marks time of request to next url 
                yield scrapy.Request(url=next_url, headers=self.headers, meta={'created_time': created_time, 'url': next_url}, callback=self.parse) #calls request on next url