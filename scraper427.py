"""
 * @category : Functionality
 * @author : Rishabh Gaur
 * @created date : May 28, 2024
 * @updated date : May 28, 2024
 * @company : Birbals Inc
 * @description : Web Scraping for the cars on 427garage.com; collecting dynamic data about these vehicles in MYSQL database
"""

#STEPS ON RUNNING THE WEB SCRAPER
"""
1) Open 'New Terminal' 
2) Type scrapy runspider, then drag the web scraper you want to run to the terminal (to get path to the .py file). Click enter.
"""

import requests #import requests module allows us to make HTTP GET requests to desired site
from bs4 import BeautifulSoup #Beautiful soup enables parsing of HTML returned by requests
import scrapy #import scrapy module allows us to call web-scraping from the terminal and sets up response/request structure for this functionality
from scrapy import signals #signals enables us to run spider_closed when the scraper completes
import re # re lets us parse inconsistent strings with consistent patterns
from establish_db_connection import mydb #mydb lets us access our database created in establish_db_connection
from sql_query_builder import create_table, add_nonduplicate_row, move_columns_to_end#need these for dynamic SQL queris; functions' precise descriptions can be found in sql_query_builder

class scraper427Spider(scrapy.Spider): #necessary formatting to run scraper through terminal (see above)
    name = "scraper427" #string matches filename, necessary formatting to run scraper through terminal (see above)
    allowed_domains = ["427garage.com"] #427garage.com is the only link visited, necessary formatting to run scraper through terminal (see above)
    table_name = 'Cars'
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs): #method creates signal when scrapy scraping is done, allows spider_closed to run when scraping ends
        spider = super(scraper427Spider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self): #function gathers the links to all cars listed on 427garage.com
        urls = ["https://www.427garage.com/"] #list of urls that will eventually expand
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'} #header used to bypass robots.txt of 427garage.com
        
        r = requests.get(urls[0], headers=headers) #making HTTP GET request 
        soup = BeautifulSoup(r.text, 'html.parser') #converting request's output into HTML to parse
        links = soup.find_all('a', class_='planet-inventory-item') #finds HTML portions that contain links to for-sale cars in the outputted HTML
        hrefs = [link.get('href') for link in links] #gathers all sub-domains (ie. /z/x/y) in the links list
        
        #creating all for-sale cars links to scrape and adding them to urls
        for href in hrefs: 
            urls.append("https://www.427garage.com" + href)
        del urls[0] #removing https://www.427garage.com itself from urls
        
        #create scrapy request to parse for all the urls in our list
        for url in urls:
            yield scrapy.Request(url=url, headers=headers, callback=self.parse)
    
    def parse(self, response): #this function gathers all Car data and adds it to a table in a pre-defined database
        table_name = self.table_name
        create_table(table_name, ['Name'], ['VARCHAR(255)']) #creates table with only one column: Name

        name = (response.css('title::text').getall())[0] #gets all names from the HTML, uncleaned
        name = re.sub(r'\s*\|\s*427 Garage$', '', name) #cleans the names from the HTML
        
        db_dict = {"Name" : name} #creates dictionary with format column name : data

        elements = response.css('div.col-xs-12.col-sm-6.col-md-12') # body of HTML containing all remaining Car data
        for element in elements:
            planet_spec_title = element.css('dt.planet-spec-title::text').get() #will be our column names (ie. Vin, Price, etc.)
            lead_planet_value = element.css('dd.lead.planet-spec-value::text').get() #will be our values (ie. 67GHneF5, $55,000, etc.)

            if('Price'.lower() in planet_spec_title.lower()): #if the column is Price
                planet_spec_title = 'Price'
                lead_planet_value = response.css('dd.lead.planet-price::text').get() #look for the price section in the HTML   
            if lead_planet_value is None:
                lead_planet_value = '-' # '-' is used as an indication of no value provided

            db_dict[planet_spec_title] = lead_planet_value #adding column name and value for that row to the dictionary
            print(planet_spec_title + ", " +lead_planet_value ) #print column and value
        
        col_type_list = ['VARCHAR(255)' for _ in range(len(db_dict.keys()))] #all of our columns (db_dict.keys() is list of columns) are strings of VARCHAR(255)
        add_nonduplicate_row(table_name, list(db_dict.keys()), col_type_list, list(db_dict.values()), 'Vin') #adding non-duplicate rows using column names and values stored in db_dict; Vin is the unique identifier

    def spider_closed(self, spider): #function runs after all scraping ends; closes mydb connection and moves updated/created cols to end of table
        try:
            table_name = self.table_name
            # print("EEEEEEEE doing post-scraping tasks")
            # move_columns_to_end(f"{table_name}") #running move columns to end from sql_query_builder.py with class variable table_name
        except Exception as e:
            print(f"Error in spider_closed: {e}")
        finally:
            mydb.close()