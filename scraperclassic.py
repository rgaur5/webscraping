"""
 * @category : Functionality
 * @author : Rishabh Gaur
 * @created date : May 28, 2024
 * @updated date : June 4, 2024
 * @company : Birbals Inc
 * @description : Web Scraping for the car sellers provided by classic.com; collecting Source, Link, and Sellertype in a database via Scrapy
"""

#STEPS ON RUNNING THE WEB SCRAPER
"""
1) Open 'New Terminal' 
2) Type scrapy runspider, then drag the web scraper you want to run to the terminal (to get path to the .py file). Click enter.
"""

import scrapy #import scrapy module allows us to call web-scraping from the terminal and sets up response/request structure for this functionality
import requests #import requests module allows us to make HTTP GET requests to desired site
from bs4 import BeautifulSoup #Beautiful soup enables parsing of HTML returned by requests
from sql_query_builder import create_table, create_unique_identifier, add_nonduplicate_row, move_columns_to_end,add_flag_column #need these for dynamic SQL queris; functions' precise descriptions can be found in sql_query_builder
from scrapy import signals #signals enables us to run spider_closed when the scraper completes
from establish_db_connection import mydb #mydb lets us access our database created in establish_db_connection

class scraperclassicSpider(scrapy.Spider): #necessary formatting to run scraper through terminal (see above)
    name = 'scraperclassic' #string matches filename, necessary formatting to run scraper through terminal (see above)
    allowed_domains = ["classic.com"] #classic.com is the only link visited, necessary formatting to run scraper through terminal (see above)
    table_name = 'Sites' 

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs): #method creates signal when scrapy scraping is done, allows spider_closed to run when scraping ends
        spider = super(scraperclassicSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider


    def start_requests(self): #function creates Scrapy requests to all the pages in pagination of classic.com/data
        urls = ["https://www.classic.com/data"] #list of urls, expanded later in code
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'} #header used to bypass robots.txt of classics.com, not necessary but in case their robots.txt remains so does this
        response = requests.get(urls[0], headers=headers) #making HTTP GET request 
        soup = BeautifulSoup(response.content, 'html.parser') #converting request's output into HTML to parse
        input_element = soup.find('input', {'name': 'page_id', 'id': 'ScrollToTopTable1'}) #finds an input element with name page_id and id ScrollToTopTable1

        max_page = int(input_element['max']) #max is an element in the HTML that notes how many pages exist for classic.com/data (ie. pages 1-12 currently)
        
        #for loop creates url for each page of the pagination using max_page as an upper bound
        for i in range(max_page):
            complete_url = "https://www.classic.com/data?page=" + f"{i+1}" #i+1 because the loop begins at 0
            urls.append(complete_url)
            print(complete_url)

        del urls[0] #removes https://www.classic.com/data, we will only visit the pages in the pagination
        urls.sort(key=lambda url: int(url.split('=')[-1]))  #ensure URLs are sorted 1-12,, NOT FUNCTIONAL in making the links appear in consistent order

        #create scrapy request to parse for all the urls in our list
        for url in urls:
            print("fff", url)
            yield scrapy.Request(url=url, headers=headers, callback=self.parse)


    def parse(self, response): #this function initializes the table Sites, gathers data, and inserts that data in the table for each pagination

        #INITIALIZING TABLE (ASSUMING DB EXISTS)
        table_name = self.table_name
        
        column_names = ['Source', 'Link', 'Sellertype'] #list of column names
        column_types = ['VARCHAR(255)', 'VARCHAR(255)', 'VARCHAR(255)'] #all column names are VARCHAR(255) in SQL, use this list in sql queries later
        create_table(table_name, column_names, column_types) #creates table with specified columns named Sites

        create_unique_identifier(table_name, 'Link') #the value in the link column for any particular row is used to differentiate one entry from another

        #GATHERING DATA IN 3 SAME-LENGTH LISTS
        link_list = []
        source_list = []
        type_list = []
        
        sourceandlink = response.css('a.typography-body1[rel="noopener"]') #this section of HTMl contains both the source and link for each respective seller

        #gathering and appending all the links and sources from each body of HTML gathered in sourceandlink
        for sl in sourceandlink:
            link = sl.attrib.get('href')
            source = sl.css('span::text').get()
            link_list.append(link)
            source_list.append(source)

        types = response.css('span.typography-body2.typography-primary-color.typography-sm-.typography-md-.typography-lg-') # this list gathers all HTMl spans containing the type of seller (Dealer or Auction)
        #getting type for each seller and appending to type_list
        for t in types:
            typee = t.css('::text').get() #need to use ::text and get() because the types list above contains HTML, not types themselves
            type_list.append(typee) 
            
        #both link_list and source_list double count links and sources; we divide the list in half and only retain first part to make sure no duplicates exist
        link_list = link_list[:(len(link_list) // 2)]
        source_list = source_list[:(len(source_list) // 2)]
        
        type_list = type_list[6:len(type_list)-2]#the HTML bodies we captured for types above contain unnecessary data before index 6 and in the last 3 indices
        

        #specific entries to always appear first
        predefined_entries = [
            ('Barons', 'http://barons-auctions.com', 'Auction'),
            ('Barrett-Jackson', 'http://barrett-jackson.com', 'Auction'),
            ('427 Garage', 'https://427garage.com/', 'Dealer'),
            ('Bavarian Motorsport', 'https://bavarianmotorsport.com/', 'Dealer'),
            ('GR Auto Gallery', 'https://grautogallery.com/', 'Dealer')
        ]

        combined_list = sorted(zip(source_list, link_list, type_list))#ensure same order of entries every time file is run, NOT FUNCTIONAL
        
        #filter out predefined entries from combined_list to avoid duplication
        filtered_combined_list = [
            (source, link, typee) for source, link, typee in combined_list
            if (source, link, typee) not in predefined_entries
        ]

        combined_list = predefined_entries + filtered_combined_list #add predefined entries to the beginning

        #INSERTING DATA
        if len(link_list) == len(source_list) == len(type_list):
            for source, link, typee in combined_list:
                if ('id' in column_names):
                    column_names.remove('created_date')
                    column_names.remove('updated_date')
                    column_names.remove('id')
                add_nonduplicate_row(table_name, column_names, column_types, [source, link, typee]) #add non-unique non-duplicate rows for each seller
    
    def spider_closed(self, spider): #function runs after all scraping ends; closes mydb connection and moves updated/created cols to end of table
        try:
            table_name = self.table_name
            move_columns_to_end(f"{table_name}") #running move columns to end from sql_query_builder.py with class variable table_name
            add_flag_column(f"{table_name}",'scraping_status')
        except Exception as e:
            print(f"Error in spider_closed: {e}")
        finally:
            mydb.close()