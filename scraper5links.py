"""
 * @category : Functionality 
 * @author : Rishabh Gaur
 * @created date : May 30, 2024
 * @updated date : June 4, 2024
 * @company : Birbals Inc
 * @description : Provides 5 static scrapers to use in scrapermulitplelinksfromclassic.
"""

import time #lets us set delays for x amount of seconds
import requests #import requests module allows us to make HTTP GET requests to desired site
from bs4 import BeautifulSoup #Beautiful soup enables parsing of HTML returned by requests
import scrapy #import scrapy module allows us to call web-scraping from the terminal and sets up response/request structure for this functionality
from scrapy import signals #signals enables us to run spider_closed when the scraper completes
import re # re lets us parse inconsistent strings with consistent patterns
from establish_db_connection import mydb #mydb lets us access our database created in establish_db_connection
from sql_query_builder import create_table, add_nonduplicate_row, add_nonduplicate_row_relational, move_columns_to_end, get_first_n_rows #need these for dynamic SQL queris; functions' precise descriptions can be found in sql_query_builder
from establish_log import req_get_log, req_get_log_selenium #see establish log for detailed descriptions


from selenium import webdriver #selenium is used for automating web browser interaction, useful for web scraping and testing.
from selenium.webdriver.common.by import By #provides a way to locate elements within a document, such as by ID, name, class name, etc., for browser automation.
from selenium.webdriver.chrome.service import Service as ChromeService #manages the starting and stopping of the ChromeDriver, which allows selenium to control Chrome.
from webdriver_manager.chrome import ChromeDriverManager #automatically manages the download and setup of the ChromeDriver for selenium.

#BARONS AUCTIONS (id 1)
def baronsauctionsscrape1(responsetext, table_name, site_source_id):
    soup = BeautifulSoup(responsetext, 'html.parser') #parse the response text into a BeautifulSoup object
    latest_entries_div = soup.find('div', class_='latest-entries') #find the div containing the latest entries
    if latest_entries_div:
        links = latest_entries_div.find_all('a', href=True) #find all anchor tags with href attribute in the latest entries div
        hrefs = [a['href'] for a in links] #extract href attributes from the anchor tags
        #get the full URL for the latest auction cars page
        auction_cars_response = req_get_log(requests.compat.urljoin("http://www.barons-auctions.com", hrefs[-1]), site_source_id)
        if auction_cars_response.status_code == 200:
            auction_cars_soup = BeautifulSoup(auction_cars_response.content, 'html.parser') #parse the auction cars response into a BeautifulSoup object
            cars_div = auction_cars_soup.find('div', class_='cars') #find the div containing car details
            if cars_div:
                cars_hrefs = cars_div.find_all('a', href=True) #find all anchor tags with href attribute in the cars div
                
                all_car_urls = set() #create a set to store unique car URLs
                for a in cars_hrefs:
                    #construct the full URL for each car and add to the set if not already present
                    full_url = requests.compat.urljoin("http://www.barons-auctions.com/content/auctions.php", a['href'])
                    if full_url not in all_car_urls:
                        all_car_urls.add(full_url)
                
                #iterate over each unique car URL
                for url in all_car_urls:
                    print(url)
                    car_response = req_get_log(url, site_source_id) #make a GET request to fetch car details
                    car_soup = BeautifulSoup(car_response.content, 'html.parser') #parse the car response into a BeautifulSoup object
                    
                    cardetails_div = car_soup.find('div', class_='cardetails') #find the div containing car details
                    #getting car name
                    car_title_span = car_soup.find('span', class_='cartitle') #find the span containing the car title
                    car_title_text = car_title_span.get_text(strip=True) #extract text from the car title span
                    #clean the car title to remove "Lot" and the lot number
                    car_name = re.sub(r'^Lot \d+: ', '', car_title_text)

                    columns = ['Name'] #initialize columns list with "Name"
                    values = [car_name] #initialize values list with the car name

                    divs = cardetails_div.find_all('div') #find all divs within the car details div
                    for i, div in enumerate(divs):
                        text = div.get_text(strip=True) #extract text from each div
                        if text.endswith(':'):
                            text = text[:-1] #remove the last character if it's a colon
                        if i % 2 == 0:
                            columns.append(text) #append text to columns if it's an even index
                        else:
                            values.append(text) #append text to values if it's an odd index
                    
                    print("Columns:", columns)
                    
                    print("Values:", values)
                    col_type_list = ['VARCHAR(255)' for _ in range(len(columns))] #define column types as VARCHAR(255)
                    #add the non-duplicate row to the relational table
                    add_nonduplicate_row_relational(table_name, columns, col_type_list, values, site_source_id)
            else:
                print("no div with class 'cars' found on barons auction cars site.") #print an error if the cars div is not found
        else:
            print(f"failed to retrieve the auction cars site. Status code: {auction_cars_response.status_code}") #print an error if the auction cars page retrieval fails
    else:
        raise ValueError("unable to find latest entries for barons auction") #raise an error if the latest entries div is not found

#BARRETT JACKSON (id 2)
def scrape2_get_car_links(base_url, docket_links, site_source_id):
    car_links = [] #initialize an empty list to store car links
    for url in docket_links:
        response = req_get_log_selenium(url, 5, site_source_id) #make a selenium request to get the docket page
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser') #parse the response content into a BeautifulSoup object
            a_tags = soup.find_all('a', class_='pull-left', href=True) #find all 'a' tags with class 'pull-left' and href attribute
            for a_tag in a_tags:
                href = a_tag['href'] #extract href attribute from 'a' tag
                full_url = requests.compat.urljoin(base_url, href) #construct full URL using the base URL and href
                car_links.append(full_url) #append the full URL to the car links list
        else:
            print(f"failed to retrieve the docket page: {url}") #print an error message if the request fails
    return car_links #return the list of car links

def scrape2extract_car_details(car_links, table_name, site_source_id):
    for url in car_links:
        status_words = ['Name'] #initialize a list to store status words
        id_words = [] #initialize a list to store id words
        response = req_get_log_selenium(url, 0.5, site_source_id) #make a selenium request to get the car details page
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser') #parse the response content into a BeautifulSoup object
            
            text_content = soup.find('strong').get_text() #extract text from the 'strong' tag
            parts = text_content.split('\xa0') #split the text using unicode representation of &nbsp;
            if len(parts) > 1:
                name_after_nbsp = parts[1].strip() #strip any whitespace from the text after &nbsp;
                id_words.append(name_after_nbsp) #append the cleaned name to id_words
            #find all 'td' tags with a specific style attribute
            td_tags = soup.find_all('td', style=lambda value: value and 'width: 40%' in value)
            for td_tag in td_tags:
                status_word = td_tag.get_text(strip=True) #extract and strip text from 'td' tag
                #find the next sibling 'td' tag with a specific style attribute
                next_td = td_tag.find_next_sibling('td', style=lambda value: value and 'width: 60%' in value)
                if next_td:
                    span_tag = next_td.find('span') #find 'span' tag within the next 'td' tag
                    if span_tag and span_tag.get('id') == status_word:
                        id_word = span_tag.get_text(strip=True) #extract and strip text from 'span' tag
                        status_words.append(status_word) #append status word to status_words list
                        id_words.append(id_word) #append id word to id_words list
        col_type_list = ['VARCHAR(255)' for _ in range(len(status_words))] #create a list of column types as VARCHAR(255)
        add_nonduplicate_row_relational(table_name, status_words, col_type_list, id_words, site_source_id) #add the non-duplicate row to the relational table

def barrettjacksonscrape2(response, table_name, site_source_id):
    initial_site_soup = BeautifulSoup(response, 'html.parser') #parse the initial response into a BeautifulSoup object
    
    #find all 'a' tags that contain 'Collector Car Docket' in their text
    collector_car_docket_links = [] #initialize an empty list to store collector car docket links
    for a_tag in initial_site_soup.find_all('a', string=lambda text: text and 'Collector Car Docket' in text):
        href = a_tag.get('href') #extract href attribute from 'a' tag
        if href:
            full_url = requests.compat.urljoin('https://barrett-jackson.com', href) #construct full URL using base URL and href
            collector_car_docket_links.append(full_url) #append the full URL to the docket links list
    
    #get car links from the docket pages
    car_links = scrape2_get_car_links('https://barrett-jackson.com', collector_car_docket_links, site_source_id)
    scrape2extract_car_details(car_links, table_name, site_source_id) #extract and store car details from the car links
    
#427 GARAGE (id 3)
def garage427scrape3(table_name, site_source_id):
    urls = []
    
    r = req_get_log("https://www.427garage.com/", site_source_id) #making HTTP GET request 
    soup = BeautifulSoup(r.text, 'html.parser') #converting request's output into HTML to parse
    links = soup.find_all('a', class_='planet-inventory-item') #finds HTML portions that contain links to for-sale cars in the outputted HTML

    hrefs = [link.get('href') for link in links] #gathers all sub-domains (ie. /z/x/y) in the links list
    #creating all for-sale cars links to scrape and adding them to urls
    for href in hrefs: 
        urls.append("https://www.427garage.com" + href)
    
    #create scrapy request to parse for all the urls in our list
    for url in urls:
        response = req_get_log(url, site_source_id)
        car_soup = BeautifulSoup(response.content, 'html.parser')

        #extracting the name
        name_tag = car_soup.find('title').get_text()
        name = re.sub(r'\s*\|\s*427 Garage$', '', name_tag) #cleans the names from the HTML
        db_dict = {"Name": name} #creates dictionary with format column name : data

        #extracting elements containing car data
        elements = car_soup.find_all('div', class_='col-xs-12 col-sm-6 col-md-12')
        for element in elements:
            planet_spec_title = element.find('dt', class_='planet-spec-title')
            lead_planet_value = element.find('dd', class_='lead planet-spec-value')

            if planet_spec_title:
                planet_spec_title = planet_spec_title.get_text().strip()
            else:
                continue

            if lead_planet_value:
                lead_planet_value = lead_planet_value.get_text().strip()
            else:
                lead_planet_value = '-'

            if 'price' in planet_spec_title.lower(): # if the column is Price
                planet_spec_title = 'Price'
                lead_planet_value = car_soup.find('dd', class_='lead planet-price').get_text().strip()

            db_dict[planet_spec_title] = lead_planet_value # adding column name and value for that row to the dictionary
            # print(planet_spec_title + ", " + lead_planet_value) # print column and value
        
        col_type_list = ['VARCHAR(255)' for _ in range(len(db_dict.keys()))] # all of our columns (db_dict.keys() is list of columns) are strings of VARCHAR(255)
        add_nonduplicate_row_relational(table_name, list(db_dict.keys()), col_type_list, list(db_dict.values()), site_source_id) # adding non-duplicate rows using column names and values stored in db_dict; Vin is the unique identifier

#BAVARIAN MOTORSPORTS (id 4)
def bavarianscrape4(table_name, site_source_id):
    #make a request to the Bavarian Motorsports inventory page
    response = req_get_log('https://www.bavarianmotorsport.com/inventory', site_source_id) 
    soup = BeautifulSoup(response.content, 'html.parser') #parse the response content into a BeautifulSoup object
    urls = [] #initialize an empty list to store car URLs
    
    #find all h4 tags with the class 'result-item-title text-center'
    for h4_tag in soup.find_all('h4', class_='result-item-title text-center'):
        a_tag = h4_tag.find('a', href=True) #find the 'a' tag within the h4 tag
        if a_tag:
            full_url = 'https://www.bavarianmotorsport.com' + a_tag['href'] #construct the full URL for the car
            urls.append(full_url) #append the full URL to the list of car URLs
    
    #configure Chrome options for Selenium
    options = webdriver.ChromeOptions()
    options.add_argument('--headless') #run Chrome in headless mode
    options.add_argument('--disable-gpu') #disable GPU acceleration
    
    #initialize the Chrome driver
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    #iterate over each car URL
    for url in urls:
        response = req_get_log(url, site_source_id) #make a request to the car details page
        car_soup = BeautifulSoup(response.content, 'html.parser') #parse the response content into a BeautifulSoup object
        cols = ['Name', 'Price'] #initialize columns list with 'Name' and 'Price'
        name = car_soup.find('div', class_='ws_title').find('h3').get_text(strip=True) #extract car name
        price = car_soup.find('div', class_='ws_price').get_text(strip=True) #extract car price
        vals = [name, price] #initialize values list with name and price
        
        #find all divs with specific classes within the 'car-detail' div
        details = car_soup.find('div', class_='car-detail').find('div', class_='row').find_all('div', class_='col-md-3 col-sm-4 col-xs-12')
        
        #iterate over each detail div
        for detail in details:
            detail_col = detail.find('strong').get_text(strip=True) #extract text from 'strong' tag
            detail_col = detail_col[:-1] #remove the last character if it's a colon
            if detail_col == 'Stock #':
                detail_col = 'Stock' #rename 'Stock #' to 'Stock'
            cols.append(detail_col) #append detail column name to columns list
            if detail_col == 'VIN':
                driver.get(url) #open the car details page in the browser
                span_element = driver.find_element(By.CSS_SELECTOR, 'span[onclick]') #find the span element with 'onclick' attribute
                span_element.click() #click the span element to reveal VIN
                detail_val = span_element.text.strip() #extract and strip the VIN text
            else:
                detail_val = detail.find('p').get_text(strip=True) #extract text from 'p' tag
                detail_val = detail_val.split(":", 1)[1].strip() #split and strip the text after the colon
                detail_val = detail_val.replace('\t', '') #remove any tab characters
                
            vals.append(detail_val) #append detail value to values list
        
        col_type_list = ['VARCHAR(255)' for _ in range(len(cols))] #define column types as VARCHAR(255)
        add_nonduplicate_row_relational(table_name, cols, col_type_list, vals, site_source_id) #add the non-duplicate row to the table

#GR Auto parse 5
def grautoscrape5(table_name, site_source_id):
    url = 'https://www.grautogallery.com/vehicles/current'
    response = req_get_log(url, site_source_id) #make a request to the current vehicles page
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser') #parse the response content into a BeautifulSoup object
        
        #find all <a> tags with itemtype "https://schema.org/Car"
        car_links = soup.find_all('a', itemtype="https://schema.org/Car")
        
        urls = []
        for link in car_links:
            url = link.get('href')
            if url:
                urls.append('https://www.grautogallery.com' + url) #construct full URLs for the car detail pages
        
        for url in urls:
            car_response = req_get_log(url, site_source_id) #make a request to the car detail page
            car_soup = BeautifulSoup(car_response.content, 'html.parser') #parse the car detail page content
            
            #extract the car name from the h1 tag
            h1_tag = car_soup.find('h1')
            year = h1_tag.find('span', class_='year').get_text(strip=True)
            make = h1_tag.find('span', class_='make').get_text(strip=True)
            model = h1_tag.find('span', class_='model').get_text(strip=True)
            sub_model = h1_tag.find('span', class_='sub_model').get_text(strip=True)
            name = f"{year} {make} {model} {sub_model}".strip()
            
            #extract the car price from the vehicle-price div
            price = car_soup.find('div', class_='vehicle-price').find('h2').get_text(strip=True)

            cols = ['Name', 'Price']
            vals = [name, price]

            #extract specifications from ag-spec-summary divs
            spec_summary_div = car_soup.findAll('div', class_='ag-spec-summary')
            for spec in spec_summary_div:
                cols.append(spec.find('dt').get_text(strip=True))
                vals.append(spec.find('dd', class_='lead').get_text(strip=True))
                        
            print("Cols:", cols)
            print("Vals:", vals)

            col_type_list = ['VARCHAR(255)' for _ in range(len(cols))]
            #add the non-duplicate row to the relational table
            add_nonduplicate_row_relational(table_name, cols, col_type_list, vals, site_source_id)
    else:
        raise Exception(f"failed to get data from {url}, status code: {response.status_code}") #raise an exception if the request fails

#Cassandra parse 5 (BUGS IN THIS SCRAPER RELATED TO SELENIUM, IGNORE)
# def cassscrape5(table_name, site_source_id):
#     options = webdriver.ChromeOptions()
#     options.add_argument('--headless')
#     options.add_argument('--disable-gpu')
#     driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
#     driver.get("https://www.cassandramotorsports.com/inventory/")
#     driver.implicitly_wait(1)  

#     soup = BeautifulSoup(driver.page_source, 'html.parser')
#     elements = soup.find_all("a", class_="border-btn")
    
#     urls = []
#     for element in elements:
#         onclick_value = element.get('onclick')
#         if onclick_value:
#             start = onclick_value.find("getDetailed('") + len("getDetailed('")
#             end = onclick_value.find("')", start)
#             url_part = onclick_value[start:end]
#             urls.append("https://www.cassandramotorsports.com/" + url_part)
    
#     driver.quit()

#     for url in urls:
#         response = req_get_log_selenium(url, 10, site_source_id)
#         car_soup = BeautifulSoup(response.content, 'html.parser')
#         name = car_soup.find('h1', class_="mb-0 font-goodtimes").get_text(strip=True)
#         price = car_soup.find('h6', class_="mb-4 mb-lg-2 font-goodtimes").get_text(strip=True)
#         price = price.split("$", 1)[1].strip()
#         cols = ['Name', 'Price']
#         vals = [name, price]

#         details = car_soup.find_all('li', class_='mb-3')
        
#         for detail in details:
#             try:
#                 detail_col = detail.find('div', class_='divfirst').get_text(strip=True)
#             except:
#                 print("UUU ", details)
#                 raise ValueError("eee")
            
#             if (detail_col == 'Stock #'):
#                 detail_col = 'Stock'
#             detail_val = detail.find_all('div')[1].get_text(strip=True)
#             if detail_col not in cols:
#                 cols.append(detail_col)
#             if detail_val not in vals:
#                 vals.append(detail_val)
        
#         print("TOM", cols)
#         print("JERRY", vals)
#         col_type_list = ['VARCHAR(255)' for _ in range(len(cols))]
#         add_nonduplicate_row_relational(table_name, cols, col_type_list, vals, site_source_id)
