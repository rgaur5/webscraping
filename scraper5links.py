import time
import requests #import requests module allows us to make HTTP GET requests to desired site
from bs4 import BeautifulSoup #Beautiful soup enables parsing of HTML returned by requests
import scrapy #import scrapy module allows us to call web-scraping from the terminal and sets up response/request structure for this functionality
from scrapy import signals #signals enables us to run spider_closed when the scraper completes
import re # re lets us parse inconsistent strings with consistent patterns
from establish_db_connection import mydb #mydb lets us access our database created in establish_db_connection
from sql_query_builder import create_table, add_nonduplicate_row, move_columns_to_end, get_first_n_rows #need these for dynamic SQL queris; functions' precise descriptions can be found in sql_query_builder
from establish_log import req_get_log, req_get_log_selenium

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
#BARONS AUCTIONS (id 1)
def baronsauctionsscrape1(responsetext, table_name):
    soup = BeautifulSoup(responsetext, 'html.parser')
    latest_entries_div = soup.find('div', class_='latest-entries')
    if latest_entries_div:
        links = latest_entries_div.find_all('a', href=True)
        hrefs = [a['href'] for a in links]
        auction_cars_response = req_get_log(requests.compat.urljoin("http://www.barons-auctions.com", hrefs[-1]))
        if auction_cars_response.status_code == 200:
            auction_cars_soup = BeautifulSoup(auction_cars_response.content, 'html.parser')
            cars_div = auction_cars_soup.find('div', class_='cars')
            if cars_div:
                cars_hrefs = cars_div.find_all('a', href=True)
                
                all_car_urls = set()
                for a in cars_hrefs:
                    full_url = requests.compat.urljoin("http://www.barons-auctions.com/content/auctions.php", a['href'])
                    if full_url not in all_car_urls:
                        all_car_urls.add(full_url)
                

                for url in all_car_urls:
                    print(url)
                    car_response = req_get_log(url)
                    car_soup = BeautifulSoup(car_response.content, 'html.parser')
                    # print(car_response.content)
                    cardetails_div = car_soup.find('div', class_='cardetails')
                    # print(f"JJJ {cardetails_div}")
                    
                    #getting car name
                    car_title_span = car_soup.find('span', class_='cartitle')
                    car_title_text = car_title_span.get_text(strip=True)
                    car_name = re.sub(r'^Lot \d+: ', '', car_title_text)

                    columns = ['Name']
                    values = [car_name]

                    divs = cardetails_div.find_all('div')
                    for i, div in enumerate(divs):
                        text = div.get_text(strip=True)
                        if text.endswith(':'):
                            text = text[:-1]  #remove the last character if it's a colon
                        if i % 2 == 0:
                            columns.append(text)
                        else:
                            values.append(text)
                    
                    print("Columns:", columns)
                    
                    print("Values:", values)
                    col_type_list = ['VARCHAR(255)' for _ in range(len(columns))]
                    add_nonduplicate_row(table_name,columns,col_type_list,values,"Registration")
            else:
                print("no div with class 'cars' found on barons auction cars site.")
        else:
            print(f"failed to retrieve the auction cars site. Status code: {auction_cars_response.status_code}")
    else:
        raise ValueError("unable to find latest entries for barons auction")

#BARRETT JACKSON (id 2)
def scrape2_get_car_links(base_url, docket_links):
    car_links = []
    for url in docket_links:
        
        response = req_get_log_selenium(url, 5)
        if response.status_code == 200:
            # print("200 status code received")
            soup = BeautifulSoup(response.content, 'html.parser')
            # Print the entire HTML content for debugging
            # print(soup.prettify())
            # print(f"Fetching URL: {url}")
            # print("Searching for 'a' tags with class 'pull-left'")
            a_tags = soup.find_all('a', class_='pull-left', href=True)
            # print(f"Found {len(a_tags)} 'a' tags with class 'pull-left'")
            for a_tag in a_tags:
                href = a_tag['href']
                # print(f"Found href: {href}")
                full_url = requests.compat.urljoin(base_url, href)
                car_links.append(full_url)
        else:
            print(f"Failed to retrieve the docket page: {url}")
    return car_links

def scrape2extract_car_details(car_links, table_name):

    for url in car_links:
        status_words = ['Name']
        id_words = []
        response = req_get_log_selenium(url, 0.5)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            text_content = soup.find('strong').get_text()
            parts = text_content.split('\xa0')  # \xa0 is the unicode representation of &nbsp;
            if len(parts) > 1:
                name_after_nbsp = parts[1].strip()  #use strip to remove any leading/trailing whitespace
                id_words.append(name_after_nbsp)
            td_tags = soup.find_all('td', style=lambda value: value and 'width: 40%' in value)
            for td_tag in td_tags:
                status_word = td_tag.get_text(strip=True)
                next_td = td_tag.find_next_sibling('td', style=lambda value: value and 'width: 60%' in value)
                if next_td:
                    span_tag = next_td.find('span')
                    if span_tag and span_tag.get('id') == status_word:
                        id_word = span_tag.get_text(strip=True)
                        status_words.append(status_word)
                        id_words.append(id_word)
        print("TEMMY",status_words)
        print("GEMMY",id_words)
        col_type_list = ['VARCHAR(255)' for _ in range(len(status_words))]
        add_nonduplicate_row(table_name, status_words, col_type_list, id_words, "Location")

        # else:
        #     print(f"Failed to retrieve the car details page: {url}")

def barrettjacksonscrape2(response, table_name):
    # response = req_get_log('https://barrett-jackson.com/')

    initial_site_soup = BeautifulSoup(response, 'html.parser')
    
    #find all 'a' tags that contain 'Collector Car Docket' in their text
    collector_car_docket_links = []
    for a_tag in initial_site_soup.find_all('a', string=lambda text: text and 'Collector Car Docket' in text):
        href = a_tag.get('href')
        if href:
            #ensure the href is an absolute URL
            full_url = requests.compat.urljoin('https://barrett-jackson.com', href)
            collector_car_docket_links.append(full_url)
    
    car_links = scrape2_get_car_links('https://barrett-jackson.com', collector_car_docket_links)
    print(f"\n CAR LINKS {car_links} \n")
    scrape2extract_car_details(car_links, table_name)
    print("done with parse2")
    
#427 GARAGE (id 3)
    
def garage427scrape3(response, table_name):
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
    
#BAVARIAN MOTORSPORTS (id 4)
def bavarianscrape4(table_name):
    response = req_get_log('https://www.bavarianmotorsport.com/inventory') 
    soup = BeautifulSoup(response.content, 'html.parser')
    urls = []
    for h4_tag in soup.find_all('h4', class_='result-item-title text-center'):
        a_tag = h4_tag.find('a', href=True)
        if a_tag:
            full_url = 'https://www.bavarianmotorsport.com' + a_tag['href']
            urls.append(full_url)
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  
    options.add_argument('--disable-gpu')  
    
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    for url in urls:
        response = req_get_log(url)
        car_soup = BeautifulSoup(response.content, 'html.parser')
        cols = ['Name', 'Price']
        name = car_soup.find('div', class_='ws_title').find('h3').get_text(strip=True)
        price = car_soup.find('div', class_='ws_price').get_text(strip=True)
        vals = [name, price]
        details = car_soup.find('div', class_='car-detail').find('div', class_ = 'row').find_all('div', class_ = 'col-md-3 col-sm-4 col-xs-12')
        
        for detail in details:
            detail_col = detail.find('strong').get_text(strip=True)
            detail_col = detail_col[:-1]
            if (detail_col == 'Stock #'):
                detail_col = 'Stock'
            cols.append(detail_col)
            if (detail_col == 'VIN'):
                driver.get(url)
                span_element = driver.find_element(By.CSS_SELECTOR, 'span[onclick]')
                span_element.click()
                detail_val = span_element.text.strip()
            else:
                detail_val = detail.find('p').get_text(strip=True)
                detail_val = detail_val.split(":", 1)[1].strip()
                detail_val = detail_val.replace('\t', '')
                
            vals.append(detail_val)
        print("TOM", cols)
        print("JERRY", vals)
        col_type_list = ['VARCHAR(255)' for _ in range(len(cols))]
        add_nonduplicate_row(table_name,cols,col_type_list,vals,"VIN")


#Cassandra parse 5
        
def cassscrape5(table_name):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    driver.get("https://www.cassandramotorsports.com/inventory/")
    driver.implicitly_wait(1)  

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    elements = soup.find_all("a", class_="border-btn")
    
    urls = []
    for element in elements:
        onclick_value = element.get('onclick')
        if onclick_value:
            start = onclick_value.find("getDetailed('") + len("getDetailed('")
            end = onclick_value.find("')", start)
            url_part = onclick_value[start:end]
            urls.append("https://www.cassandramotorsports.com/" + url_part)
    
    driver.quit()

    for url in urls:
        response = req_get_log_selenium(url, 10)
        car_soup = BeautifulSoup(response.content, 'html.parser')
        name = car_soup.find('h1', class_="mb-0 font-goodtimes").get_text(strip=True)
        price = car_soup.find('h6', class_="mb-4 mb-lg-2 font-goodtimes").get_text(strip=True)
        price = price.split("$", 1)[1].strip()
        cols = ['Name', 'Price']
        vals = [name, price]

        details = car_soup.find_all('li', class_='mb-3')
        
        for detail in details:
            try:
                detail_col = detail.find('div', class_='divfirst').get_text(strip=True)
            except:
                print("UUU ", details)
                raise ValueError("eee")
            
            if (detail_col == 'Stock #'):
                detail_col = 'Stock'
            detail_val = detail.find_all('div')[1].get_text(strip=True)
            if detail_col not in cols:
                cols.append(detail_col)
            if detail_val not in vals:
                vals.append(detail_val)
        
        print("TOM", cols)
        print("JERRY", vals)
        col_type_list = ['VARCHAR(255)' for _ in range(len(cols))]
        add_nonduplicate_row(table_name, cols, col_type_list, vals, "VIN")
