import time
import requests #import requests module allows us to make HTTP GET requests to desired site
from bs4 import BeautifulSoup #Beautiful soup enables parsing of HTML returned by requests
import scrapy #import scrapy module allows us to call web-scraping from the terminal and sets up response/request structure for this functionality
from scrapy import signals #signals enables us to run spider_closed when the scraper completes
import re # re lets us parse inconsistent strings with consistent patterns
from establish_db_connection import mydb #mydb lets us access our database created in establish_db_connection
from sql_query_builder import create_table, add_nonduplicate_row, move_columns_to_end, get_first_n_rows #need these for dynamic SQL queris; functions' precise descriptions can be found in sql_query_builder
from establish_log import req_get_log
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

def scrape2_get_car_links(base_url, docket_links):
    car_links = []
    for url in docket_links:
        response = req_get_log(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            for h2_tag in soup.find_all('h2', class_='media-heading'):
                a_tag = h2_tag.find('a', href=True)
                if a_tag:
                    href = a_tag['href']
                    full_url = requests.compat.urljoin(base_url, href)
                    car_links.append(full_url)
        else:
            print(f"Failed to retrieve the docket page: {url}")
    return car_links

def scrape2extract_car_details(car_links, table_name):

    for url in car_links:
        status_words = []
        id_words = []
        response = req_get_log(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
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
        col_type_list = ['VARCHAR(255)' for _ in range(len(status_words))]
        add_nonduplicate_row(table_name, status_words, col_type_list, id_words, "Location")

        # else:
        #     print(f"Failed to retrieve the car details page: {url}")

def barrettjacksonscrape2(response, table_name):
    # response = req_get_log('https://barrett-jackson.com/')

    initial_site_soup = BeautifulSoup(response.content, 'html.parser')
    
    #find all 'a' tags that contain 'Collector Car Docket' in their text
    collector_car_docket_links = []
    for a_tag in initial_site_soup.find_all('a', string=lambda text: text and 'Collector Car Docket' in text):
        href = a_tag.get('href')
        if href:
            #ensure the href is an absolute URL
            full_url = requests.compat.urljoin('https://barrett-jackson.com', href)
            collector_car_docket_links.append(full_url)
    
    car_links = scrape2_get_car_links('https://barrett-jackson.com', collector_car_docket_links)
    scrape2extract_car_details(car_links, table_name)
    
        
        



def barrettjacksonscrape2():
    pass