"""
 * @category : Configuration
 * @author : Rishabh Gaur
 * @created date : May 30, 2024
 * @updated date : June 4, 2024
 * @company : Birbals Inc
 * @description : Several necessary functions related to creating and maintaining a log table, throughout several types of requests.
"""



import mysql.connector #import mysql connector allows sending SQL commands to MySQL database
import requests #import requests module allows us to make HTTP GET requests to desired site
from establish_db_connection import mydb, close_cursor_connection #mydb lets us access our database created in establish_db_connection, close_cursor allows us to close cursor connection
from datetime import datetime, timezone #datetime and timezone let us report time at which some event happens
import time #time is used to make the program wait for some duration on the server said
from selenium import webdriver #selenium is used for automating web browser interaction, useful for web scraping and testing.
from selenium.webdriver.chrome.service import Service as ChromeService #manages the starting and stopping of the ChromeDriver, which allows selenium to control Chrome.
from selenium.webdriver.chrome.options import Options #allows setting various options for the Chrome browser, such as running in headless mode.
from selenium.webdriver.common.by import By #provides a way to locate elements within a document, such as by ID, name, class name, etc., for browser automation.

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'} #header used to bypass robots.txt of 427garage.com
#Function to make log table
def create_log_table(): 
    try:
        mycursor = mydb.cursor()
        #command to create table for log - site source id notes which site from the Sites table the request is from
        # create_table_query = """
        #     CREATE TABLE IF NOT EXISTS `log` (
        #     `id` int(11) NOT NULL AUTO_INCREMENT,
        #     `frontend_request` mediumtext DEFAULT NULL COMMENT 'Insert complete frontend request from API call',
        #     `backend_response` mediumtext DEFAULT NULL COMMENT 'Insert complete backend response',
        #     `response_error_code` int(6) NOT NULL DEFAULT 0 COMMENT 'Insert the backend response error code',
        #     `created_date` datetime NOT NULL DEFAULT current_timestamp() COMMENT 'Insert the request received date and time in UTC format',
        #     `updated_date` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp() COMMENT 'Update the response sent date and time in UTC format',
        #     `site_source_id` MEDIUMINT,
        #     PRIMARY KEY (`id`)
        #     ) 
        #     """
        #long blob version of the above sql query
        create_table_query = """
           CREATE TABLE IF NOT EXISTS `log` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `frontend_request` LONGBLOB DEFAULT NULL COMMENT 'Insert complete frontend request from API call',
            `backend_response` LONGBLOB DEFAULT NULL COMMENT 'Insert complete backend response',
            `response_error_code` int(6) NOT NULL DEFAULT 0 COMMENT 'Insert the backend response error code',
            `created_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Insert the request received date and time in UTC format',
            `updated_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update the response sent date and time in UTC format',
            `site_source_id` MEDIUMINT,
            PRIMARY KEY (`id`)
        );
            """
        mycursor.execute(create_table_query) #executes the query 
        
    
    except mysql.connector.Error as err: #error handling
        print(f"Error in create_log_table: {err}")
    finally:
        close_cursor_connection(mycursor, mydb)


create_log_table() #NECESSARY DO NOT DELETE, CREATES LOG TABLE WHENEVER WE IMPORT THIS FILE
#Function to add row to log table
def add_log(front_end_req, backend_resp, response_error_code, request_utc, response_utc, site_source_id):
    
    try:
        mycursor = mydb.cursor()
        insert_query = "INSERT INTO log (frontend_request, backend_response, response_error_code, created_date, updated_date, site_source_id) VALUES (%s, %s, %s, %s, %s, %s)" 
        log_tuple = (front_end_req, backend_resp, response_error_code, request_utc, response_utc, site_source_id)
        mycursor.execute(insert_query, log_tuple) #adds all the cols and values noted in the arguments for this function
    except mysql.connector.Error as err: #error handling
        print(f"Error in add_log: {err}")
    finally:
        close_cursor_connection(mycursor, mydb)
#Function makes the flag column (scrsaping_status) either 0 or 1 for some row; item specifies which row we want to toggle for, col_of_item helps us locate that item
def flag_toggle(table_name, item, col_of_item):
    try:
        #sql query for updating flag to 1 
        mycursor = mydb.cursor()
        mycursor.execute(f"UPDATE {table_name} SET scraping_status = 1 WHERE {col_of_item} = %s", (item,))
    except mysql.connector.Error as err: #err handling
        print(f"Error in flag_toggle: {err}")
    finally:
        close_cursor_connection(mycursor, mydb)

#Function is equivalent to requests.get and add_log (a combo of the two)
def req_get_log(url, site_source_id): #url specifies which url to do a get req from
    req = requests.Request('GET', url, headers=headers) #calls for get request
    prepared = req.prepare() #prepares the request so we can write it in text
    request_text = '{}\n{}\n\n{}'.format( #formatting the text of the request to better log it in the log table
        f'{prepared.method} {prepared.url} HTTP/1.1',
        '\n'.join(f'{k}: {v}' for k, v in prepared.headers.items()),
        prepared.body or ''
    )
    created_time = datetime.now(timezone.utc) #marks time that the request was made
    #further preparing for receiving response
    session = requests.Session() 
    response = session.send(prepared) 
    updated_time = datetime.now(timezone.utc) #marks time the response was sent
    add_log(request_text, response.text, response.status_code, created_time, updated_time, site_source_id) #adds log to table
    return response
#Function allows us to do a request.get with some delay on client side using selenium 
def req_get_log_selenium(url, wait_seconds, site_source_id):
    path_to_chrome_driver = '/Users/rishabhgaur/Downloads/chromedriver-mac-arm64/chromedriver' #if you get security issue opening, open manually with terminal first then proceed as usual
    options = Options()
    options.headless = True  #run in headless mode
    service = ChromeService(executable_path=path_to_chrome_driver) 
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)
    
    #wait for the page to fully load (adjust the sleep time as necessary)
    time.sleep(wait_seconds)  #adjust this delay as needed to ensure the page fully loads
    
    #get the page source
    response_content = driver.page_source
    
    #close the driver
    driver.quit()
    
    #log the request
    created_time = datetime.now(timezone.utc)
    request_text = f'GET {url} HTTP/1.1'
    updated_time = datetime.now(timezone.utc)
    
    add_log(request_text, response_content, 200, created_time, updated_time, site_source_id)
    
    #mock a response object
    response = requests.models.Response()
    response.status_code = 200
    response._content = response_content.encode('utf-8')
    response.url = url
    
    return response
