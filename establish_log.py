import mysql.connector #import mysql connector allows sending SQL commands to MySQL database
import requests
from establish_db_connection import mydb, close_cursor_connection
from datetime import datetime, timezone
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'}

def create_log_table(): 
    try:
        mycursor = mydb.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS `log` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `frontend_request` mediumtext DEFAULT NULL COMMENT 'Insert complete frontend request from API call',
            `backend_response` mediumtext DEFAULT NULL COMMENT 'Insert complete backend response',
            `response_error_code` int(6) NOT NULL DEFAULT 0 COMMENT 'Insert the backend response error code',
            `created_date` datetime NOT NULL DEFAULT current_timestamp() COMMENT 'Insert the request received date and time in UTC format',
            `updated_date` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp() COMMENT 'Update the response sent date and time in UTC format',
            PRIMARY KEY (`id`)
            ) 
            """
        # create_table_query = """
        #    CREATE TABLE IF NOT EXISTS `log` (
        #     `id` int(11) NOT NULL AUTO_INCREMENT,
        #     `frontend_request` LONGBLOB DEFAULT NULL COMMENT 'Insert complete frontend request from API call',
        #     `backend_response` LONGBLOB DEFAULT NULL COMMENT 'Insert complete backend response',
        #     `response_error_code` int(6) NOT NULL DEFAULT 0 COMMENT 'Insert the backend response error code',
        #     `created_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Insert the request received date and time in UTC format',
        #     `updated_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update the response sent date and time in UTC format',
        #     PRIMARY KEY (`id`)
        # );
        #     """
        mycursor.execute(create_table_query)
    
    except mysql.connector.Error as err:
        print(f"Error in create_log_table: {err}")
    finally:
        close_cursor_connection(mycursor, mydb)


create_log_table()
def add_log(front_end_req, backend_resp, response_error_code, request_utc, response_utc):
    
    try:
        mycursor = mydb.cursor()
        insert_query = "INSERT INTO log (frontend_request, backend_response, response_error_code, created_date, updated_date) VALUES (%s, %s, %s, %s, %s)"
        log_tuple = (front_end_req, backend_resp, response_error_code, request_utc, response_utc)
        mycursor.execute(insert_query, log_tuple)
    except mysql.connector.Error as err:
        print(f"Error in add_log: {err}")
    finally:
        close_cursor_connection(mycursor, mydb)

def flag_toggle(table_name, item, col_of_item):
    try:
        mycursor = mydb.cursor()
        mycursor.execute(f"UPDATE {table_name} SET Flag = 1 WHERE {col_of_item} = %s", (item,))
    except mysql.connector.Error as err:
        print(f"Error in flag_toggle: {err}")
    finally:
        close_cursor_connection(mycursor, mydb)

def req_get_log(url):
    req = requests.Request('GET', url, headers=headers)
    prepared = req.prepare()
    request_text = '{}\n{}\n\n{}'.format(
        f'{prepared.method} {prepared.url} HTTP/1.1',
        '\n'.join(f'{k}: {v}' for k, v in prepared.headers.items()),
        prepared.body or ''
    )
    created_time = datetime.now(timezone.utc)
    session = requests.Session()
    response = session.send(prepared)
    updated_time = datetime.now(timezone.utc)
    add_log(request_text, response.text, response.status_code, created_time, updated_time)
    return response

def req_get_log_selenium(url, wait_seconds):
    path_to_chrome_driver = '/Users/rishabhgaur/Downloads/chromedriver-mac-arm64/chromedriver' #if you get security issue opening, open manually with terminal first then proceed as usual
    options = Options()
    options.headless = True  #run in headless mode
    service = ChromeService(executable_path=path_to_chrome_driver) 
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)
    
    # Wait for the page to fully load (adjust the sleep time as necessary)
    time.sleep(wait_seconds)  # Adjust this delay as needed to ensure the page fully loads
    
    # Get the page source
    response_content = driver.page_source
    
    # Close the driver
    driver.quit()
    
    # Log the request
    created_time = datetime.now(timezone.utc)
    request_text = f'GET {url} HTTP/1.1'
    updated_time = datetime.now(timezone.utc)
    
    add_log(request_text, response_content, 200, created_time, updated_time)
    
    # Mock a response object
    response = requests.models.Response()
    response.status_code = 200
    response._content = response_content.encode('utf-8')
    response.url = url
    
    return response
