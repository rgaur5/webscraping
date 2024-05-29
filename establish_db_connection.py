"""
 * @category : Configuration
 * @author : Rishabh Gaur
 * @created date : May 28, 2024
 * @updated date : May 28, 2024
 * @company : Birbals Inc
 * @description : File allows for creation and interaction with database. Also, provides function to close cursor and connection.
"""

import mysql.connector #import mysql connector allows sending SQL commands to MySQL database
#PART 1: Function to create database with name web_scraping
def create_db_web_scraping(): #function to create the database, if the database exists access it using mydb
    temp = mysql.connector.connect( #create temp connector (use mydb if you want to access the database)
    host="localhost",
    user="root",
    password="Gaur2023!"
    )

    cursor = temp.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS web_scraping") # create a new database if not exists already #eee

    
    cursor.close() # close the cursor
    temp.close() #close temporary connection

#PART 2: Variable to access web_scraping 
#Do 'from establish_db_connection import mydb' to get connection to database
create_db_web_scraping() #ensures db is created before attempting connection whenever mydb is imported

#DO NOT CLOSE MYDB UNTIL ALL SCRAPING IS COMPLETE
mydb = mysql.connector.connect( #creates connector named mydb which will be used often throughout this project
  host="localhost",
  user="root",
  password="Gaur2023!",
  database="web_scraping"
)



#PART 3: Function to close cursor and commit connection.
def close_cursor_connection(cursor, connection):
    connection.commit() #commits remaining changes to database
    cursor.close() #closes cursor
    
    