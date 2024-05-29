"""
 * @category : Common Functions
 * @author : Rishabh Gaur
 * @created date : May 28, 2024
 * @updated date : May 28, 2024
 * @company : Birbals Inc
 * @description : Common SQL query structures necessary for scraping; the dynamic nature of these functions make them usable over wide amount of web-scraping problems
"""

import mysql.connector #import mysql connector allows sending SQL commands to MySQL database
from establish_db_connection import mydb, close_cursor_connection #mydb lets us access our database; close_cursor_connection lets us commit our changes and close our cursor 

#Function to create table
#Note: The indices of name and type must match in the two respective lists otherwise the function WILL mismatch types and names.
def create_table(table_name, column_names, column_types): 
    if (len(column_names) != len(column_types)): #checking that names and types are of equal length
        raise ValueError("length of column_names must equal length of column_types")
    
    mycursor = mydb.cursor() #establishes cursor
    default_columns = [ #adds default columns needed for every table
        "`id` INT AUTO_INCREMENT PRIMARY KEY", #id is autoincremented
        "`created_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP", #created date maintains current timestamp
        "`updated_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" #updated date maintains updated timestamp
    ]
    columns = default_columns + [f"`{name}` {dtype}" for name, dtype in zip(column_names, column_types)] #combines column names and column types to be placed in query
    columns_str = ", ".join(columns) #joins these columns with commas
    mycursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str});") #calls query for creating not-already-existing table
    close_cursor_connection(mycursor, mydb) #closes connection to mycursor


#Function to create a unique identifier for each entry in the table
def create_unique_identifier(table_name, existing_column_name):
    mycursor = mydb.cursor() #establishes cursor
    mycursor.execute(f"ALTER TABLE `{table_name}` ADD UNIQUE (`{existing_column_name}`);") #calls query for altering table
    close_cursor_connection(mycursor, mydb) #closes connection to mycursor


#Function to add single column to end of table
def add_single_column_to_table(table_name, new_col_name, new_col_type):
    try:
        mycursor = mydb.cursor() #establishes cursor
        mycursor.execute(f"ALTER TABLE `{table_name}` ADD `{new_col_name}` {new_col_type};") #adds news column as last column in table
        close_cursor_connection(mycursor, mydb) #closes connection to mycursor
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        raise ValueError("issue occurred; check that new_col_type is a valid SQL type")

#Function checks whether table has column, then adds column to the table (this added column MAY be unique)
#Note: unique_bool is True if you want this new column to be a unique identifier, False if not
def add_column_if_not_exists(table_name, new_col_name, new_col_type, unique_bool):
    mycursor = mydb.cursor() #establishes cursor
    #selects one column from the table with column_name
    mycursor.execute(f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND table_schema = DATABASE() 
            AND column_name = '{new_col_name}'
        );
    """)
    if mycursor.fetchone()[0] == 0: #if there are no matching columns to column_nae
        if unique_bool == True: #if we want this added column to be unique
            add_single_column_to_table(table_name, new_col_name, new_col_type) #add single column
            create_unique_identifier(table_name, new_col_name) #make that column a unique identifier
        
        else:
            add_single_column_to_table(table_name, new_col_name, new_col_type) #add single column
    else:
        print(f"column {new_col_name} already in {table_name}")
    close_cursor_connection(mycursor, mydb) #closes connection to mycursor

#Function to add a row to the table
# Note: This function will add a column to the table if the inserted row has more columns than currently exist in the table.
# Note: If unique_identifier is not a column name in your table, all add_column_if_not_exists will always add non-unique column
def add_nonduplicate_row(table_name, col_name_list, col_type_list, values_to_add_list, unique_identifier):
    mycursor = mydb.cursor() #establishes cursor
    if (len(col_name_list) != len(values_to_add_list)): #checking that column names and values to add are of equal length
        raise ValueError("length of column_name_list must equal length of values_to_add_list")
    if (len(col_name_list) != len(col_type_list)): #checking that names and types are of equal length
        raise ValueError("length of col_name_list must equal length of col_type_list")
    
    for i in range(len(col_name_list)): #looping through length of all the lists
        if (col_name_list[i] == unique_identifier):
            add_column_if_not_exists(table_name, col_name_list[i], col_type_list[i], True) #add unique column
        else:
            add_column_if_not_exists(table_name, col_name_list[i], col_type_list[i], False) #add non-unique column
        
        

    col_names_comma_separated = ", ".join(f"`{col}`" for col in col_name_list) #conjoins column names with , and space
    temp = ", ".join('%s' for x in range(len(values_to_add_list)))  #conjoins %s x number of times with , and space

    sql = f"INSERT IGNORE INTO {table_name} ({col_names_comma_separated}) VALUES ({temp})" #inserts row into the table unless its a duplicate value
    vals = tuple(str(value) for value in values_to_add_list) #vals is a tuple of all the values in values_to_add_list
    
    mycursor.execute(sql, vals) #runs sql above
    close_cursor_connection(mycursor, mydb) #closes connection to mycursor
