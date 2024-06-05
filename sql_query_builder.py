"""
 * @category : Common Functions
 * @author : Rishabh Gaur
 * @created date : May 28, 2024
 * @updated date : June 4, 2024
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
    # else:
    #     print(f"column {new_col_name} already in {table_name}")
    close_cursor_connection(mycursor, mydb) #closes connection to mycursor

#Function adds non-duplicate row to specified table; values_to_add_list contains contents of the row and col_name_list specifies which columns to add values to
def add_nonduplicate_row(table_name, col_name_list, col_type_list, values_to_add_list):
    mycursor = mydb.cursor()

    #check lengths of input lists
    if len(col_name_list) != len(values_to_add_list):
        raise ValueError("Length of column_name_list must equal length of values_to_add_list")
    if len(col_name_list) != len(col_type_list):
        raise ValueError("Length of col_name_list must equal length of col_type_list")

    #ensure all columns exist in the table
    for i in range(len(col_name_list)):
        add_column_if_not_exists(table_name, col_name_list[i], col_type_list[i], False)
    
    #check if the row already exists
    where_clause = " AND ".join(f"`{col}` = %s" for col in col_name_list)
    check_sql = f"SELECT COUNT(*) FROM `{table_name}` WHERE {where_clause}"
    check_vals = tuple(str(value) for value in values_to_add_list)
    mycursor.execute(check_sql, check_vals)
    result = mycursor.fetchone()

    if result[0] > 0: 
        print("row already exists, skipping insertion.")
        return

    #get existing columns in the table
    mycursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing_columns = set(row[0] for row in mycursor.fetchall())

    #prepare to insert values
    col_names_comma_separated = ", ".join(f"`{col}`" for col in col_name_list)
    temp = ", ".join('%s' for _ in range(len(values_to_add_list)))

    #insert row with provided values
    sql = f"INSERT IGNORE INTO {table_name} ({col_names_comma_separated}) VALUES ({temp})"
    vals = tuple(str(value) for value in values_to_add_list)
    mycursor.execute(sql, vals)


    col_name_list.append('id')
    col_name_list.append('created_date')
    col_name_list.append('updated_date')

    last_id = mycursor.lastrowid

    #insert NULLs for any new columns not in the provided values
    for col in existing_columns - set(col_name_list): #for the columns that dont have any values as mentioned by values_to_add_list
        mycursor.execute(f"UPDATE `{table_name}` SET `{col}` = NULL WHERE `id` = %s", (last_id,))

    #close cursor and connection
    close_cursor_connection(mycursor, mydb)




#Function to move created date and updated date to the end of the table
def move_columns_to_end(table_name):
    mycursor = mydb.cursor() #establishes cursor
    try:
        #get all columns from the table
        mycursor.execute(f"SHOW COLUMNS FROM {table_name};") 
        columns = mycursor.fetchall()
        if columns:
            last_col_name = columns[-1][0] # the last column name will be the last entry in the result set
        else:
            last_col_name = None #will resilt in error being raised

        # move created_date to after last col name and updated_date to after created_date at the end of the table
        mycursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN `created_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP AFTER `{last_col_name}`;")
        mycursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN `updated_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER `created_date`;")
    except mysql.connector.Error as err:
        print(f"Error in move_columns_to_end: {err}") #error handling
    finally:
        close_cursor_connection(mycursor, mydb)  #closes connection to mycursor

#Function to get the first n rows from a specific column in a specific table.
def get_first_n_rows(table_name, column_name, n):
    try:
        mycursor = mydb.cursor() #establishes cursor
        query = f"SELECT {column_name} FROM `{table_name}` WHERE `id` BETWEEN %s AND %s;" #get first %s columns of column column_name from table_name
        mycursor.execute(query, (1,n)) #the limit is n
        results = mycursor.fetchall() 
        values = [result[0] for result in results] #list of all datapoints collected
        print(f"TTT {values}")
        return values
    except mysql.connector.Error as err:
        print(f"Error in get_first_n_rows: {err}") #error handling
    finally:
        close_cursor_connection(mycursor, mydb) #closes connection to mycursor

#Function adds flag column to end of a table
def add_flag_column(table_name, column_name):

    mycursor = mydb.cursor()
    try:
        mycursor.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE '{column_name}';")  #check if the column already exists
        column_exists = mycursor.fetchone()

        if not column_exists: #if it doesn't exist
            #add the column and set all values to 0
            mycursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{column_name}` INT DEFAULT 0;")
        else: #if it does exist
            mycursor.execute(f"UPDATE `{table_name}` SET `{column_name}` = 0 WHERE `{column_name}` IS NULL;") #set all not-already-defined rows to have a flag 0
    except mysql.connector.Error as err:
        print(f"Error in add_flag_column: {err}")
    finally:
        close_cursor_connection(mycursor, mydb)

#Function to add non-duplicate row to a table while also specifying how the row is related to the Sites table
def add_nonduplicate_row_relational(table_name, col_name_list, col_type_list, values_to_add_list, site_source_id):
    mycursor = mydb.cursor()

    #check lengths of input lists
    if len(col_name_list) != len(values_to_add_list):
        raise ValueError("Length of column_name_list must equal length of values_to_add_list")
    if len(col_name_list) != len(col_type_list):
        raise ValueError("Length of col_name_list must equal length of col_type_list")

    #ensure all columns exist in the table
    for i in range(len(col_name_list)):
        add_column_if_not_exists(table_name, col_name_list[i], col_type_list[i], False)
    
    #check if the row already exists
    where_clause = " AND ".join(f"`{col}` = %s" for col in col_name_list)
    check_sql = f"SELECT COUNT(*) FROM `{table_name}` WHERE {where_clause}"
    check_vals = tuple(str(value) for value in values_to_add_list)
    mycursor.execute(check_sql, check_vals)
    result = mycursor.fetchone()

    if result[0] > 0:
        print("row already exists, skipping insertion.")
        return

    #get existing columns in the table
    mycursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing_columns = set(row[0] for row in mycursor.fetchall())

    #prepare to insert values
    col_names_comma_separated = ", ".join(f"`{col}`" for col in col_name_list)
    col_names_comma_separated += ", site_source_id"
    values_to_add_list.append(site_source_id)
    temp = ", ".join('%s' for _ in range(len(values_to_add_list)))
    #insert row with provided values
    sql = f"INSERT IGNORE INTO {table_name} ({col_names_comma_separated}) VALUES ({temp})"
    
    vals = tuple(str(value) for value in values_to_add_list)
    mycursor.execute(sql, vals)

    col_name_list.append('id')
    col_name_list.append('created_date')
    col_name_list.append('updated_date')
    col_name_list.append('site_source_id')

    last_id = mycursor.lastrowid

    #insert NULLs for any new columns not in the provided values
    for col in existing_columns - set(col_name_list): #for the columns that dont have any values as mentioned by values_to_add_list
        mycursor.execute(f"UPDATE `{table_name}` SET `{col}` = NULL WHERE `id` = %s", (last_id,))

    #close cursor and connection
    close_cursor_connection(mycursor, mydb)
