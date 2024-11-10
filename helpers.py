import mysql.connector
import time
import os
from datetime import datetime, timedelta
from config import database_name, database_host, database_user, database_password, database_port, remaining_time, hour, minute

def connect_to_database(start_time):
    """
        CONNECT TO DATABASE FUNCTION
    """
    while True:
        try:
            # Connect to database
            connection = mysql.connector.connect(
                host=database_host, 
                port=database_port,
                user=database_user,
                password=database_password,
            )
            cursor = connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name};")
            connection.database = database_name                
            return cursor, connection                
        except mysql.connector.Error as e:
            message = f"Database error: {e}"
            erors_actualization(start_time=start_time, etap='connect_to_database', krs=None, day=None, message=message, register=None)
            
def close_db(cursor, connection):
    """
        DISCONNECT DATABASE FUNCTION
    """
    if cursor:
        cursor.close()
    if connection:
        connection.close()

def timer(time_w: int):
    """
        TIMER
    """
    for i in range(time_w, 0, -1):
        print(f"{i} seconds remaining...")
        time.sleep(1)

def next_run_time():
    """
    RETURNS THE NEXT STARTUP TIME
    """
    now = datetime.now()
    next_run = now.replace(hour=hour, minute=minute) # Start time 

    if now >= next_run:  # If it's already after start time set it for tomorrow
        next_run += timedelta(days=1)

    return next_run

def erors_actualization(start_time, etap, krs, day, message, register):
    """
        IF ERRORS EXIST THIS METHOD WRITE THEM TO FILE
    """
    # find current directory
    current_directory = os.getcwd()

    # check if folder exist
    folder_path = current_directory+'/errors'

    # create folder if not exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # set file path
    file_path = folder_path+f'/errors_log - [{start_time.strftime('%Y-%m-%d %H-%M')}].csv'    

    # set time of error message
    error_message_time = datetime.now()

    # check if file exist
    file_exists = os.path.isfile(file_path) 
         
    
    with open (file_path, 'a', newline='') as errors_log:
        # create headers
        if not file_exists:
            errors_log.write(f'ETAP;KRS;DAY;MESSAGE;REGISTER;DATE;TIME\n')
        # create error record
        errors_log.write(f'{etap};{krs};{day};{message};{register};{error_message_time.strftime('%Y-%m-%d')};{error_message_time.strftime('%H:%M:%S')}\n')        

    # remaining time
    timer(remaining_time)

def avg_krs_dl_time_raport(start_time, day, avg_krs_dl_time, register):
    """
        MAKE RAPORTS ABOUT TIME OF DOWNLOAD KRS
    """
    # find current directory
    current_directory = os.getcwd()

    # check if folder exist
    folder_path = current_directory+'/raports'

    # create folder if not exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # set file path
    file_path = folder_path+f'/av_krs - [{start_time.strftime('%Y-%m-%d %H-%M')}].csv'    

    # set time of error message
    time = datetime.now()

    # check if file exist
    file_exists = os.path.isfile(file_path)       
    
    with open (file_path, 'a', newline='') as raport_record:
        # create headers
        if not file_exists:
            raport_record.write(f'DAY;AVARAGE_DOWNLOAD_TIME;REGISTER;DATE;TIME\n')
        # create error record
        raport_record.write(f'{day};{avg_krs_dl_time};{register};{time.strftime('%Y-%m-%d')};{time.strftime('%H:%M:%S')}\n')
