"""
MAIN FILE THAT STARTS THE APP
"""
import time
from datetime import datetime
from helpers import next_run_time, erors_actualization

from generate_krs_database import generate_krs_database

if __name__ == "__main__":

    start_time = datetime.now()
    #first start and generate db
    try:    
        generate_krs_database(start_time)
    except Exception as e:        
        erors_actualization(start_time=start_time, etap='generate_krs_database', krs=None, day=None, message=f'CRITICAL ERROR - {e}', register=None)

    # Automatic daily actualization   
    while True:
        run_time = next_run_time()
        sleep_time = (run_time - datetime.now()).total_seconds()
        print(f'Next update will start at {run_time}')
        time.sleep(sleep_time)
        start_time = datetime.now()
        # Start generate database
        try:    
            generate_krs_database(start_time)
        except Exception as e:        
            erors_actualization(start_time=start_time, etap='generate_krs_database', krs=None, day=None, message=f'CRITICAL ERROR - {e}', register=None)