import requests
import mysql.connector
import json
from datetime import timedelta, datetime
import numpy as np

from config import start_date, stop_date, godzinaOd, godzinaDo, database_table, database_name, remaining_time
from helpers import connect_to_database, close_db, erors_actualization, avg_krs_dl_time_raport

def generate_krs_database(start_time):
    """
    A FUNCTION THAT CREATE A DATABASE KRS COMPANIES WITH USING API REQUEST
    """
    # functions

    def combine_krs_numbers(day):
        """
        A FUNCTION THAT COMBINE KRS WITH USING HISTORY OF CHANGE IN KRS SYSTEM
        """ 
        krs_list = []
        while True:
            try:                 
                actual_url = f'https://api-krs.ms.gov.pl/api/Krs/Biuletyn/{day}?godzinaOd={godzinaOd}&godzinaDo={godzinaDo}'
                response = requests.get(actual_url)
                data = response.json()
                if type(data)==list:
                    for krs in data:
                        krs_list.append(krs)                    
                print(f'{day} - downloaded {len(krs_list)} KRS nubers')                
                return(krs_list)
                       
            except requests.exceptions.ConnectionError:
                message = "Connection error: Could not connect to server."
                erors_actualization(start_time=start_time, etap='combine_krs_numbers', krs=krs, day=krs, message=message, register=None)

            except requests.exceptions.Timeout:
                message ="Timed out waiting for a response from the server."
                erors_actualization(start_time=start_time, etap='combine_krs_numbers', krs=krs, day=krs, message=message, register=None)

            except requests.exceptions.HTTPError as http_err:
                message =f"HTTP error: {http_err}"
                erors_actualization(start_time=start_time, etap='combine_krs_numbers', krs=krs, day=krs, message=message, register=None)
    
    def combine_data(krs_list_filtered, register):
        """
        A FUNCTION THAT COMBINE DATA FROM JSONS AND CREATE LIST OF DELETED KRS
        """
        index__number = 0
        data_pack = []
        deleted_krs = []
        while index__number + 1 <= len(krs_list_filtered):            
            krs = krs_list_filtered[index__number]          
            
            if (index__number+1) % 100 == 0 or (index__number+1)== len(krs_list_filtered):
                print(f'{day} - {index__number+1} of {len(krs_list_filtered)} checked in register {register} - {round((index__number+1)/len(krs_list_filtered)*100, 2)}%')                                                     
            try: 
                actual_url = f'https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs}?rejestr={register}&format=json'                                
                
                response = requests.get(actual_url)                
                            
                # check if the element exist
                if response.status_code == 404:
                    # print(f'element with KRS number {krs} dont exist in register {register} - {response.status_code}')
                    index__number += 1                  
                    continue
                
                # check if the element is deleted
                if response.status_code == 204:
                    # print(f'element with KRS number {krs} has been deleted {register} - {response.status_code}')
                    deleted_krs.append(krs)
                    index__number += 1                  
                    continue

                # check if the response is correct
                if response.status_code != 200:
                    message = f'service_error {response.status_code} with KRS {krs} I will try again {register} - {response.status_code}'
                    erors_actualization(start_time=start_time, etap='combine_data', krs=krs, day=None, message=message, register=register)                                 
                    continue        

                # attempting to parse the response to JSON
                data = response.json() 
            
            except requests.exceptions.ConnectionError:
                message = "Connection error: Could not connect to server."
                erors_actualization(start_time=start_time, etap='combine_data', krs=krs, day=None, message=message, register=register)              
                continue

            except requests.exceptions.Timeout:
                message = "Timed out waiting for a response from the server."
                erors_actualization(start_time=start_time, etap='combine_data', krs=krs, day=None, message=message, register=register) 
                continue

            except requests.exceptions.HTTPError as http_err:
                message = f"HTTP error: {http_err}"
                erors_actualization(start_time=start_time, etap='combine_data', krs=krs, day=None, message=message, register=register) 
                continue

            except json.JSONDecodeError:
                message = "JSON decoding error: response is not in JSON format."
                erors_actualization(start_time=start_time, etap='combine_data', krs=krs, day=None, message=message, register=register) 
                index__number += 1 
                continue

            except requests.exceptions.RequestException as req_err:
                message = f"Unknown error with request: {req_err}"
                erors_actualization(start_time=start_time, etap='combine_data', krs=krs, day=None, message=message, register=register) 
                index__number += 1 
                continue

            # data structure                    
            danePodmiotu = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('danePodmiotu', {})
            danePodmiotuZagranicznego = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('danePodmiotuZagranicznego', {})

            siedziba = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('siedzibaIAdres', {}).get('siedziba', {})
            siedzibaZagraniczna = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('siedzibaIAdresPodmiotuZagranicznego', {}).get('siedziba', {})
            
            adres = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('siedzibaIAdres', {}).get('adres', {})
            adresZagraniczny = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('siedzibaIAdresPodmiotuZagranicznego', {}).get('adres', {})

            email = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('siedzibaIAdres', {}).get('adresPocztyElektronicznej', None)
            emailZagraniczny = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('siedzibaIAdresPodmiotuZagranicznego', {}).get('adresPocztyElektronicznej', None)

            www = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('siedzibaIAdres', {}).get('adresStronyInternetowej', None)
            wwwZagraniczny = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('siedzibaIAdresPodmiotuZagranicznego', {}).get('adresStronyInternetowej', None)

            # convert date
            date = datetime.strptime(data.get('odpis', {}).get('naglowekA', {}).get('stanZDnia'), '%d.%m.%Y')
            converted_date = date.strftime('%Y-%m-%d')

            # convert value of capital
            capital = data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('kapital', {}).get('wysokoscKapitaluZakladowego', {}).get('wartosc', None) 
            if capital is not None:
                capital = float(capital.replace(',','.'))

            # merge main pkd
            def main_pkd():
                main_pkd_item = data.get('odpis', {}).get('dane', {}).get('dzial3', {}).get('przedmiotDzialalnosci', {}).get('przedmiotPrzewazajacejDzialalnosci', None)
                # Check if element exist and have more than 0 items
                if isinstance(main_pkd_item, list) and len(main_pkd_item) > 0:
                    main_pkd = ".".join(list(main_pkd_item[0].values())[1:])
                    return main_pkd
                # If element doesn't exist                     
                return None

            # merge remaining_pkds
            def merged_remaining_pkds():
                merge_pkds_set = set()
                remaining_pkd_items = data.get('odpis', {}).get('dane', {}).get('dzial3', {}).get('przedmiotDzialalnosci', {}).get('przedmiotPozostalejDzialalnosci', None)
                if isinstance(remaining_pkd_items, list):
                    for item in remaining_pkd_items:
                        if isinstance(item, dict) and len(item) > 1:
                            remaining_pkd_item = ".".join(list(item.values())[1:])
                            merge_pkds_set.add(remaining_pkd_item)                    
                # merge elements
                if merge_pkds_set:
                    return ", ".join(merge_pkds_set)
                
                # If element doesn't exist
                return None              
                
            def check_national_or_foregin_values(national_value, foregin_value):
                """
                check alternative values when entity is foregin
                """
                if national_value:
                    return(national_value)
                elif foregin_value:
                    return(foregin_value)
                else:
                    return None  
                
            entry_data = {
                "KRS": krs,
                
                "STAN_Z_DNIA": converted_date,

                "NAZWA": check_national_or_foregin_values(national_value=danePodmiotu.get('nazwa', None),
                                                            foregin_value=danePodmiotuZagranicznego.get('nazwa', None)),
                                                            
                "FORMA_PRAWNA": check_national_or_foregin_values(national_value=danePodmiotu.get('formaPrawna', None),
                                                            foregin_value=danePodmiotuZagranicznego.get('formaPrawna', None)),

                "REJESTR": data.get('odpis', {}).get('naglowekA', {}).get('rejestr', None),
                
                "NIP": check_national_or_foregin_values(national_value=danePodmiotu.get('identyfikatory', {}).get('nip', None),
                                                        foregin_value=danePodmiotuZagranicznego.get('identyfikatory', {}).get('nip', None)),

                "REGON": check_national_or_foregin_values(national_value=danePodmiotu.get('identyfikatory', {}).get('regon', None),
                                                            foregin_value=danePodmiotuZagranicznego.get('identyfikatory', {}).get('regon', None)),
                
                "PKD_GLOWNY": main_pkd(),

                "PKD_POZOSTALE": merged_remaining_pkds(),

                "KRAJ": check_national_or_foregin_values(national_value=siedziba.get('kraj', None),
                                                            foregin_value=siedzibaZagraniczna.get('kraj', None)),

                "WOJEWODZTWO": check_national_or_foregin_values(national_value=siedziba.get('wojewodztwo', None),
                                                                foregin_value=siedzibaZagraniczna.get('wojewodztwo', None)),
                                                                
                "POWIAT": check_national_or_foregin_values(national_value=siedziba.get('powiat', None),
                                                            foregin_value=siedzibaZagraniczna.get('powiat', None)),

                "GMINA": check_national_or_foregin_values(national_value=siedziba.get('gmina', None),
                                                        foregin_value=siedzibaZagraniczna.get('gmina', None)),

                "MIEJSCOWOSC": check_national_or_foregin_values(national_value=siedziba.get('miejscowosc', None),
                                                        foregin_value=siedzibaZagraniczna.get('miejscowosc', None)),

                "ULICA": check_national_or_foregin_values(national_value=adres.get('ulica', None),
                                                            foregin_value=adresZagraniczny.get('ulica', None)),

                "NR_DOMU": check_national_or_foregin_values(national_value=adres.get('nrDomu', None),
                                                            foregin_value=adresZagraniczny.get('nrDomu', None)),

                "KOD_POCZTOWY": check_national_or_foregin_values(national_value=adres.get('kodPocztowy', None),
                                                                    foregin_value=adresZagraniczny.get('kodPocztowy', None)),

                "POCZTA": check_national_or_foregin_values(national_value=adres.get('poczta', None),
                                                            foregin_value=adresZagraniczny.get('poczta', None)),

                "KRAJ_POCZTY": check_national_or_foregin_values(national_value=adres.get('kraj', None),
                                                                foregin_value=adresZagraniczny.get('kraj', None)),

                "EMAIL": check_national_or_foregin_values(national_value=email,
                                                            foregin_value=emailZagraniczny),

                "WWW": check_national_or_foregin_values(national_value=www,
                                                        foregin_value=wwwZagraniczny),
                "WYSOKOSC_KAPITALU_ZAKLADOWEGO": capital,
                "WALUTA_KAPITALU_ZAKLADOWEGO": data.get('odpis', {}).get('dane', {}).get('dzial1', {}).get('kapital', {}).get('wysokoscKapitaluZakladowego', {}).get('waluta', None)
            }

            index__number += 1
            data_pack.append(entry_data)            

        return data_pack, deleted_krs

    def create_table_and_send_data(data_pack, deleted_krs):
        """
        CREATE DATABASE TABLE, INSERT AND DELETE DATA
        """
        #Connect to database   
        cursor, connection = connect_to_database(start_time)        

        # Create table if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {database_table_register} (
                KRS VARCHAR(60) PRIMARY KEY,
                STAN_Z_DNIA DATE,
                NAZWA VARCHAR(2000),
                FORMA_PRAWNA VARCHAR(255),
                REJESTR VARCHAR(60),
                NIP VARCHAR(60),
                REGON VARCHAR(60),
                PKD_GLOWNY VARCHAR(60),
                PKD_POZOSTALE TEXT,
                KRAJ VARCHAR(60),
                WOJEWODZTWO VARCHAR(60),
                POWIAT VARCHAR(60),
                GMINA VARCHAR(60),
                MIEJSCOWOSC VARCHAR(60),
                ULICA VARCHAR(255),
                NR_DOMU VARCHAR(60),
                KOD_POCZTOWY VARCHAR(60),
                POCZTA VARCHAR(60),
                KRAJ_POCZTY VARCHAR(60),
                EMAIL VARCHAR(255),
                WWW VARCHAR(255),
                WYSOKOSC_KAPITALU_ZAKLADOWEGO DECIMAL(20, 2),
                WALUTA_KAPITALU_ZAKLADOWEGO VARCHAR(60)
            );
        """)
        connection.commit()

        if data_pack != []:
            # upload krs records
            insert_query = f"""
                INSERT INTO {database_table_register} (
                    KRS, STAN_Z_DNIA, NAZWA, FORMA_PRAWNA, REJESTR, NIP, REGON, PKD_GLOWNY, PKD_POZOSTALE, 
                    KRAJ, WOJEWODZTWO, POWIAT, GMINA, MIEJSCOWOSC, ULICA, NR_DOMU, 
                    KOD_POCZTOWY, POCZTA, KRAJ_POCZTY, EMAIL, WWW, 
                    WYSOKOSC_KAPITALU_ZAKLADOWEGO, WALUTA_KAPITALU_ZAKLADOWEGO
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    STAN_Z_DNIA = VALUES(STAN_Z_DNIA),
                    NAZWA = VALUES(NAZWA),
                    FORMA_PRAWNA = VALUES(FORMA_PRAWNA),
                    REJESTR = VALUES(REJESTR),
                    NIP = VALUES(NIP),
                    REGON = VALUES(REGON),
                    PKD_GLOWNY = VALUES(PKD_GLOWNY),
                    PKD_POZOSTALE = VALUES(PKD_POZOSTALE),
                    KRAJ = VALUES(KRAJ),
                    WOJEWODZTWO = VALUES(WOJEWODZTWO),
                    POWIAT = VALUES(POWIAT),
                    GMINA = VALUES(GMINA),
                    MIEJSCOWOSC = VALUES(MIEJSCOWOSC),
                    ULICA = VALUES(ULICA),
                    NR_DOMU = VALUES(NR_DOMU),
                    KOD_POCZTOWY = VALUES(KOD_POCZTOWY),
                    POCZTA = VALUES(POCZTA),
                    KRAJ_POCZTY = VALUES(KRAJ_POCZTY),
                    EMAIL = VALUES(EMAIL),
                    WWW = VALUES(WWW),
                    WYSOKOSC_KAPITALU_ZAKLADOWEGO = VALUES(WYSOKOSC_KAPITALU_ZAKLADOWEGO),
                    WALUTA_KAPITALU_ZAKLADOWEGO = VALUES(WALUTA_KAPITALU_ZAKLADOWEGO)
            """
            for company in data_pack:
                values = (
                    company["KRS"],
                    company["STAN_Z_DNIA"],
                    company["NAZWA"],
                    company["FORMA_PRAWNA"],
                    company["REJESTR"],
                    company["NIP"],
                    company["REGON"],
                    company["PKD_GLOWNY"],
                    company["PKD_POZOSTALE"],
                    company["KRAJ"],
                    company["WOJEWODZTWO"],
                    company["POWIAT"],
                    company["GMINA"],
                    company["MIEJSCOWOSC"],
                    company["ULICA"],
                    company["NR_DOMU"],
                    company["KOD_POCZTOWY"],
                    company["POCZTA"],
                    company["KRAJ_POCZTY"],
                    company["EMAIL"],
                    company["WWW"],
                    company["WYSOKOSC_KAPITALU_ZAKLADOWEGO"],
                    company["WALUTA_KAPITALU_ZAKLADOWEGO"]
                )
                cursor.execute(insert_query, values)

            connection.commit()

        print(f'{len(data_pack)} of KRS numbers have been updated to {database_name}, table: {database_table_register}')
        

        # delete krs records
        if deleted_krs != []:
            dynamic_placeholders = ', '.join(['%s'] * len(deleted_krs))
            delete_query = f"DELETE FROM firmy_krs_table_P WHERE KRS IN ({dynamic_placeholders})"
            cursor.execute(delete_query, deleted_krs)

            records_deleted = cursor.rowcount
        
            print(f'{records_deleted} of KRS numbers have been deleted {database_name}, table: {database_table_register}')
        else:
            print(f'0 of KRS numbers have been deleted {database_name}, table: {database_table_register}')
            
        connection.commit()

        #close database connection and cursor
        close_db(cursor, connection)

    # START MAIN FUNCTION

    #registers
    registers = ['P','S']
       
    # table history change name
    database_table_update_status = database_table+"_history_change"

    #Connect to database   
    cursor, connection = connect_to_database(start_time) 

    # create table update status
    cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {database_table_update_status} (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                STAN_Z_DNIA DATE,
                KRS_CHANGED MEDIUMTEXT,
                AMOUNT INT
            );
        """)
    connection.commit()

    cursor.execute(f"""
        SELECT STAN_Z_DNIA FROM {database_table_update_status}
        """)
            
    last_date_from_db = cursor.fetchall()

    if last_date_from_db != []:
        day=last_date_from_db[-1][0]
    else:
        day=start_date

    #close database connection and cursor
    close_db(cursor, connection)    

    while day <= stop_date:

        krs_list = combine_krs_numbers(day)         
            
        # combine data
        if krs_list != []:
            for register in registers:
                # create table name
                database_table_register = database_table + "_" + register

                # set start time
                combine_data_start_time = datetime.now()                
                # create krs and update
                krs_and_update_day = []

                # connect to database
                cursor, connection = connect_to_database(start_time)                            

                # filtr krs numbers if table register exist
                try:
                    cursor.execute(f"""
                        SELECT COUNT(*) 
                        FROM information_schema.tables 
                        WHERE table_schema = '{database_name}'
                        AND table_name = '{database_table_register}'
                        """)
                    table_exist = cursor.fetchall()                   
                    
                    if table_exist[0][0]:                
                        cursor.execute(f"""
                        SELECT KRS, STAN_Z_DNIA FROM {database_table_register}
                        """)
                        krs_and_update_day = cursor.fetchall()

                except mysql.connector.Error as e:
                    message = f"Database error: {e}"
                    erors_actualization(start_time=start_time, etap='filtr krs numbers if table register exist', krs=None, day=day, message=message, register=register)
     
                # filtr krs list
                if krs_and_update_day != []:
                    # filtr krs_and_update_day - only days past current day (day)
                    # convert to numpy matrix
                    krs_and_update_day = np.array(krs_and_update_day, dtype=object)                    
                    filtered_krs_db = krs_and_update_day[krs_and_update_day[:,1] > day, 0]
                    # convert krs_list to numpy matrix
                    krs_list_np = np.array(krs_list, dtype=object)
                    # difference between krs_list and filtered_krs_db - KRS to check
                    krs_list_filtered = np.setdiff1d(krs_list_np,filtered_krs_db)
                    # convert krs_list to list
                    krs_list_filtered = krs_list_filtered.tolist()                    
                else:
                    krs_list_filtered = krs_list               

                # combine data of krs numbers from list                
                data_pack, deleted_krs = combine_data(krs_list_filtered, register)

                # uplooad data to table in database
                if data_pack != [] or deleted_krs != []:  
                    create_table_and_send_data(data_pack, deleted_krs)

                #close database connection and cursor
                close_db(cursor, connection)

                # set stop time
                combine_data_stop_time = datetime.now()

                # avarage krs download time
                avg_krs_dl_time = (combine_data_stop_time-combine_data_start_time).total_seconds()/len(krs_list_filtered)
                
                # save to raport
                avg_krs_dl_time_raport(start_time=start_time, day=day, avg_krs_dl_time=round(avg_krs_dl_time,3), register=register)
                 

                
        # connect to database
        cursor, connection = connect_to_database(start_time)            

        # update data status
        entry_data_table_update_status = {  
                    "STAN_Z_DNIA": day,
                    "KRS_CHANGED": ", ".join(krs_list),
                    "AMOUNT": len(krs_list)
                }        
        insert_query = f"""
            INSERT INTO {database_table_update_status} (
                STAN_Z_DNIA, KRS_CHANGED, AMOUNT
            ) VALUES (%s, %s, %s);
            """
        
        values = (
            entry_data_table_update_status["STAN_Z_DNIA"],
            entry_data_table_update_status["KRS_CHANGED"],
            entry_data_table_update_status["AMOUNT"]
        )        
        
        if last_date_from_db == [] or day != last_date_from_db[-1][0]:
            
            cursor.execute(insert_query, values)

        connection.commit()

        day += timedelta(days=1)

        #close database connection and cursor
        close_db(cursor, connection)

    # THE END