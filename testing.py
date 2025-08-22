# dedicated Rescraper logger logic - for possible easier rescraping implementation. Not implemented due to uncertainty of need: unclear client log reading software capabilities
# rescrape_logger = logging.getLogger("rescraper")
# rescrape_logger.setLevel(logging.INFO)
# #creating a handler for rescraper_logger
# rescrape_handler = logging.FileHandler("failed_urls.log")


"""A web scraping script for collecting IC3 annual report data.

This script automates the process of fetching and parsing data from the
IC3.gov website for various years and states. It uses the pandas library
for data handling and saves the scraped data into a partitioned
directory structure using Parquet files.

Author: Zygimantas Sirvys
Date: 2025-08-14
"""
from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import random

# Configuring a logger for requests.urllib3 to see retry messages (When urllib3 performs a retry, it already has a built-in logger that it uses to report this information)
requests_logger = logging.getLogger("urllib3")
requests_logger.setLevel(logging.INFO)

# dedicated Rescraper logger logic - for possible easier rescraping implementation. Not implemented due to uncertainty of need: unclear client log reading software capabilities
# rescrape_logger = logging.getLogger("rescraper")
# rescrape_logger.setLevel(logging.INFO)
# #creating a handler for rescraper_logger
# rescrape_handler = logging.FileHandler("failed_urls.log")

session = requests.Session()
retries = Retry(total=3, backoff_factor=2)
session.mount('https://', HTTPAdapter(max_retries=retries))

#set up of logging basic configuration: TIME - SEVERITY - MESSAGE
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="monitoring.log"
)

#state name list and map, hardcoded, because of the unchangable nature of data (state number, state name and state order)
state_names = ['Alabama', 'Alaska', 'American Samoa', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
               'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Guam', 'Hawaii', 'Idaho', 'Illinois',
               'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
               'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey',
               'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Northern Mariana Islands', 'Ohio',
               'Oklahoma', 'Oregon', 'Pennsylvania', 'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota',
               'Tennessee', 'Texas', 'United States Minor Outlying Islands', 'Utah', 'Vermont', 'Virgin Islands',
               'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
state_map = {index: state for index, state in enumerate(state_names, start=1)}

def get_ic3_url(year, state_number):
    """constructs url for given year and state

    Args:
        year (int): The year for which the data was scraped.
        state_number (int): The state number used to retrieve the state name.
    Returns:
        str: The url for given year and state.
    """
    return f'https://www.ic3.gov/AnnualReport/Reports/{year}State/#?s={state_number}'

def get_soup(url):
    """Fetches a URL, then transforms it into and returns BeautifulSoup object
    Function:
    1) makes an HTTP GET request to a given URL using pre-configured session with retry logic
    2) logs the success or failure of request
    3) includes a delay to avoid 403 blocking (happened scraping without delay)

    Args:
        url (str): URL to scrape
    Returns:
        BeautifulSoup if the request is successful, otherwise None
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/127.0.0.1 Safari/537.36"
        )
    }
    response = session.get(url, headers=headers)

    if response.status_code != 200:
        logging.error(f"Session to {url} failed with a status code: {response.status_code}")
        return None

    # delay to avoid overwhelming the server and getting blocked
    time.sleep(random.uniform(1.0, 3.0))

    return BeautifulSoup(response.text, 'html.parser')

def scrape_report(url):
    """scrapes given url and returns 5 tables as DataFrames

    This function:
    1) calls get_soup() function to get soup object for a given URL
    2) uses .select() css selectors to select 2 sets of tables (first 4 (crimetype) tables  with same structure(2x columns repeated 2 times in each row) and last (age group) table with different structure (3 unique columns))
    3) loops through 4 crimetype tables, their rows, cells and extracts text into 4 dictionaries
    4) loops through 1 last age-group table and extracts text into  dictionary
    5) creates 5 DataFrames for each dictionary

    Returns:
        a dictionary with 5 DataFrames as values
    """
    #

    soup = get_soup(url)
    if soup is None:
        # failed request that already was logged as error, returning empty dictionary to prevent crashing
        return {}
    #check if selectors finds anything but 5 tables (usual web schema)
    all_tables = soup.select('table')
    if len(all_tables) != 5:
        logging.error(f"website schema changed or webpage was blocked: {url}. It should have 5 tables, not: {len(all_tables)}. Skipping...")
        return {}

    #separating 4x .crimetype tables from 5th table with different schema
    crimetype_tables = soup.select('table.crimetype')

    #extracting data into dictionaries
    extracted_tables = []
    dataframes = {}

    if not crimetype_tables:
        logging.error(f"No class=crimetype tables found at {url}. Skipping...")
        return {}
    for table in crimetype_tables:
        rows = table.select('tr')
        table_headers = [th.text for th in rows[0].select('th')[0:2]]
        extracted_table_data = []
        for row in rows[1:]:
            cells = row.select('td')
            #filtering empty rows (css garbage)
            if len(cells) == 4:
                #creating 2 dictionaries for each (double column) row in a table
                row1_data = {}
                row2_data = {}

                for i, header in enumerate(table_headers):
                    row1_data[header] = cells[i].text

                #taking last 2 element from cells (4 elements list)
                for i, header in enumerate(table_headers):
                    row2_data[header] = cells[i + 2].text
                #inserting 2 rows of current table into extracted_table_data
                extracted_table_data.append(row1_data)
                extracted_table_data.append(row2_data)
        extracted_tables.append(extracted_table_data)
    # The last table contains data by age group and has a different header structure.
    # It is selected by finding the last 'table' element on the page.
    last_table = all_tables[-1]
    extracted_last_table = []
    if not last_table:
        logging.error(f"No last table found at {url}. Skipping...")
        return {}
    rows = last_table.find_all('tr')
    table_headers = [th.text for th in last_table.select('thead th')]

    for row in rows[1:]:
        cells = row.select('th, td')

        if len(cells) == 3:
            extracted_row = {}
            for i, header in enumerate(table_headers):
                extracted_row[header] = cells[i].text
            extracted_last_table.append(extracted_row)

    #extracted data transfer into DataFrame
    dataframes['ic3__crime_type_by_victim_count'] = pd.DataFrame(extracted_tables[0])
    dataframes['ic3__crime_type_by_victim_loss'] = pd.DataFrame(extracted_tables[1])
    dataframes['ic3__crime_type_by_subject_count'] = pd.DataFrame(extracted_tables[2])
    dataframes['ic3__crime_type_by_subject_loss'] = pd.DataFrame(extracted_tables[3])
    dataframes['ic3__victims_by_age_group'] = pd.DataFrame(extracted_last_table)

    return dataframes

# Parquet file saving
def save_to_folder(all_dataframes, year, state_number):
    """Saves multiple pandas DataFrames to a directory structure.

        The function creates a partitioned directory path in the format:
        'IC3_data/year=<year>/state=<state_name>/<dataframe_name>.parquet'.
        It handles creating the directories and saves each DataFrame as a Parquet file.

        Args:
            all_dataframes (dict): A dictionary with DataFrame names as keys and
                                   pandas DataFrames as values.
            year (int): The year for which the data was scraped.
            state_number (int): The state number used to retrieve the state name.
        """

    state_name = state_map.get(state_number, f'UnknownState_{state_number}')

    # Create the partition path
    # e.g., 'IC3_data/year=2016/state=Alabama'
    partition_path = os.path.join(
        os.getcwd(),
        'IC3_data',
        f'year={year}',
        f'state={state_name}'
    )
    try:
        for df_name, df in all_dataframes.items():
            #creating a directory, exist_ok=True flag ignores creation if directory already exists
            os.makedirs(partition_path, exist_ok=True)

            #adding file name to partition_path. .to_parquet() expects path with file name
            file_path = os.path.join(partition_path, f'{df_name}.parquet')
            df.to_parquet(file_path)
    except Exception as e:
        logging.error(f'Failed to save for year: {year}, state: {state_name}. Error: {e}')

def main():
    for year in range(2016, 2017):
        for state_number in range(1, 3):
            all_dataframes = scrape_report(
                get_ic3_url(year, state_number)
            )
            save_to_folder(all_dataframes, year, state_number)

if __name__ == "__main__":
    main()
