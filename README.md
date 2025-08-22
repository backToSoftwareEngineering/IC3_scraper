# IC3 Annual Report Scraper

This is a **Python-based web scraping script** designed to automatically collect and process public annual report data from the [IC3.gov](https://www.ic3.gov) website. The project demonstrates core **data engineering skills** by extracting unstructured data and storing it in a **clean, structured format** ready for further analysis.

## Key Features

- Uses the `requests` library with a built-in retry mechanism to handle temporary connection failures.

-  Extracts tabular data from HTML and saves it into highly efficient **Parquet files**.

-  Organizes the output data into a logical directory structure partitioned by **year** and **state**, which is a common practice for large datasets.

-  Implements a basic logging system to monitor the scraping process and identify URLs where data extraction failed.
