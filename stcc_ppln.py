#!/usr/bin/env python
# coding: utf-8

# # St. Clair County Land Use and Parcel Data Pipeline

# In[ ]:


get_ipython().system('pip3 install Selenium')
get_ipython().system('pip3 install pandas')
get_ipython().system('pip3 install lxml')
get_ipython().system('pip3 install html5lib')
get_ipython().system('pip3 install findspark')
get_ipython().system('pip3 install ydata-profiling[notebook]')
get_ipython().system('pip3 install ydata-profiling[pyspark]')


# In[ ]:


import csv
import json
import math
import os
import pprint
import re
import requests
import time
import queue

import findspark
findspark.init()
import pandas as pd
import pyspark.sql.functions as F


# In[ ]:


from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import timedelta
from io import StringIO
from queue import Queue
from threading import Lock, RLock, Thread
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from ydata_profiling import ProfileReport


# In[ ]:


from IPython.core.display import HTML
display(HTML("<style>pre { white-space: pre !important; }</style>"))


# ## Extraction

# In[ ]:


search_pg = "https://stclairil.devnetwedge.com/"
cwd = os.getcwd()


# ### Download St. Clair Co. Property Tax Inquiry Selected Townships Parcel Listing 

# In[ ]:


options = Options()
options.add_argument("--start-maximized")
options.add_argument("--headless=new")
prefs = {"download.default_directory": f"{cwd}"}
options.add_experimental_option("prefs", prefs)


# In[ ]:


driver = webdriver.Chrome(options)
driver.implicitly_wait(3)
driver.get(search_pg)


# In[ ]:


# Click into Advanced Search Tab
advance_search_tab = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "//a[@href='#advanced-search']"))
)
advance_search_tab.click()


# In[ ]:


# Select Townships
township_select = Select(WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.ID, "advanced-search-townships"))
))
township_select.select_by_value("02")
township_select.select_by_value("11")
township_select.select_by_value("01")
township_select.select_by_value("06")


# In[ ]:


# Check All Years Box and Search
all_years_chkbx = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.ID, "advanced-search-include-all-years"))
)
form = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "//form"))
)
driver.execute_script(f"document.getElementById('advanced-search-include-all-years').click()")
all_years_chkbx.submit()


# In[ ]:


# Export Results to CSV and Download
export_btn = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, "//a[@href='/Search/ExportClientsListToCSV']"))
)
export_btn.click()


# In[ ]:


driver.quit()


# ### Scrape Parcel Information Tables

# In[ ]:


parcel_list_df = pd.read_csv("Exported_Search_Results.csv")


# In[ ]:


parcel_list_df.describe(include="all")


# In[ ]:


parcel_list_df.head()


# In[ ]:


# Format PropertyAccountNumber to be Solely Numeric + 'X'
def only_numeric(str):
    return "".join(re.findall(r"[\dX]", str))

parcel_list_df['Property Account Number'] = parcel_list_df['Property Account Number'].apply(only_numeric)
parcel_list_df.head()


# In[ ]:


with open("Exported_Search_Results.csv", newline="") as csvfile:
    reader = csv.reader(csvfile)
    parcel_list = list(reader)[2:]

url_pcs = [ (only_numeric(row[1]), row[0]) for row in parcel_list ]
display(len(url_pcs))
print(url_pcs[::1000])


# In[ ]:


def scrape_parcel_pg(listing_number, listing_year):
    parcel_url = f"{search_pg}parcel/view/{listing_number}/{listing_year}"
    parcel_pg = requests.get(parcel_url)
    parcel_pg = BeautifulSoup(parcel_pg.text, "html.parser")
    panel_divs = parcel_pg.find_all(class_="panel panel-info")
        
    tables_dict = {}
    
    for div in panel_divs:
        try:
            tbl_key = div.div.h3.text
            tbl = div.div.h3.parent.find_next_sibling().find("table").prettify()
            tables_dict[tbl_key] = pd.read_html(StringIO(tbl))[0]
        except:
            continue
    
    parcel_number = listing_number
    year = int(listing_year)

    try:
        # Property Information Table
        parcel_address = tables_dict['Property Information'][1][0].split("Site Address")[1].strip()
        sale_status = tables_dict["Property Information"][0][2].split("Sale Status")[1].strip()
        property_class = tables_dict["Property Information"][0][3].split("-")[0].split("Property Class")[1].strip()
        tax_status = tables_dict["Property Information"][2][3].split("Tax Status")[1].strip()
        net_taxable = tables_dict["Property Information"][0][4].split("Net Taxable Value")[1].replace(",", "").strip("")
        tax_rate = tables_dict["Property Information"][1][4].split("Tax Rate")[1].strip()
        total_tax = tables_dict["Property Information"][2][4].split("$")[1].strip()
        township = tables_dict["Property Information"][0][5].split("Township")[1].strip()
        acreage = tables_dict["Property Information"][1][5].split("Acres")[1].strip()
        
        # Assessments Table
        homesite_val = tables_dict["Assessments"].get("Homesite")[0]
        dwelling_val = tables_dict["Assessments"].get("Dwelling")[0]
        dept_rev_val = tables_dict["Assessments"].get("Total")[0]
    
        # Billing Table
        total_billed = tables_dict["Billing"].get("Totals")[4].strip("$")
        total_unpaid = tables_dict["Billing"].get("Totals")[6].strip("$")
    
        # Owner Information Table
        owner_name = tables_dict["Parcel Owner Information"].get("Name")[0]
        owner_address = tables_dict["Parcel Owner Information"].get("Address")[0]
    except Exception as err:
        time.sleep(1)
        return {
            "parcel": listing_number,
            "year": listing_year,
            "error": err
        }
    else:
        time.sleep(2)
        return {
            "parcel_number": parcel_number,
            "year": year,
            "parcel_address": parcel_address,
            "owner": owner_name,
            "owner_address": owner_address,
            "sale_status": sale_status,
            "property_class": property_class,
            "tax_status": tax_status,
            "net_taxable": net_taxable, 
            "tax_rate": tax_rate,
            "total_tax": total_tax,
            "township": township,
            "acreage": acreage,
            "homesite_val": homesite_val, 
            "dwelling_val": dwelling_val,
            "dept_rev_val": dept_rev_val,
            "total_billed": total_billed,
            "total_unpaid": total_unpaid
        }


# In[ ]:


def write_records():
    
    function_start = time.perf_counter()
    max_threads = 1000
    processed_ct = 0
    records_missed = []
    parcel_records = []
    success_headers = [
        "parcel_number", "year", "parcel_address", "owner", "owner_address", 
        "sale_status", "property_class", "tax_status", "net_taxable", 
        "tax_rate", "total_tax", "township", "acreage", "homesite_val", 
        "dwelling_val", "dept_rev_val", "total_billed", "total_unpaid"
    ]
    fail_headers = ["parcel", "year", "error"]
    record_q = Queue() 
    csv_rlock = RLock()
    flush_lock = Lock()

    
    def write_CSV(filename, headers, data, r_lock):
        with r_lock:
            if os.path.exists(filename):
                with open(filename, mode="a", newline="") as records_file:
                    records_writer = csv.writer(records_file, dialect="excel")
                    records_writer.writerows(data)
                    data.clear()
            else:
                with open(filename, mode="a", newline="") as records_file:
                    records_writer = csv.writer(records_file, dialect="excel")
                    records_writer.writerow(headers)
                    records_writer.writerows(data)
                    data.clear()
    
    def process_records_q(rec_queue, batch_start_t, r_lock):
        parcel_info = rec_queue.get()
        if len(parcel_info) == 18:
            parcel_records.append(list(parcel_info.values()))
            if len(parcel_records) == 500:
                write_CSV("parcel_records.csv", success_headers, parcel_records, r_lock)
                parcel_records.clear()
        if len(parcel_info) == 3:
            records_missed.append(list(parcel_info.values()))
            if len(records_missed) == 50:
                write_CSV("missed_records.csv", fail_headers, records_missed, r_lock)
                records_missed.clear()

    def flushing_writes(fxn_start_t, lock):
        write_CSV("parcel_records.csv", success_headers, parcel_records, lock)
        write_CSV("missed_records.csv", fail_headers, records_missed, lock)
        print(f"Writing parcel datums took: {timedelta(seconds=time.perf_counter()-fxn_start_t)} seconds.")
        records_df = pd.read_csv("parcel_records.csv")
        fail_df = pd.read_csv("missed_records.csv")
        display(records_df.describe(include="all"))
        display(fail_df.describe(include="all"))

    def q_flow():
        print("Running queue")
        while True:
            while record_q.empty() == False:
                process_records_q(record_q, batch_write_start, csv_rlock)
            else:
                time.sleep(3)
                continue

    
    q_thread = Thread(target=q_flow)

    url_list_length = len(url_pcs)
    decimals = [ i/10 for i in range(1, 11, 1) ]
    split_length = [ url_list_length*decimal for decimal in decimals ]

    with ThreadPoolExecutor(max_workers=max_threads) as p1:
        begin_at = 0
        for split in split_length:
            print("Proceeding...")
            futures = [ p1.submit(scrape_parcel_pg, row[0], row[1]) for row in url_pcs[begin_at:math.floor(split)] ]
            if q_thread.is_alive() == False:
                q_thread.start()
            elif q_thread.is_alive() == True:
                pass
            batch_write_start = time.perf_counter()
            for future in as_completed(futures):
                record_q.put(future.result())
                processed_ct += 1
                if processed_ct % 500 == 0:
                    print(f"Have processed {processed_ct} records in {timedelta(seconds=time.perf_counter()-batch_write_start)} seconds.")
                    batch_write_start = time.perf_counter()
            flushing_writes(function_start, flush_lock)
            begin_at = math.floor(split)
            time.sleep(30)
            print(f"Proceeding to next batch...setting at index {begin_at}..")
    print("Finished.")
    exit()
    
                


# In[ ]:


write_records()


# In[ ]:


records_df = pd.read_csv("parcel_records.csv")
display(records_df.dtypes)
display(records_df.sample(7))
display(records_df.describe(include="all"))


# ## Data Cleaning with PySpark and YData Profiling

# In[ ]:


spark = SparkSession.builder \
    .appName("stcc_ppln") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.execution.arrow.pyspark.enabled","true") \
    .config("spark.sql.caseSensitive", True) \
    .master("local[*]") \
    .getOrCreate()
sc = spark.sparkContext
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(logger.Level.OFF)
logger.LogManager.getLogger("org.apache.spark.SparkEnv").setLevel(logger.Level.ERROR)


# ### Data Profiling

# In[ ]:


records_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("parcel_records.csv")
records_df.printSchema()


# In[ ]:


records_df.show(1, truncate=False, vertical=True)


# In[ ]:


record_profile = ProfileReport(records_df.toPandas())


# In[ ]:


record_profile.to_notebook_iframe()


# In[ ]:


record_df = records_df \
    .withColumn("parcel_number", F.col("parcel_number").cast("string").alias("parcel_number")) \
    .withColumn("net_taxable", F.regexp_replace(F.col("net_taxable"), ",", "").cast("integer").alias("net_taxable")) \
    .withColumn("tax_rate", F.col("tax_rate").cast("decimal(8, 6)").alias("tax_rate")) \
    .withColumn("total_tax", F.col("total_tax").cast("double").alias("total_tax")) \
    .withColumn("homesite_val", F.col("homesite_val").cast("double").alias("homesite_val")) \
    .withColumn("dept_rev_val", F.col("dept_rev_val").cast("double").alias("dept_rev_val")) \
    .withColumn("total_billed", F.col("total_billed").cast("double").alias("total_billed")) \
    .withColumn("total_unpaid", F.col("total_unpaid").cast("double").alias("total_unpaid")) \
    .dropDuplicates()


# In[ ]:


record_df.printSchema()


# In[ ]:


record_df.show(5, truncate=False)


# Missing the leading zeroes on the parcel_number column data. Extract the ideal schema from record_df and use it to recreate records_df by reading from the CSV file.

# In[ ]:


parcel_record_schema = record_df.schema
records_df = spark.read \
    .option("header", True) \
    .schema(parcel_record_schema) \
    .csv("parcel_records.csv")
records_df.printSchema()
records_df.show(5, truncate=False)


# In[ ]:


records_df.select("*").where(F.isnull("net_taxable")).show(10, truncate=False)


# In[ ]:


timesrs_schema = {
    "year": "timeseries",
    "net_taxable": "timeseries",
    # "tax_rate": "timeseries",
    "total_tax": "timeseries",
    "acreage": "timeseries",
    "homesite_val": "timeseries",
    "dwelling_val": "timeseries",
    "dept_rev_val": "timeseries",
    "total_billed": "timeseries",
    "total_unpaid": "timeseries",
}


# In[ ]:


records_profile = ProfileReport(records_df.toPandas(), tsmode=True, type_schema=timesrs_schema, sortby="year")


# In[ ]:


records_profile.to_notebook_iframe()


# ### Data Validation

# Dropping duplicate records and those missing values for required information than cannot be substituted.

# In[ ]:


records_df = records_df.drop_duplicates()


# In[ ]:


records_df.select("*").where(F.isnull("parcel_address")).show(truncate=False)


# The above parcel record is for the coal on a lot designated in another field without an address. "Legal Description: COAL UNDERLYING LOT/SEC-33-SUBL/TWP-2N-BLK/RG-7W SW 1/4 A02197454". This parcel is a child parcel created via combination, so the address of the parent parcel will replace the null value.

# In[ ]:


records_df = records_df.fillna("STATE RT 50 OFALLON, IL 62269", "parcel_address")


# In[ ]:


records_df.select("*").where(F.isnull("parcel_address")).show(truncate=False)


# In[ ]:


records_df.select("*").where(F.isnull("net_taxable")).show(10, truncate=False)


# The null values point to an issue casting the comma-formatted string of an integer to the attempted double data type.

# In[ ]:


null_net_tax_df = records_df.select("*").where(F.isnull("net_taxable"))
# TODO: Delete comma from net_taxable value when scraping.
null_net_tax_df.select("*") \
    .withColumn("net_taxable", F.col("net_taxable").cast("string").alias("net_taxable")) \
    .replace(",", "").show(10, truncate=False)


# In[ ]:


records_df.select("*").where(F.isnull("total_tax")).show(10, truncate=False)


# In[ ]:


records_df.select("*").where(F.col("acreage") == 0).show(10, truncate=False)


# In[ ]:


records_df.select("*").where(F.isnull("homesite_val")).show(truncate=False)


# In[ ]:


records_df.select("*").where(F.isnull("total_billed")).show(10, truncate=False)


# In[ ]:


records_df.select("*").where(F.isnull("total_unpaid")).show(truncate=False)


# In[ ]:




