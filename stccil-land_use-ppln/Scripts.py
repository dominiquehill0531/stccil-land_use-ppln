# Native
from bs4 import BeautifulSoup
from datetime import timedelta
from io import StringIO
import os
import requests
import time
# 3rd Party

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
import pyspark.pandas as pd
from pyspark.sql import DataFrame, functions as F, SparkSession, types as T
# Local


target_nums = [1, 2, 6, 11]
all_nums = list(range(1, 23))
parcel_list_filename = "Exported_Search_Results.csv"
search_pg = "https://stclairil.devnetwedge.com/"
cwd = os.getcwd()
success_headers = [
    "parcel_number", "year", "parcel_address", "owner", "owner_address", 
    "sale_status", "property_class", "tax_status", "net_taxable", 
    "tax_rate", "total_tax", "township", "acreage", "homesite_val", 
    "dwelling_val", "dept_rev_val", "total_billed", "total_unpaid"
]
fail_headers = ["parcel", "year", "error"]
errored_records_data = []
parcel_records_data = []


def strfyNums(*nums: int) -> list[str]:
    return [ "{:02d}".format(num) for num in target_nums]


def dwnldParcelList(curr_workg_dir: str, search_page: str, *township_nums: int):
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--headless=new")
    prefs = {
        "download.default_directory": f"{curr_workg_dir}",
        "download.prompt_for_download": False,
        # "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options)
    driver.implicitly_wait(3)
    driver.get(search_pg)

    # Click into Advanced Search Tab
    advance_search_tab = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//a[@href='#advanced-search']"))
    )
    advance_search_tab.click()
    
    # Select Townships
    township_select = Select(WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "advanced-search-townships"))
    ))
    for str_num in strfyNums(township_nums):
        township_select.select_by_value(str_num)

    # Check All Years Box and Search
    all_years_chkbx = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "advanced-search-include-all-years"))
    )
    form = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//form"))
    )
    driver.execute_script(f"document.getElementById('advanced-search-include-all-years').click()")
    all_years_chkbx.submit()

    # Export Results to CSV and Download
    export_btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//a[@href='/Search/ExportClientsListToCSV']"))
    )
    export_btn.click()

    if os.path.isfile("./{}".format(parcel_list_filename)):
        driver.quit()
    else:
        time.sleep(10)
        


def createSparkSession() -> SparkSession:
    spark: SparkSession = SparkSession.builder \
        .appName("stcc_ppln") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.execution.arrow.pyspark.enabled","true") \
        .config("spark.sql.caseSensitive", True) \
        .master("local[*]") \
        .getOrCreate()
    return spark


def createParcelListDF(spark: SparkSession, filename: str) -> DataFrame:
    parcel_list_df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(filename)
    parcel_list_df = parcel_list_df \
        .withColumn("Prop_Acct_Num",
                    F.regexp_replace("Property Account Number",
                                     "\D",
                                     ""))
    return parcel_list_df


def scrapeParcelPg(search_page: str, listing_number: str, listing_year: int):
    parcel_url = f"{search_page}parcel/view/{listing_number}/{listing_year}"
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
    

def writeDFtoCSV(data: list[dict], data_schema: list[str], csv_filename: str):
    df: DataFrame = createSparkSession().createDataFrame(data, data_schema)
    df.write \
        .option("header",True) \
        .mode("append") \
        .csv(csv_filename)


def scrapeOneRow(listing):
    batch_start_time = time.perf_counter()
    scraped_data = scrapeParcelPg(search_pg, listing["Prop_Acct_Num"], listing["Year"])
    if len(scraped_data) == 18:
        parcel_records_data.append(scraped_data)
        if len(parcel_records_data) >= 500:
            batch_end_time = time.perf_counter()
            writeDFtoCSV(parcel_records_data, success_headers, "parcel_records.csv")
            print("~500 records successfully processed in {:.2f} minutes.".format((batch_end_time - batch_start_time) / 60))
            parcel_records_data.clear()
            batch_start_time = time.perf_counter()
    if len(scraped_data) == 3:
        errored_records_data.append(scraped_data)
        if len(errored_records_data) >= 50:
            writeDFtoCSV(errored_records_data, fail_headers, "missed_records.csv")
            errored_records_data.clear()



def scrapeAllParcelPgs(spark: SparkSession, list_filename: str, search_page: str):
    process_start_time = time.perf_counter()
    parcel_list_df = createParcelListDF(spark, list_filename)
    parcel_list_df.foreach(scrapeOneRow)
    return print("Processed {} total records in {:.2f} minutes.".format(
        parcel_list_df.count(),
        (time.perf_counter() - process_start_time) / 60
    ))