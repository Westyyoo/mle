import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

import utils.data_processing_bronze_table
import utils.data_processing_silver_table
import utils.data_processing_gold_table

import utils.data_processing_bronze_table_cs
import utils.data_processing_bronze_table_att
import utils.data_processing_bronze_table_fin


import utils.data_processing_silver_table_att
import utils.data_processing_silver_table_fin


import utils.data_processing_gold_table_feature


# Initialize SparkSession
spark = pyspark.sql.SparkSession.builder \
    .appName("dev") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to ERROR to hide warnings
spark.sparkContext.setLogLevel("ERROR")

# set up config
snapshot_date_str = "2023-01-01"

start_date_str = "2023-01-01"
end_date_str = "2024-12-01"

# generate list of dates to process
def generate_first_of_month_dates(start_date_str, end_date_str):
    # Convert the date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # List to store the first of month dates
    first_of_month_dates = []

    # Start from the first of the month of the start_date
    current_date = datetime(start_date.year, start_date.month, 1)

    while current_date <= end_date:
        # Append the date in yyyy-mm-dd format
        first_of_month_dates.append(current_date.strftime("%Y-%m-%d"))
        
        # Move to the first of the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    return first_of_month_dates

dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
print(dates_str_lst)

# create bronze datalake
bronze_lms_directory = "datamart/bronze/lms/"

if not os.path.exists(bronze_lms_directory):
    os.makedirs(bronze_lms_directory)

# run bronze backfill
for date_str in dates_str_lst:
    utils.data_processing_bronze_table.process_bronze_table(date_str, bronze_lms_directory, spark)

bronze_clickstream_directory = "datamart/bronze/cs/"

if not os.path.exists(bronze_clickstream_directory):
    os.makedirs(bronze_clickstream_directory)
utils.data_processing_bronze_table_cs.process_bronze_table_cs(bronze_clickstream_directory, spark)

bronze_att_directory = "datamart/bronze/att/"

if not os.path.exists(bronze_att_directory):
    os.makedirs(bronze_att_directory)
utils.data_processing_bronze_table_att.process_bronze_table_att(bronze_att_directory, spark)

bronze_fin_directory = "datamart/bronze/fin/"

if not os.path.exists(bronze_fin_directory):
    os.makedirs(bronze_fin_directory)
utils.data_processing_bronze_table_fin.process_bronze_table_fin(bronze_fin_directory, spark)



# create bronze datalake
silver_loan_daily_directory = "datamart/silver/loan_daily/"

if not os.path.exists(silver_loan_daily_directory):
    os.makedirs(silver_loan_daily_directory)

# run silver backfill
for date_str in dates_str_lst:
    utils.data_processing_silver_table.process_silver_table(date_str, bronze_lms_directory, silver_loan_daily_directory, spark)

silver_att_directory = "datamart/silver/att/"

if not os.path.exists(silver_att_directory):
    os.makedirs(silver_att_directory)

# run silver backfill

utils.data_processing_silver_table_att.process_silver_table_att(bronze_att_directory, silver_att_directory, spark)

silver_fin_directory = "datamart/silver/fin/"

if not os.path.exists(silver_fin_directory):
    os.makedirs(silver_fin_directory)

# run silver backfill

utils.data_processing_silver_table_fin.process_silver_table_fin(bronze_fin_directory, silver_fin_directory, spark)


# create bronze datalake
gold_label_store_directory = "datamart/gold/label_store/"

if not os.path.exists(gold_label_store_directory):
    os.makedirs(gold_label_store_directory)

# run gold backfill
for date_str in dates_str_lst:
    utils.data_processing_gold_table.process_labels_gold_table(date_str, silver_loan_daily_directory, gold_label_store_directory, spark, dpd = 30, mob = 6)

gold_feature_store_directory = "datamart/gold/feature_store/"

if not os.path.exists(gold_feature_store_directory):
    os.makedirs(gold_feature_store_directory)

# run gold backfill

utils.data_processing_gold_table_feature.process_feature_gold_table(silver_fin_directory, gold_feature_store_directory,spark)


folder_path = gold_label_store_directory
files_list = [folder_path+os.path.basename(f) for f in glob.glob(os.path.join(folder_path, '*'))]
df = spark.read.option("header", "true").parquet(*files_list)
print("row_count:",df.count())

df.show()



    