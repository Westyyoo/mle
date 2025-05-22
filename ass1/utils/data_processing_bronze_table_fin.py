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
import argparse

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


def process_bronze_table_fin(bronze_fin_directory, spark):
    
    
    
    # connect to source back end - IRL connect to back end source system
    csv_file_path = "data/features_financials.csv"

    # load data - IRL ingest from back end source system
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    print('financials' + 'row count:', df.count())

    # save bronze table to datamart - IRL connect to database to write
    partition_name = "bronze_financials" +  '.csv'
    filepath = bronze_fin_directory + partition_name
    #df.toPandas().to_csv(filepath, index=False)
    df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(bronze_fin_directory + partition_name)
    print('saved to:', filepath)

    return df
