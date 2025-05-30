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


def process_bronze_table_att(bronze_att_directory, spark):
    
    
    
    # connect to source back end - IRL connect to back end source system
    csv_file_path = "data/features_attributes.csv"

    # load data - IRL ingest from back end source system
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    print('attributes' + 'row count:', df.count())

    # save bronze table to datamart - IRL connect to database to write
    partition_name = "bronze_attributes" +  '.csv'
    filepath = bronze_att_directory + partition_name
    #df.toPandas().to_csv(filepath, index=False)
    df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(bronze_att_directory + partition_name)
    print('saved to:', filepath)

    return df
