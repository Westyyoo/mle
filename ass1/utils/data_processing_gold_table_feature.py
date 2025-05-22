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


def process_feature_gold_table(silver_fin_directory, gold_feature_store_directory, spark):
    

    
    # connect to bronze table
    partition_name = "silver_fin" + '.parquet'
    filepath = silver_fin_directory + partition_name
    df = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', df.count())
    df = df.select("Customer_ID","Num_Credit_Card","Interest_Rate","Num_of_Loan","Monthly_Balance","Delay_from_due_date","Num_of_Delayed_Payment","Num_Credit_Inquiries",
                   "Credit_Utilization_Ratio","Annual_Income","Monthly_Inhand_Salary")





    # save gold table - IRL connect to database to write
    partition_name = "gold_feature_store_" + '.parquet'
    filepath = gold_feature_store_directory + partition_name
    df.write.mode("overwrite").parquet(filepath)
    # df.toPandas().to_parquet(filepath,
    #           compression='gzip')
    print('saved to:', filepath)
    
    return df