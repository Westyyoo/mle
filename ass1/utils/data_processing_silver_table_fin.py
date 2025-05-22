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

from pyspark.sql.functions import (
    col, trim, regexp_replace, when, lower, lit)
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


def process_silver_table_fin(bronze_fin_directory, silver_fin_directory, spark):

    
    # connect to bronze table
    partition_name = "bronze_financials" + '.csv'
    filepath = bronze_fin_directory + partition_name
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    print('loaded from:', filepath, 'row count:', df.count())

    # clean data: enforce schema / data type
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
 
        "Customer_ID": StringType(),

        "Type_of_Loan": StringType(),
        "Num_Credit_Card": IntegerType(),"Interest_Rate": IntegerType(),"Num_of_Loan": IntegerType(),"Monthly_Balance": IntegerType(),
        "Delay_from_due_date": IntegerType(),"Num_of_Delayed_Payment": IntegerType(),"Num_Credit_Inquiries": IntegerType(),"Credit_Utilization_Ratio": FloatType(),
        "Annual_Income": FloatType(),"Monthly_Inhand_Salary": FloatType(),
        "snapshot_date": DateType(),

    }

    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))
    
    df = df.withColumn("Num_Credit_Card",when(col("Num_Credit_Card") < 0, 0).otherwise(col("Num_Credit_Card")))
    df = df.withColumn("Interest_Rate",when(col("Interest_Rate") < 0, 0).otherwise(col("Interest_Rate")))
    df = df.withColumn("Num_of_Loan",when(col("Num_of_Loan") < 0, 0).otherwise(col("Num_of_Loan")))
    df = df.withColumn("Monthly_Balance",when(col("Monthly_Balance") < 0, 0).otherwise(col("Monthly_Balance")))
    df = df.withColumn("Delay_from_due_date",when(col("Delay_from_due_date") < 0, 0).otherwise(col("Delay_from_due_date")))
    df = df.withColumn("Num_of_Delayed_Payment",when(col("Num_of_Delayed_Payment") < 0, 0).otherwise(col("Num_of_Delayed_Payment")))
    df = df.withColumn("Num_Credit_Inquiries",when(col("Num_Credit_Inquiries") < 0, 0).otherwise(col("Num_Credit_Inquiries")))
    df = df.withColumn("Credit_Utilization_Ratio",when(col("Credit_Utilization_Ratio") < 0, 0).otherwise(col("Credit_Utilization_Ratio")))
    df = df.withColumn("Annual_Income",when(col("Annual_Income") < 0, 0).otherwise(col("Annual_Income")))
    df = df.withColumn("Monthly_Inhand_Salary",when(col("Monthly_Inhand_Salary") < 0, 0).otherwise(col("Monthly_Inhand_Salary")))
    

    df = df.select("Customer_ID","Type_of_Loan","Num_Credit_Card","Interest_Rate","Num_of_Loan","Monthly_Balance","Delay_from_due_date","Num_of_Delayed_Payment","Num_Credit_Inquiries",
                   "Credit_Utilization_Ratio","Annual_Income","Monthly_Inhand_Salary")
    # save silver table - IRL connect to database to write
    partition_name = "silver_fin" + '.parquet'
    filepath = silver_fin_directory + partition_name
    df.write.mode("overwrite").parquet(filepath)
    # df.toPandas().to_parquet(filepath,
    #           compression='gzip')
    print('saved to:', filepath)
    
    return df