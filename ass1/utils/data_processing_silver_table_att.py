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


def process_silver_table_att(bronze_att_directory, silver_att_directory, spark):

    
    # connect to bronze table
    partition_name = "bronze_attributes" + '.csv'
    filepath = bronze_att_directory + partition_name
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    print('loaded from:', filepath, 'row count:', df.count())

    # clean data: enforce schema / data type
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
 
        "Customer_ID": StringType(),

        "Name": StringType(),
        "Age": IntegerType(),

    }

    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))
    
    df = df.withColumn(
        "Age",
        when(col("Age").isNull(), lit(-1))
        .when((col("Age") < 0) | (col("Age") > 100), lit(-1))
        .otherwise(col("Age"))
    )


    df = df.select("Customer_ID","Name","Age")
    # save silver table - IRL connect to database to write
    partition_name = "silver_att" + '.csv'
    filepath = silver_att_directory + partition_name
    #df.toPandas().to_csv(filepath, index=False)
    df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(silver_att_directory + partition_name)
    
    print('saved to:', filepath)
    
    return df