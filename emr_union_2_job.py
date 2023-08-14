#----------------------------------------------------------------------------------------
# Author: Ramesh Raghupathy / Gaurav Jain / Raj Patel
# Description: This pyspark script reads in 3 arguments (Input file name, multiply count and outputPath (S3)). 
# It reads the Input file and unions it as many times as the Multiply count. 
#-----------------------------------------------------------------------------------------
from __future__ import print_function
from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .master("yarn") \
        .appName("GenericUnion") \
        .getOrCreate()

sc = spark.sparkContext
print("spark context sc:",sc)
import sys
import time
from pyspark.sql import DataFrame
from functools import reduce

if (len(sys.argv) < 2):
    print ("Insufficient args ")
    quit()

ip_full_path = sys.argv[1]
outputPath = sys.argv[2].strip()
multiply_count = int(sys.argv[3].strip())
delimiter = '|'

ip_file = ip_full_path


print ("----------- Args Start '-------------")
print (multiply_count)
print (ip_full_path)
print (outputPath)
print ("----------- Args Done -------------")

ip_df  = spark.read.parquet(ip_file)
print(ip_df.count())
ip_df.show(20)

df_union = ip_df

for i in range(multiply_count):
    df_union = df_union.union(df_union)
    print(df_union.count())

df_union.show(20)

df_union.write.format("csv").mode("overwrite").option("compression", "bzip2").option("delimiter", delimiter).option("ignoreTrailingWhiteSpace", False).option("ignoreLeadingWhiteSpace", False).option("nullValue", "").option("emptyValue","").option("multiline","True").save(outputPath)
