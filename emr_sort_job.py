#----------------------------------------------------------------------------------------
# Author: Ramesh Raghupathy / Raj Patel / Gaurav Jain
# Date: 06/15/2022
# Description: This pyspark script reads in 3 arguments (Input file name 
# and outputPath (S3)) and sort key. It reads the Input file and sort the data based on columns in that dataset. 
#---------------------------------------
#s3://myjigurbucketnv/scripts/emr/emr_union_job.py

#spark-submit --deploy-mode cluster s3://myjigurbucketnv/scripts/emr/emr_union_job.py s3://myjigurbucketnv/tpcds-data/venue/  s3://myjigurbucketnv/venue/venue_out/ venuename
#-------------------------------------------------
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
sort_key = sys.argv[3].strip()
delimiter = '|'

ip_file = ip_full_path


print ("----------- Args Start '-------------")
print (sort_key)
print (ip_full_path)
print (outputPath)
print ("----------- Args Done -------------")

ip_df  = spark.read.parquet(ip_file)
print(ip_df.count())
ip_df.show(20)

df_sort = ip_df

df_sort.sort(sort_key).show(20)

df_sort.write.format("csv").mode("overwrite").option("compression", "bzip2").option("delimiter", delimiter).option("ignoreTrailingWhiteSpace", False).option("ignoreLeadingWhiteSpace", False).option("nullValue", "").option("emptyValue","").option("multiline","True").save(outputPath)
