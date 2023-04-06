import sys 
from threading import Thread
from pathlib import Path
from pyspark.sql import SparkSession 

# Project Directories
ROOT = Path(__file__).parent.parent
# Append the path
sys.path.append(f'{ROOT}')

from tweet_process.process_tweet import process_tweet
from finance_process.process_finance import process_finance
from config import core, schema

# config file
APP_CONFIG = schema.SparkConfig(**core.load_config().data["spark_config"])

spark = SparkSession.builder.appName(APP_CONFIG.app_name ).getOrCreate() 

# launch   each process topic
print('$' * 100, end = "\n")

th1 = Thread(target=process_tweet, args=(spark,))
th2 = Thread(target=process_finance, args=(spark,)) 

print('1' * 100, end = "\n")
th1.start() 

print('2' * 100, end = "\n")

th2.start()

th1.join()
th2.join()