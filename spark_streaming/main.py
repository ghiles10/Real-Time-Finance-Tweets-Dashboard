import sys 
import threading
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

# launch threads for each process topic
thread1 = threading.Thread(target=process_tweet, args=(spark,))
thread2 = threading.Thread(target=process_finance, args=(spark,))

thread1.start()
thread2.start()

thread1.join()
thread2.join()