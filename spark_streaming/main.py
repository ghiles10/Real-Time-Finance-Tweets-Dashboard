import sys 
from multiprocessing import Process
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
p1 = Process(target=process_tweet, args=(spark,))
p2 = Process(target=process_finance, args=(spark,))

p1.start()
p2.start()

p1.join()
p2.join()

sys.exit()


