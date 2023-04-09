
QUERY_FACT = f"""SELECT time.hour, symbol, prices.high, prices.low
FROM data-engineering-streaming.finance.temp_table ; 
"""

QUERY_DIM_TIME = f"""
SELECT time
FROM data-engineering-streaming.finance.temp_table  ; 
"""

QUERY_DIM_STOCK = f"""
SELECT symbol, prices.buy, prices.sell, volume.vol
FROM data-engineering-streaming.finance.temp_table  ; 
"""


