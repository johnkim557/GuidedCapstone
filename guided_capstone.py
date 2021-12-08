from pyspark.sql import SparkSession

session = SparkSession.builder.master('local').appName('app').getOrCreate()

container = 'azcopyblobstorage'
storage_account_name = 'azstorage'
sas_token = 'sp=racwd&st=2021-09-20T20:23:32Z&se=2021-09-30T04:23:32Z&spr=https&sv=2020-08-04&sr=c&sig=%2BhK7jPgtzruCsJs3Q%2FOyaPCbVRGqLBFRZSvbLvAaFjE%3D'

session.conf.set(f"fs.azure.sas.{storage_account_name}.azcopyblobstorage.blob.core.windows.net",sas_token)

key = 'kEpoDeexDaGX850vwg7kjgEUD2sQ/8QoHBtnyXNgcNGvl8ZHcLWPjpX90QOi27y61Y0HUeF6xSqZggvFAMf7gg==' #storage account

dbutils.fs.mount(
  source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net",
  mount_point = "/mnt/azureStorage",
  extra_configs = {f"fs.azure.account.key.azcopyblobstorage.blob.core.windows.net": key}
)

dbutils.fs.mounts()

%fs ls "dbfs:/mnt/azureStorage"

csv = dbutils.fs.ls('/mnt/azureStorage/csv')
csv_files = [i.name for i in csv]
json = dbutils.fs.ls('mnt/azureStorage/json')
json_files = [i.name for i in json]

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,TimestampType,DecimalType
csv_schema = StructType([ 
    StructField("trade_date",DateType(),True),
    StructField("trade_dt",TimestampType(),True), \
    StructField("rec_type",StringType(),True), \
    StructField("symbol",StringType(),True), \
    StructField("event_tm", TimestampType(), True), \
    StructField("event_seq_nb", IntegerType(), True), \
    StructField("exchange", StringType(), True),
    StructField("bid_pr", DecimalType(),True),
    StructField("bid_size", IntegerType(),True),
    StructField("ask_pr",DecimalType(),True),
    StructField("ask_size",IntegerType(),True),
    StructField("partition",StringType(),True),
    StructField("execution_id",StringType(),True),
    StructField("trade_price", DecimalType(),True)
    
  ])

  raw_csv1 = spark.read \
    .format("csv")\
    .option("path", "/mnt/azureStorage/csv/"+csv_files[0])\
    .schema(csv_schema)\
    .load()
  
raw_csv2 = spark.read \
    .format("csv")\
    .option("path", "/mnt/azureStorage/csv/"+csv_files[1])\
    .schema(csv_schema)\
    .load() 

raw_csv1.collect()

raw_csv1.first()[1]

rdd_csv1 = raw_csv1.rdd
rdd_csv2 = raw_csv2.rdd

def parse_csv(line):
      if line[2] == 'Q':
    trade_date = line[0]
    date_tm = line[1]
    rec_type = line[2]
    symbol = line[3]
    event_tm = line[4]
    event_seq_tm = line[5]
    exchange = line[6]
    bid_price = line[7] ####
    bid_size = line[8]
    ask_price = line[9]
    ask_size = line[10]
    
    return trade_date, date_tm, rec_type, symbol, event_tm, event_seq_tm,exchange,bid_price,bid_size,ask_price,ask_size,'Q','NA',None
  elif line[2] == 'T':
    
    #trade_date, date_tm, rec_type, symbol, event_tm, event_seq_tm,exchange,execution_id,trade_pr,,trade_size,bid_price,bid_size,ask_price,ask_size,'Q'
    trade_date = line[0]
    date_tm = line[1]
    rec_type = line[2]
    symbol = line[3]
    event_tm = line[4]
    event_seq_tm = line[5]
    exchange= line[6]
    trade_price = line[7]
    
    ##check this area for trade price
    return trade_date, date_tm, rec_type, symbol, event_tm, event_seq_tm,exchange,None,None,None,None,'T','NA',trade_price
  else:
    return None,None,None,None,None,None,None,None,None,None,None,'B',None,None

parsed_csv1 = rdd_csv1.map(lambda line : parse_csv(line))
parsed_csv2 = rdd_csv2.map(lambda line : parse_csv(line))

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,TimestampType,DecimalType
#schema fields must match the json file's keys
json_schema = StructType([ 
    StructField("trade_dt",DateType(),True),
    StructField("file_tm",TimestampType(),True), \
    StructField("event_type",StringType(),True), \
    StructField("symbol",StringType(),True), \
    StructField("event_tm", TimestampType(), True), \
    StructField("event_seq_nb", IntegerType(), True), \
    StructField("exchange", StringType(), True),
    StructField("bid_pr", DecimalType(),True),
    StructField("bid_size", IntegerType(),True),
    StructField("ask_pr",DecimalType(),True),
    StructField("ask_size",IntegerType(),True),
    StructField("partition",StringType(),True),
    StructField("execution_id",StringType(),True),
    StructField("price", DecimalType(),True)
  ])

raw_json1 = spark.read \
    .format("json")\
    .option("path", "/mnt/azureStorage/json/"+json_files[0])\
    .schema(json_schema)\
    .load()
  
raw_json2 = spark.read \
    .format("json")\
    .option("path", "/mnt/azureStorage/json/"+json_files[1])\
    .schema(json_schema)\
    .load()  

def parse_json(line):
      if line['event_type'] == 'Q':
  
    trade_date = line['trade_dt']
    date_tm = line['file_tm']
    rec_type = line['event_type']
    symbol = line['symbol']
    event_tm = line['event_tm']
    event_seq_tm = line['event_seq_nb']
    exchange = line['exchange']
    bid_price = line['bid_pr']
    bid_size = line['bid_size']
    ask_price = line['ask_pr']
    ask_size = line['ask_size']
    return trade_date, date_tm, rec_type, symbol, event_tm, event_seq_tm,exchange,bid_price,bid_size,ask_price,ask_size,'Q','NA',None
     #trade_date, date_tm, rec_type, symbol, event_tm, event_seq_tm,exchange,execution_id,trade_pr,,trade_size,bid_price,bid_size,ask_price,ask_size,'Q'
  elif line['event_type']=='T':
    trade_date = line['trade_dt']
    date_tm = line['file_tm']
    rec_type = line['event_type']
    symbol = line['symbol']
    event_tm = line['event_tm']
    event_seq_tm = line['event_seq_nb']
    exchange= line['exchange']
    trade_price = line['price']
    execution_id = line['execution_id']
    
    
    return trade_date, date_tm, rec_type, symbol, event_tm, event_seq_tm,exchange,None,None,None,None,'T',execution_id,trade_price
  
  else:
    return None,None,None,None,None,None,None,None,None,None,None,'B',None,None

parsed_json1 = rdd_json1.map(lambda line: parse_json(line))
parsed_json2 = rdd_json2.map(lambda line: parse_json(line))

total_rdd = sc.union([parsed_csv1, parsed_csv2, parsed_json1, parsed_json2]) #columns are not matching

total_rdd_df = spark.createDataFrame(data=total_rdd,schema=json_schema)

total_rdd_df.show(10,truncate= False)


total_rdd_df.write.partitionBy("partition").mode("overwrite").parquet("/mnt/azureStorage/data")


trade_common_t = spark.read.parquet("/mnt/azureStorage/data/partition=T")
trade_common_q = spark.read.parquet("/mnt/azureStorage/data/partition=Q")

trade_common_t = trade_common_t.withColumnRenamed('file_tm','arrival_tm')
trade_common_q = trade_common_q.withColumnRenamed('file_tm','arrival_tm')

trade_common_t.printSchema()

trade_t = trade_common_t.select('trade_dt', 'symbol', 'exchange', 'event_tm','event_seq_nb', 'arrival_tm','execution_id','price')
trade_q = trade_common_q.select('trade_dt', 'symbol', 'exchange', 'event_tm','event_seq_nb', 'arrival_tm','execution_id','price') 

from pyspark.sql import functions as func

trade_corrected_t = trade_common_t.groupBy('trade_dt','symbol','exchange','event_tm','event_seq_nb','execution_id','price').agg(func.max('arrival_tm'))
trade_corrected_q = trade_common_q.groupBy('trade_dt','symbol','exchange','event_tm','event_seq_nb','execution_id','price').agg(func.max('arrival_tm'))

from pyspark.sql.functions import col
trade_dt_t = trade_corrected_t.withColumn("trade_dt",col("trade_dt").cast(StringType())).first()["trade_dt"]
trade_dt_q = trade_corrected_q.withColumn("trade_dt",col("trade_dt").cast(StringType())).first()["trade_dt"]
trade_dt_t

trade_t.write.mode("overwrite").parquet("/mnt/azureStorage/trade/trade_dt={}".format(trade_dt_t))
trade_q.write.mode("overwrite").parquet("/mnt/azureStorage/trade/trade_dt={}".format(trade_dt_q))

trade = spark.read.parquet("/mnt/azureStorage/trade/trade_dt={}".format("2020-08-05"))

trade.createOrReplaceTempView("trade")

df = spark.sql("SELECT symbol, exchange, event_tm,event_seq_nb,price from trade where trade_dt = '2020-08-05'")

df.createOrReplaceTempView("tmp_trade_moving_avg")

mov_avg_df = spark.sql(""" select symbol, exchange, event_tm, event_seq_nb, price, avg(price) OVER(PARTITION BY symbol, exchange ORDER BY event_tm RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr from tmp_trade_moving_avg
order by event_tm
""")
mov_avg_df.show()

df.createOrReplaceTempView("tmp_last_trade")

last_trade_pr = spark.sql("""
    SELECT
        symbol,
        exchange,
        price as last_td_pr
    FROM
     (
        SELECT
            symbol,
            exchange,
            event_tm,
            event_seq_nb,
            price,
            ROW_NUMBER() OVER(PARTITION BY symbol, exchange ORDER BY event_tm DESC, event_seq_nb DESC) as r_number
        FROM
            tmp_last_trade
    ) a
    WHERE
        r_number = 1
            """)

mov_avg_df.write.saveAsTable("temp_last_trade")

quote_union = spark.sql# Querying quote_union
last_trade_pr = spark.sql("""
    SELECT
        trade_dt,
        symbol,
        exchange,
        event_tm,
        event_seq_nb,
        bid_pr,
        bid_size,
        ask_pr,
        ask_size
    FROM
        quotes
    UNION ALL
    SELECT
        trade_dt,
        symbol,
        exchange,
        event_tm,
        event_seq_nb,
        bid_pr,
        bid_size,
        ask_pr,
        mov_avg_pr
    FROM
        tmp_trade_moving_avg
            """)

quote_union.createOrReplaceTempView("quote_union")

quote_update = spark.sql("""
select trade_dt, symbol, event_tm, event_seq_nb, exchange,
bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
from quote_union_update
where rec_type = 'Q'
""")
quote_update.createOrReplaceTempView("quote_update")

quote_final = spark.sql("""
select trade_dt, symbol, event_tm, event_seq_nb, exchange,
bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr,
bid_pr - close_pr as bid_pr_mv, ask_pr - close_pr as ask_pr_mv
from a
""")

quote_update.write.parquet("cloud-storage-path/quote-trade-analytical/date={}".format(trade_date))